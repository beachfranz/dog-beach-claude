"""extract_one_fid.py — per-fid extractor wrapper.

Phase C atom of the state-launch pipeline. Given a single arena_group_id
(`beaches_gold.fid`), look up the beach + URL candidates, run the LLM
extraction across active prompt variants, and insert results into
beach_policy_extractions. The existing `populate_from_research_gold`
populator picks those up on the next promote_to_gold pass.

Usage:
  python scripts/extract_one_fid.py --fid 1234 --kind dogs_extraction [--apply]
  python scripts/extract_one_fid.py --fid 1234 --kind park_url_discovery --apply
  python scripts/extract_one_fid.py --fid 1234 --apply --max-urls 2

Behavior:
  * --fid is required; resolves name/county/state from beaches_gold.
  * --kind is informational (drives URL prioritization):
      dogs_extraction      → prefer existing operator/dog-policy URLs
      park_url_discovery   → prefer park_url candidates
      full_refresh         → all candidates
  * --apply writes to beach_policy_extractions; without it, dry-run.
  * --max-urls N caps how many distinct URLs to fetch per fid (default 2).

Composes from extract_for_orphans primitives (fetch_html, bs4_strip_loose,
upsert_source, insert_extractions) and from extract_beach_policies
primitives (call_llm, parse_response, run_sql, sql_literal). No new
LLM-prompt logic — just sources + dispatch.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.parse
import uuid
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

sys.path.insert(0, str(SCRIPT_DIR))
from extract_beach_policies import call_llm, parse_response, run_sql, sql_literal  # noqa
from extract_for_orphans import (  # noqa
    fetch_html, bs4_strip_loose, upsert_source, insert_extractions,
)


def lookup_beach(fid: int) -> dict | None:
    rows = run_sql(f"""
        select g.fid, g.name, g.county_name, g.state, g.location_id,
               coalesce(a.group_id, g.fid) as arena_group_id
          from public.beaches_gold g
          left join public.arena a on a.fid = g.fid
         where g.fid = {fid}
         limit 1;
    """)
    return rows[0] if rows else None


def candidate_urls(fid: int, kind: str, max_urls: int) -> list[dict]:
    """Return [{url, source_type, priority}] candidates for this fid.

    Pulled from:
      1. beach_enrichment_provenance.source_url for this gold_fid (existing extractions)
      2. operators.dog_policy_url + operators.url via arena→operator_id join
      3. park_url_extractions.url for this fid
    De-duped by URL. Priority orders by --kind affinity.
    """
    seen: dict[str, dict] = {}

    # 1. BEP source_urls already attached to this fid
    bep = run_sql(f"""
        select distinct source_url, source
          from public.beach_enrichment_provenance
         where gold_fid = {fid}
           and source_url is not null
           and source_url <> ''
         limit 50;
    """)
    for r in bep:
        url = r["source_url"]
        src = r["source"] or ""
        prio = 1 if "park_url" in src else 2 if "operator" in src else 3
        seen[url] = {"url": url, "source_type": f"bep:{src}", "priority": prio}

    # 2. operators dog_policy_url + website (via BEP.operator_id linkage)
    op = run_sql(f"""
        select distinct o.dog_policy_url, o.website
          from public.beach_enrichment_provenance bep
          join public.operators o on o.id = bep.operator_id
         where bep.gold_fid = {fid}
           and bep.operator_id is not null
         limit 5;
    """)
    for r in op:
        for url, prio in ((r.get("dog_policy_url"), 1), (r.get("website"), 4)):
            if url and url not in seen:
                seen[url] = {"url": url, "source_type": "operator_url", "priority": prio}

    # 3. park_url_extractions
    pu = run_sql(f"""
        select distinct source_url
          from public.park_url_extractions
         where fid = {fid}
           and source_url is not null
         limit 10;
    """)
    for r in pu:
        url = r["source_url"]
        if url and url not in seen:
            seen[url] = {"url": url, "source_type": "park_url", "priority": 1}

    # Apply kind-specific re-weighting
    if kind == "park_url_discovery":
        for c in seen.values():
            if "park_url" in c["source_type"]:
                c["priority"] = 0  # bump
    elif kind == "dogs_extraction":
        for c in seen.values():
            if "operator" in c["source_type"]:
                c["priority"] = max(1, c["priority"] - 1)

    cands = sorted(seen.values(), key=lambda c: (c["priority"], c["url"]))
    return cands[:max_urls]


def load_variants(field_filter: str | None = None) -> list[dict]:
    if field_filter:
        names = [n.strip() for n in field_filter.split(",") if n.strip()]
        in_clause = ",".join(sql_literal(n) for n in names)
        return run_sql(f"""
            select id, field_name, variant_key, prompt_template,
                   expected_shape, target_model
              from public.extraction_prompt_variants
             where active = true and field_name in ({in_clause})
             order by target_model, field_name, variant_key;
        """)
    return run_sql("""
        select id, field_name, variant_key, prompt_template,
               expected_shape, target_model
          from public.extraction_prompt_variants
         where active = true
         order by target_model, field_name, variant_key;
    """)


def extract_for_url(client, beach: dict, cand: dict, variants: list[dict],
                    apply: bool) -> tuple[int, int, list[dict]]:
    """Fetch URL, run all variants, return (n_ok, n_fail, rows_to_insert)."""
    print(f"   -> {cand['source_type']}: {cand['url']}")
    html = fetch_html(cand["url"])
    if not html:
        print(f"     ! fetch failed")
        return 0, 0, []
    page = bs4_strip_loose(html)
    if len(page) < 200:
        print(f"     ! page too small ({len(page)} chars after strip)")
        return 0, 0, []
    print(f"     page content: {len(page)} chars")

    source_id = upsert_source(cand["url"]) if apply else -1
    rows: list[dict] = []
    n_ok = n_fail = 0
    consensus: dict[str, list] = {}

    for v in variants:
        try:
            resp = call_llm(client, v["target_model"], v["prompt_template"], page)
        except Exception as e:
            print(f"     {v['field_name']}/{v['variant_key']}: API error {e}")
            n_fail += 1
            continue
        parsed = parse_response(resp["raw"], v["expected_shape"])
        if parsed["parse_succeeded"]:
            n_ok += 1
            consensus.setdefault(v["field_name"], []).append(parsed["parsed_value"])
        else:
            n_fail += 1
        rows.append({
            "fid": beach["fid"],
            "arena_group_id": beach["arena_group_id"],
            "source_id": source_id,
            "variant_id": v["id"],
            "field_name": v["field_name"],
            "source_type": cand["source_type"][:40],
            "variant_key": v["variant_key"],
            "raw_response": resp["raw"][:6000],
            "parsed_value": parsed["parsed_value"],
            "evidence_quote": parsed["evidence_quote"],
            "raw_snippet": page[:1500],
            "parse_succeeded": parsed["parse_succeeded"],
            "model_name": v["target_model"],
            "input_tokens": resp["input_tokens"],
            "output_tokens": resp["output_tokens"],
            "latency_ms": resp["latency_ms"],
        })
        time.sleep(0.04)

    print(f"     variants: {n_ok} parsed OK, {n_fail} failed")
    for field, vals in sorted(consensus.items()):
        unique = list(dict.fromkeys(str(x) for x in vals if x is not None))
        disp = " | ".join(u[:50] for u in unique[:2])
        print(f"       {field:30}  {disp}")
    return n_ok, n_fail, rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fid", type=int, required=True)
    ap.add_argument("--kind", default="dogs_extraction",
                    choices=["dogs_extraction", "park_url_discovery", "full_refresh"])
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-urls", type=int, default=2)
    ap.add_argument("--field-filter", default=os.environ.get("FIELD_NAMES"),
                    help="Comma-separated field_names to restrict (env: FIELD_NAMES)")
    args = ap.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        sys.exit("ANTHROPIC_API_KEY missing")
    client = Anthropic(api_key=api_key)

    beach = lookup_beach(args.fid)
    if not beach:
        sys.exit(f"fid {args.fid} not found in beaches_gold")
    print(f"== {beach['name']} (fid {args.fid}, {beach['county_name']}, {beach['state']}) ==")

    cands = candidate_urls(args.fid, args.kind, args.max_urls)
    if not cands:
        print(f"   ! no URL candidates found for fid {args.fid} kind={args.kind}")
        print(f"   (try --kind park_url_discovery, or run extract_for_orphans for URL discovery first)")
        return 2

    variants = load_variants(args.field_filter)
    print(f"   {len(cands)} URL(s), {len(variants)} variant(s), kind={args.kind}, apply={args.apply}")

    run_id = f"queue-{uuid.uuid4().hex[:8]}"
    all_rows: list[dict] = []
    total_ok = total_fail = 0
    for cand in cands:
        n_ok, n_fail, rows = extract_for_url(client, beach, cand, variants, args.apply)
        total_ok += n_ok
        total_fail += n_fail
        all_rows.extend(rows)

    total_in = sum(r["input_tokens"] for r in all_rows)
    total_out = sum(r["output_tokens"] for r in all_rows)
    print(f"\n   total: {total_ok} ok, {total_fail} failed   tokens in={total_in:,} out={total_out:,}")

    if args.apply and all_rows:
        print(f"   inserting {len(all_rows)} rows to beach_policy_extractions (run_id={run_id})")
        CHUNK = 50
        for i in range(0, len(all_rows), CHUNK):
            insert_extractions(all_rows[i:i + CHUNK], run_id)
        print(f"   done.")
    elif all_rows:
        out = SCRIPT_DIR / f"_extract_one_fid_{args.fid}.json"
        out.write_text(json.dumps([
            {**r, "raw_response": (r.get("raw_response") or "")[:200]}
            for r in all_rows
        ], indent=2, default=str), encoding="utf-8")
        print(f"   (dry-run) detail at {out}")

    return 0 if total_ok > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
