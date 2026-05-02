"""
extract_for_gold_v3.py — run v3 canonical extractions for the 25 gold-set
candidates.

Targets the canonical variants added in 2026-05-02 migrations:
  has_sections, sections, feature_zones, leash_policy, temporal_restrictions,
  evidence_quote, confidence, lifeguard, restrooms, outdoor_showers

URLs resolved per beach by:
  1. existing beach_policy_extractions sources (most reliable — already proven)
  2. park_url_extractions
  3. URL_OVERRIDES dict in this file (manual fill-in)

Beaches without URL after all three are SKIPPED with a warning.

Optimization: sections variant only runs when archetype=geographic_sections
(the other 20 beaches are flagged as single-zone in the picker).
feature_zones runs on all beaches.

Usage:
  python scripts/extract_for_gold_v3.py            # dry-run: print plan, no LLM calls
  python scripts/extract_for_gold_v3.py --apply    # fetch HTML + run LLM + write to DB
"""
from __future__ import annotations
import argparse
import os
import sys
import urllib.parse
import uuid
from pathlib import Path

import psycopg2
import psycopg2.extras
from anthropic import Anthropic
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
sys.path.insert(0, str(SCRIPT_DIR))

# Reuse primitives
from extract_for_orphans import (
    fetch_html, bs4_strip_loose, lookup_legacy_fid_and_group,
    insert_extractions, upsert_source,
)
from extract_beach_policies import call_llm, parse_response  # noqa

ANTHROPIC = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

# Manual URL overrides for beaches whose auto-resolved URL is wrong / missing.
# Add entries here when curator review surfaces a bad URL.
URL_OVERRIDES = {
    2058: "https://www.newportbeachca.gov/recreation/visitors-guide/beaches",  # Cameo Shores — private gated, no dedicated page; city overview
    8264: "https://www.hermosabeach.gov/about-us/beach",                        # Hermosa City Beach
    8268: "https://beaches.lacounty.gov/torrance-beach/",                      # Torrance County Beach
    8316: "https://ocparks.com/beaches/saltcreek/",                             # Salt Creek Beach (OC Parks)
    # The auto-resolved Carlsbad Lagoon Dog Beach URL was for an ecological
    # reserve, not the dog beach. Pin to the real source:
    6622: "https://carlsbadca.gov/departments/parks-recreation/parks-trails/specialty-areas",
    # Manhattan Beach — auto URL was truncated; pin to LA County dbh page
    8265: "https://beaches.lacounty.gov/manhattan-beach/",
    # Newport Municipal Beach — auto URL pointed to a state beach (wrong);
    # use the city's beach page
    9069: "https://www.newportbeachca.gov/recreation/visitors-guide/beaches",
    # South Mission Beach — pin to lifeguards/beaches/sm rather than missionbay
    8357: "https://www.sandiego.gov/lifeguards/beaches/mb",
}

# Field set per archetype: sections runs ONLY for geographic_sections beaches
ALWAYS_FIELDS = [
    "has_sections", "feature_zones", "leash_policy", "temporal_restrictions",
    "evidence_quote", "confidence", "lifeguard", "restrooms", "outdoor_showers",
]
SECTION_BEACHES_FIELDS = ALWAYS_FIELDS + ["sections"]


def load_v3_beaches(cur, set_name="v3"):
    cur.execute("""
        with v3 as (
          select g.fid, g.name, m.archetype
            from public.beaches_gold g
            join public.gold_set_membership m on m.fid = g.fid
           where m.set_name = %s and not m.excluded
        )
        select v3.fid, v3.name, v3.archetype,
               (select cps.url
                  from public.beach_policy_extractions e
                  join public.city_policy_sources cps on cps.id = e.source_id
                 where e.arena_group_id = v3.fid
                 group by cps.url
                 order by count(*) desc limit 1) as best_existing_url,
               (select source_url
                  from public.park_url_extractions
                 where arena_group_id = v3.fid
                 order by scraped_at desc limit 1) as park_url
          from v3
         order by v3.fid
    """, (set_name,))
    out = []
    for r in cur.fetchall():
        url = URL_OVERRIDES.get(r["fid"]) or r["best_existing_url"] or r["park_url"]
        out.append({
            "arena_fid": r["fid"], "name": r["name"], "archetype": r["archetype"],
            "url": url,
        })
    return out


def load_canonical_variants(cur, field_names: list[str]):
    """Return list of {id, field_name, variant_key, prompt_template, expected_shape, target_model}
    for each canonical, active variant of the requested fields."""
    cur.execute("""
        select id, field_name, variant_key, prompt_template, expected_shape,
               coalesce(target_model, 'claude-sonnet-4-6') as target_model
          from public.extraction_prompt_variants
         where active and is_canon and field_name = any(%s)
    """, (field_names,))
    return list(cur.fetchall())


def get_or_create_source(cur, url: str) -> int:
    """Find or create a city_policy_sources row for this URL.
    Returns its id (used as beach_policy_extractions.source_id).

    source_type CHECK constraint allows: city_official, city_beaches,
    city_dog_policy, city_muni_code, visitor_bureau, visitor_bureau_beaches,
    other. We use 'other' since v3 sources span city / state / county / private."""
    cur.execute("select id from public.city_policy_sources where url = %s limit 1", (url,))
    r = cur.fetchone()
    if r:
        return r["id"]
    cur.execute("""
        insert into public.city_policy_sources (url, source_type, place_fips, curated_by, notes)
        values (%s, 'other', '06ORPH', 'extract_for_gold_v3', 'auto-created during gold v3 run')
        returning id
    """, (url,))
    return cur.fetchone()["id"]


def already_extracted_pairs(cur, fids: list[int]) -> set[tuple[int, int]]:
    """Return the set of (arena_group_id, variant_id) pairs that already
    have a successful extraction row, so we can skip them on re-runs."""
    if not fids:
        return set()
    cur.execute("""
        select arena_group_id, variant_id
          from public.beach_policy_extractions
         where arena_group_id = any(%s) and variant_id is not null
    """, (fids,))
    return {(r["arena_group_id"], r["variant_id"]) for r in cur.fetchall()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set-name", default="v3",
                    help="Gold set to extract for (gold_set_membership.set_name). Default 'v3'.")
    ap.add_argument("--apply", action="store_true",
                    help="Fetch HTML, call LLM, write to DB. Default is dry-run.")
    ap.add_argument("--include-all-canon", action="store_true",
                    help="Run every canonical variant (not just the v3-specific ALWAYS_FIELDS list). Use this when adding cross-model partners.")
    args = ap.parse_args()

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    beaches = load_v3_beaches(cur, set_name=args.set_name)
    if args.include_all_canon:
        # Run every active canonical variant — picks up cross-model partners.
        cur.execute("""
            select id, field_name, variant_key, prompt_template, expected_shape,
                   coalesce(target_model, 'claude-sonnet-4-6') as target_model
              from public.extraction_prompt_variants
             where active and is_canon
        """)
        variants = list(cur.fetchall())
    else:
        variants = load_canonical_variants(cur, ALWAYS_FIELDS + ["sections"])
    by_field = {}
    for v in variants:
        by_field.setdefault(v["field_name"], []).append(v)

    print(f"\nv3 beaches: {len(beaches)}")
    print(f"v3 fields with canonical variants: {sorted(by_field.keys())}\n")

    skipped = []
    plan = []
    for b in beaches:
        if not b["url"]:
            skipped.append(b)
            continue
        if args.include_all_canon:
            # Use every canonical field that has variants
            fields = sorted(by_field.keys())
        else:
            fields = SECTION_BEACHES_FIELDS if b["archetype"] == "geographic_sections" else ALWAYS_FIELDS
        plan.append({**b, "fields": fields})

    # Idempotency: skip (beach, variant_id) pairs already extracted
    fids = [b["arena_fid"] for b in plan]
    already = already_extracted_pairs(cur, fids)
    skip_count = 0
    for b in plan:
        b["calls"] = []
        for fname in b["fields"]:
            for variant in by_field.get(fname, []):
                if (b["arena_fid"], variant["id"]) in already:
                    skip_count += 1
                    continue
                b["calls"].append((fname, variant))

    print(f"Plan ({args.set_name}):")
    for b in plan:
        n_call = len(b["calls"])
        marker = "" if n_call else " (already complete — skip)"
        print(f"  fid={b['arena_fid']:>5}  {b['name'][:32]:<32}  {n_call:>3} new calls{marker}  url={b['url'][:50]}")
    if skipped:
        print(f"\nSkipped (no URL):")
        for b in skipped:
            print(f"  fid={b['arena_fid']:>5}  {b['name']}")

    total_calls = sum(len(b["calls"]) for b in plan)
    print(f"\nTotal new LLM calls: {total_calls} (skipping {skip_count} already-extracted pairs)")
    print(f"Estimated cost with caching: ${total_calls * 0.005:.2f}  (mix of haiku/sonnet/opus)")

    if not args.apply:
        print("\n(dry-run; rerun with --apply)")
        return 0

    # --- Live run ---
    run_id = str(uuid.uuid4())
    print(f"\nrun_id = {run_id}")
    rows_to_insert = []
    fetched_html = {}  # url -> html text (so multiple fields per beach reuse one fetch)

    for b in plan:
        url = b["url"]
        fid = b["arena_fid"]
        if not b["calls"]:
            continue
        print(f"\n[{fid}] {b['name']} ({len(b['calls'])} new calls)")
        if url not in fetched_html:
            html = fetch_html(url)
            if not html:
                print(f"  fetch failed; skipping all fields")
                continue
            fetched_html[url] = bs4_strip_loose(html)
        content = fetched_html[url]

        # Post-path-3: beach_policy_extractions.fid → beaches_gold.fid (same key
        # space as arena_group_id). lookup_legacy_fid_and_group returns the
        # OLD us_beach_points.fid which doesn't satisfy the new FK; skip it.
        source_id = upsert_source(url)

        for fname, variant in b["calls"]:
                vk = variant["variant_key"]
                vid = variant["id"]
                model = variant["target_model"]
                prompt = variant["prompt_template"]
                shape = variant["expected_shape"]
                try:
                    resp = call_llm(ANTHROPIC, model, prompt, content)
                except Exception as e:
                    print(f"  [{fname}/{vk}] LLM error: {e}")
                    continue
                raw = resp["raw"]
                parsed = parse_response(raw, shape)
                rows_to_insert.append({
                    "fid": fid,                  # post-path-3 == arena_group_id
                    "arena_group_id": fid,
                    "source_id": source_id,
                    "source_type": "other",
                    "variant_id": vid,
                    "variant_key": vk,
                    "field_name": fname,
                    "raw_response": raw,
                    "parsed_value": parsed.get("parsed_value"),
                    "evidence_quote": parsed.get("evidence_quote"),
                    "raw_snippet": "",
                    "parse_succeeded": parsed.get("parse_succeeded", False),
                    "extraction_method": "extract_for_gold_v3",
                    "run_id": run_id,
                    "model_name": model,
                    "input_tokens": resp.get("input_tokens", 0),
                    "output_tokens": resp.get("output_tokens", 0),
                    "latency_ms": resp.get("latency_ms", 0),
                    "error": None,
                })
                pv_short = str(parsed.get("parsed_value") or "")[:60].replace("\n", " ")
                print(f"  [{fname}/{vk}] {model.split('-')[1]} -> {pv_short!r}")

        # Flush rows for this beach (note: outdented from inner for-loop)
        if rows_to_insert:
            insert_extractions(rows_to_insert, run_id)
            print(f"  flushed {len(rows_to_insert)} rows")
            rows_to_insert = []

    print(f"\nDone. run_id = {run_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
