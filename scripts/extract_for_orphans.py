"""
extract_for_orphans.py — surgical per-orphan extraction.

Resumes the policy-extraction pipeline from the 2026-04-24 pause point
under the new arena-pipeline framing. Targets the 3 true orphans
(POI singletons in arena with no OSM polygon coverage) verified
2026-05-01.

Differences from extract_beach_policies.py:
  - Per-orphan input (not city fan-out)
  - Less-aggressive BS4 strip (parks.ca.gov, visit-halfmoonbay.org wrap
    real content inside header/aside; the aggressive strip in
    extract_beach_policies.py decomposes them)
  - Writes both legacy fid (us_beach_points/poi_landing.fid) AND
    arena_group_id to public.beach_policy_extractions

Usage:
  python scripts/extract_for_orphans.py            # dry-run, no DB write
  python scripts/extract_for_orphans.py --apply    # also write to DB
"""
from __future__ import annotations
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import psycopg2
from anthropic import Anthropic
from bs4 import BeautifulSoup
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

# Reuse some primitives
sys.path.insert(0, str(SCRIPT_DIR))
from extract_beach_policies import call_llm, parse_response, run_sql, sql_literal  # noqa

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "identity",
}
MAX_CONTENT_CHARS = 12000

# 3 true orphans (legacy us_beach_points.fid lookup happens at runtime)
ORPHANS = [
    {
        "arena_fid": 453,  "name": "Anita Street Beach",
        "city": "Laguna Beach",
        "url": "https://movingtolagunabeach.com/beaches/anita-street-beach/",
    },
    {
        "arena_fid": 2078, "name": "Roosevelt Beach",
        "city": "Half Moon Bay",
        "url": "https://visithalfmoonbay.org/listings/roosevelt-beach/",
    },
    {
        "arena_fid": 3671, "name": "Leo Carrillo State Beach",
        "city": "Malibu (LA County)",
        "url": "https://www.parks.ca.gov/?page_id=616",
    },
]

# 8 active consumer dog/CA beaches (Gap B run, 2026-05-01).
# arena_fid here is the canonical arena entity for the beach
# (singleton group_id after the dog-beach un-merges from Bolsa Chica/Coronado/Belmont Shore).
GAP_B_BEACHES = [
    {"arena_fid": 8606, "name": "Bolsa Chica State Beach", "city": "Huntington Beach",
     "url": "https://www.parks.ca.gov/bolsachica/"},
    {"arena_fid": 6202, "name": "Coronado Dog Beach", "city": "Coronado",
     "url": "https://www.coronado.ca.us/757/Dogs"},
    {"arena_fid": 8560, "name": "Del Mar Dog Beach", "city": "Del Mar",
     "url": "https://www.delmar.ca.us/206/Dog-Friendly-Beaches"},
    {"arena_fid": 6212, "name": "Huntington Dog Beach", "city": "Huntington Beach",
     "url": "https://www.dogbeach.org/faq"},
    {"arena_fid": 8901, "name": "Huntington City Beach", "city": "Huntington Beach",
     "url": "https://www.surfcityusa.com/things-to-do/beaches/huntington-city-beach/"},
    {"arena_fid": 8453, "name": "Huntington State Beach", "city": "Huntington Beach",
     "url": "https://www.parks.ca.gov/huntington/"},
    {"arena_fid": 8358, "name": "Ocean Beach Dog Beach", "city": "San Diego",
     "url": "https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog"},
    {"arena_fid": 6411, "name": "Rosie's Dog Beach", "city": "Long Beach",
     "url": "https://www.longbeach.gov/park/park-and-facilities/directory/rosies-dog-beach"},
]

# Pick which set to run via env var; default to the original 3
if os.environ.get("EXTRACT_SET") == "gap_b":
    ORPHANS = GAP_B_BEACHES


def fetch_html(url: str, timeout: int = 20) -> str | None:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "html" not in ct.lower():
                return None
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except Exception as e:
        print(f"  fetch failed ({url}): {e}", file=sys.stderr)
        return None


def bs4_strip_loose(html: str) -> str:
    """Permissive strip: remove only script/style/noscript/iframe.
    Keep nav/header/aside/etc. — modern sites wrap real content in those."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()
    text = (soup.body or soup).get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    cleaned = "\n".join(lines)
    return cleaned[:MAX_CONTENT_CHARS]


def lookup_legacy_fid_and_group(arena_fid: int) -> tuple[int | None, int | None]:
    """Return (us_beach_points_fid, arena_group_id) for a given arena_fid.
    POI-anchored: parse fid from source_id 'poi/<fid>'.
    OSM-anchored: borrow a sibling POI's fid from the same group."""
    rows = run_sql(f"""
        select source_code, source_id, group_id
          from public.arena where fid = {arena_fid};
    """)
    if not rows:
        return None, None
    src_code, src_id, gid = rows[0]["source_code"], rows[0]["source_id"] or "", rows[0]["group_id"]

    if src_code == "poi" and src_id.startswith("poi/"):
        return int(src_id.split("/", 1)[1]), gid

    # OSM-anchored: find any POI in the same group, take its fid
    sib = run_sql(f"""
        select source_id from public.arena
         where group_id = {gid} and source_code = 'poi'
         order by fid asc limit 1;
    """)
    if sib and sib[0]["source_id"].startswith("poi/"):
        return int(sib[0]["source_id"].split("/", 1)[1]), gid

    # No POI in group — synthesize a value from arena_fid (negative to avoid
    # collision with real us_beach_points.fid). Schema mod alternative.
    return -arena_fid, gid


def insert_extractions(rows: list[dict], run_id: str):
    if not rows:
        return
    values = ",\n".join(
        "(" + ", ".join([
            sql_literal(r["fid"]),
            sql_literal(r["arena_group_id"]),
            sql_literal(r["source_id"]),
            sql_literal(r["variant_id"]),
            sql_literal(r["field_name"]),
            sql_literal(r["source_type"]),
            sql_literal(r["variant_key"]),
            sql_literal(r["raw_response"]),
            sql_literal(r["parsed_value"]),
            sql_literal(r["evidence_quote"]),
            sql_literal(r["raw_snippet"]),
            sql_literal(r["parse_succeeded"]),
            sql_literal("llm_hybrid"),
            sql_literal(run_id),
            sql_literal(r["model_name"]),
            sql_literal(r["input_tokens"]),
            sql_literal(r["output_tokens"]),
            sql_literal(r["latency_ms"]),
        ]) + ")"
        for r in rows
    )
    sql = f"""
        insert into public.beach_policy_extractions (
          fid, arena_group_id, source_id, variant_id, field_name,
          source_type, variant_key, raw_response, parsed_value,
          evidence_quote, raw_snippet, parse_succeeded,
          extraction_method, run_id, model_name,
          input_tokens, output_tokens, latency_ms
        ) values
        {values};
    """
    run_sql(sql)


def upsert_source(url: str) -> int:
    """Insert or get an entry in city_policy_sources for this URL.
    Use a synthetic place_fips of '06ORPH' so we don't conflict with city sources.
    Returns the source id."""
    existing = run_sql(f"""
        select id from public.city_policy_sources
         where url = {sql_literal(url)} limit 1;
    """)
    if existing:
        return existing[0]["id"]
    inserted = run_sql(f"""
        insert into public.city_policy_sources
          (place_fips, source_type, url, title, notes, curated_by)
        values ('06ORPH', 'other', {sql_literal(url)},
                {sql_literal('orphan extraction page')},
                'Discovered for arena-pipeline orphan extraction',
                'extract_for_orphans.py')
        returning id;
    """)
    return inserted[0]["id"]


def main() -> int:
    apply = "--apply" in sys.argv
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        sys.exit("ANTHROPIC_API_KEY missing")
    client = Anthropic(api_key=api_key)

    variants = run_sql("""
        select id, field_name, variant_key, prompt_template,
               expected_shape, target_model
        from public.extraction_prompt_variants
        where active = true
        order by target_model, field_name, variant_key;
    """)
    print(f"loaded {len(variants)} active variants")

    run_id = f"orphan-{uuid.uuid4().hex[:8]}"
    print(f"run_id = {run_id}\n")

    rows_to_insert: list[dict] = []
    summary = []

    for orphan in ORPHANS:
        print(f"══ {orphan['name']} (arena fid {orphan['arena_fid']}) ══")
        legacy_fid, arena_group_id = lookup_legacy_fid_and_group(orphan["arena_fid"])
        if legacy_fid is None:
            print(f"   ⚠ no legacy fid found for arena fid {orphan['arena_fid']}; skipping")
            continue
        print(f"   legacy_fid={legacy_fid}  arena_group_id={arena_group_id}")
        print(f"   url: {orphan['url']}")

        html = fetch_html(orphan["url"])
        if not html:
            print(f"   ⚠ fetch failed; skipping")
            continue
        page = bs4_strip_loose(html)
        print(f"   page content: {len(page)} chars")
        if len(page) < 200:
            print(f"   ⚠ page too small after strip; skipping")
            continue

        source_id = upsert_source(orphan["url"]) if apply else -1

        n_ok = 0
        n_fail = 0
        consensus = {}  # field → list of unique parsed values
        for v in variants:
            try:
                resp = call_llm(client, v["target_model"], v["prompt_template"], page)
            except Exception as e:
                print(f"   {v['field_name']}/{v['variant_key']}: API error {e}")
                n_fail += 1
                continue
            parsed = parse_response(resp["raw"], v["expected_shape"])
            if parsed["parse_succeeded"]:
                n_ok += 1
                consensus.setdefault(v["field_name"], []).append(parsed["parsed_value"])
            else:
                n_fail += 1
            rows_to_insert.append({
                "fid": legacy_fid,
                "arena_group_id": arena_group_id,
                "source_id": source_id,
                "variant_id": v["id"],
                "field_name": v["field_name"],
                "source_type": "orphan_page",
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

        print(f"   variants: {n_ok} parsed OK, {n_fail} failed")
        # Top-level field summary (1-2 most common parsed values)
        for field, vals in sorted(consensus.items()):
            unique = list(dict.fromkeys(str(v) for v in vals if v is not None))
            disp = " | ".join(u[:50] for u in unique[:2])
            print(f"     {field:30}  {disp}")
        summary.append({"orphan": orphan, "legacy_fid": legacy_fid,
                        "arena_group_id": arena_group_id,
                        "n_ok": n_ok, "n_fail": n_fail,
                        "consensus": consensus})
        print()

    # Cost
    total_in  = sum(r["input_tokens"]  for r in rows_to_insert)
    total_out = sum(r["output_tokens"] for r in rows_to_insert)
    print(f"Total tokens: input={total_in:,}  output={total_out:,}")

    if apply and rows_to_insert:
        print(f"\nInserting {len(rows_to_insert)} rows to beach_policy_extractions...")
        # Chunk to stay under HTTP payload cap
        CHUNK = 50
        for i in range(0, len(rows_to_insert), CHUNK):
            insert_extractions(rows_to_insert[i:i+CHUNK], run_id)
            print(f"  inserted chunk {i//CHUNK + 1} ({min(CHUNK, len(rows_to_insert)-i)} rows)")
        print("Done.")
    elif rows_to_insert:
        out = SCRIPT_DIR / "_orphan_extraction.json"
        out.write_text(json.dumps([
            {**r, "raw_response": (r.get("raw_response") or "")[:200]}
            for r in rows_to_insert
        ], indent=2, default=str), encoding="utf-8")
        print(f"\n(dry-run; rerun with --apply to insert. Detail at {out})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
