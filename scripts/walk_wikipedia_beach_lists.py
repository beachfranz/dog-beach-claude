"""walk_wikipedia_beach_lists.py — one-shot per-state Wikipedia walker.

Fetches en.wikipedia.org/wiki/List_of_beaches_in_<State>, LLM-extracts every
named beach with county + description, dedupes vs arena, stages into
public.beach_discovery_queue with source_handler='wikipedia_v1'.

Proof-of-yield protocol: test on CA + OR + FL first. If post-dedup yield
>=15 net-new famous beaches per state, expand to remaining coastal states.

Per Franz directive 2026-05-27: beach-only (asymmetric); skip dog parks
(Wikipedia coverage of dog parks too thin).

Usage:
  python scripts/walk_wikipedia_beach_lists.py --state CA --dry-run
  python scripts/walk_wikipedia_beach_lists.py --state CA --apply
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.common.db import connect  # noqa: E402
from scripts.common.llm import SONNET, call_json  # noqa: E402


# Wikipedia URL formats vary slightly per state. Try in order.
def candidate_urls(state_name: str) -> list[str]:
    s = state_name.replace(" ", "_")
    return [
        f"https://en.wikipedia.org/wiki/List_of_beaches_in_{s}",
        f"https://en.wikipedia.org/wiki/Beaches_of_{s}",
        f"https://en.wikipedia.org/wiki/Beaches_in_{s}",
        f"https://en.wikipedia.org/wiki/List_of_beaches_of_{s}",
    ]


STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut",
    "DE": "Delaware", "DC": "the_District_of_Columbia", "FL": "Florida",
    "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky",
    "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana",
    "NE": "Nebraska", "NV": "Nevada", "NH": "New_Hampshire",
    "NJ": "New_Jersey", "NM": "New_Mexico", "NY": "New_York",
    "NC": "North_Carolina", "ND": "North_Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode_Island", "SC": "South_Carolina", "SD": "South_Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West_Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}


UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def fetch_wiki(url: str) -> str | None:
    """Fetch Wikipedia article. Returns plain-text article body (stripped of
    tags/scripts) or None if 404."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as r:
            html = r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise

    # Crude HTML-to-text: keep article body only.
    # Wikipedia has #mw-content-text container; strip to that region.
    m = re.search(r'id="mw-content-text"(.*?)<div\s+id="catlinks"',
                  html, re.S | re.I)
    body = m.group(1) if m else html
    body = re.sub(r"<script\b[^>]*>.*?</script>", " ", body, flags=re.S | re.I)
    body = re.sub(r"<style\b[^>]*>.*?</style>", " ", body, flags=re.S | re.I)
    body = re.sub(r"<sup\b[^>]*>.*?</sup>", " ", body, flags=re.S | re.I)
    body = re.sub(r"<table\s+[^>]*class=\"[^\"]*navbox[^\"]*\"[^>]*>.*?</table>",
                  " ", body, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", body)
    text = (text.replace("&nbsp;", " ").replace("&amp;", "&")
                 .replace("&#39;", "'").replace("&quot;", '"'))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def resolve_article_url(state: str) -> tuple[str, str] | tuple[None, None]:
    """Try candidate URL formats; return (resolved_url, body_text) or
    (None, None) if no article exists for the state."""
    state_name = STATE_NAMES.get(state)
    if not state_name:
        raise SystemExit(f"Unknown state code: {state}")
    for url in candidate_urls(state_name):
        body = fetch_wiki(url)
        if body and len(body) > 1000:
            return url, body
    return None, None


# ── LLM extraction ──────────────────────────────────────────────────────

EXTRACT_SYSTEM = """\
You extract every NAMED BEACH listed in a Wikipedia article that catalogs
beaches in one US state. The article is organized by county or region; some
entries appear in bulleted lists, others in tables, and a few in prose.

INCLUDE: any feature listed as a beach, even if the proper noun is just the
name ("Gualala Point Regional Park", "Half Moon Bay State Beach", "Rosie's
Dog Beach", "Ocean Beach"). The article calling it out as a beach is itself
the evidence.

EXCLUDE:
- Section headings (county names, region names) — these are organizers.
- Counties or regions themselves ("Marin County").
- Lighthouses, piers, headlands, parks that don't include "Beach", unless
  the article literally labels them as a beach destination.
- "See also" lists, references, external links.
- Generic phrases without proper-noun anchors.

For each beach, capture the name as written + the county (taken from the
nearest preceding section heading) + a 1-sentence description if the article
gives one.

OUTPUT JSON ONLY (no markdown). May be long — list ALL beaches the article
documents. Do not abbreviate or omit.

{
  "beaches": [
    {
      "name": "<beach name as written>",
      "county": "<county name from section heading, no 'County' suffix; null if unknown>",
      "description": "<short snippet from article if available, else null>"
    },
    ...
  ]
}
"""


def llm_extract(state_name: str, article_text: str) -> list[dict]:
    # Article can be long; Wikipedia state lists are typically 20-80k chars.
    snippet = article_text[:80000]
    user = json.dumps({"state": state_name, "article_text": snippet},
                       ensure_ascii=False)
    # CA + FL articles can produce 100+ beach entries; 16k tokens gives
    # ~12k JSON chars headroom. Sonnet 4.6 supports much higher; raise
    # again if a state truncates.
    r = call_json(EXTRACT_SYSTEM, user, model=SONNET, max_tokens=16000)
    return r["parsed"].get("beaches", []) or []


# ── Dedup ───────────────────────────────────────────────────────────────

STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09",
    "DE":"10","DC":"11","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17",
    "IN":"18","IA":"19","KS":"20","KY":"21","LA":"22","ME":"23","MD":"24",
    "MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30","NE":"31",
    "NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38",
    "OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46",
    "TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
    "WI":"55","WY":"56",
}


def find_arena_match(cur, beach_name: str, state: str) -> dict | None:
    """Closest arena entry within the state by trigram name similarity.
    Falls back to nationwide arena (rare) if state FIPS unknown."""
    fips = STATE_FIPS.get(state, "")
    if fips:
        cur.execute("""
          SELECT a.fid, a.name,
                 similarity(coalesce(a.name,''), %s) AS sim
            FROM public.arena a
           WHERE a.county_fips LIKE %s
             AND similarity(coalesce(a.name,''), %s) > 0.5
           ORDER BY similarity(coalesce(a.name,''), %s) DESC
           LIMIT 1
        """, (beach_name, f"{fips}%", beach_name, beach_name))
    else:
        cur.execute("""
          SELECT a.fid, a.name,
                 similarity(coalesce(a.name,''), %s) AS sim
            FROM public.arena a
           WHERE similarity(coalesce(a.name,''), %s) > 0.5
           ORDER BY similarity(coalesce(a.name,''), %s) DESC
           LIMIT 1
        """, (beach_name, beach_name, beach_name))
    r = cur.fetchone()
    if r is None:
        return None
    return {"fid": int(r[0]), "name": r[1], "sim": float(r[2])}


# ── Main per-state walk ─────────────────────────────────────────────────

def walk(state: str, dry_run: bool, limit: int | None) -> dict:
    stats = {"beaches_extracted": 0, "rows_inserted": 0,
             "rows_dedup": 0, "rows_skipped_no_county": 0}

    url, body = resolve_article_url(state)
    if not url:
        print(f"[{state}] No Wikipedia 'List of beaches' article found.")
        return stats
    print(f"[{state}] article: {url}  ({len(body)} chars)")

    state_name = STATE_NAMES[state].replace("_", " ")
    print(f"[{state}] calling LLM (Sonnet) for enumeration...", flush=True)
    beaches = llm_extract(state_name, body)
    stats["beaches_extracted"] = len(beaches)
    print(f"[{state}] LLM extracted {len(beaches)} beach entries", flush=True)

    if limit:
        beaches = beaches[:limit]

    if dry_run:
        for b in beaches[:20]:
            print(f"  [dry] {b.get('name'):<45} county={b.get('county')}")
        if len(beaches) > 20:
            print(f"  ... ({len(beaches)-20} more)")
        return stats

    conn = connect(); conn.autocommit = False
    try:
        cur = conn.cursor()
        for b in beaches:
            raw_name = (b.get("name") or "").strip()
            county = (b.get("county") or "").strip() or None
            desc = (b.get("description") or "").strip() or None
            if not raw_name:
                continue
            # Strip Wikipedia disambiguation suffixes: ", State", " (State)",
            # " (City, State)", etc.
            name = raw_name
            name = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
            name = re.sub(r",\s*[A-Z][a-zA-Z\s]+$", "", name).strip()
            if not name:
                continue
            # Sanity: name must contain water/beach stem
            if not re.search(
                r"\b(beach|cove|bay|lagoon|swim|sand[ay]?|shore|strand|"
                r"pier|point|coast)\b", name, re.I):
                continue

            # Dedup vs arena (name-only; no coords from Wikipedia yet).
            match = find_arena_match(cur, name, state)
            dup_status = None
            best_fid = None
            best_sim = None
            if match and match["sim"] > 0.75:
                dup_status = "duplicate"
                best_fid = match["fid"]
                best_sim = match["sim"]

            # Parent_park_name is the county (Wikipedia organizes by county).
            parent = (f"{county} County" if county else "Unknown County")

            try:
                cur.execute("""
                  INSERT INTO public.beach_discovery_queue
                    (state, parent_park_name, beach_name,
                     verbatim_quote, source_url, source_handler,
                     best_similarity, best_match_fid, status, queue_meta)
                  VALUES (%s, %s, %s, %s, %s, 'wikipedia_v1',
                          %s, %s, %s, %s)
                  ON CONFLICT
                    (state, lower(parent_park_name),
                     lower(beach_name), source_handler)
                  DO UPDATE SET
                    verbatim_quote = excluded.verbatim_quote,
                    best_similarity = excluded.best_similarity,
                    best_match_fid = excluded.best_match_fid,
                    updated_at = now()
                """, (state, parent, name, desc, url,
                      best_sim, best_fid,
                      dup_status or "pending",
                      json.dumps({"county": county, "wiki_source": True})))
                if dup_status == "duplicate":
                    stats["rows_dedup"] += 1
                else:
                    stats["rows_inserted"] += 1
            except Exception as e:
                print(f"  [insert error] {name}: {e}")
                conn.rollback()
                continue
        conn.commit()
    finally:
        conn.close()

    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--apply", action="store_true",
                    help="(synonym for not-dry-run)")
    args = ap.parse_args()
    stats = walk(args.state, dry_run=args.dry_run, limit=args.limit)
    print("\n=== SUMMARY ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
