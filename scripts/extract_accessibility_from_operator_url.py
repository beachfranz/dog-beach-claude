"""Extract ADA accessibility signals from operator URLs (operator_dogs_policy.source_url)
for the 10 states with operator-attribution but no state_park_urls.json loader entry
(MA / MI / WA / OR / OH / MD / RI / VA / NH / DE — and CA as additive).

Architecture:
  1. Pull (gold_fid, operator_url) pairs from
     beach_operator -> operators -> operator_dogs_policy.source_url
  2. Group by URL — dedup means one fetch+Haiku covers ~6.75 fids on average
  3. For each unique URL: live BS4-strip; if no ADA keywords, skip
  4. If ADA keywords found: send to Haiku with the same JSON shape as
     extract_accessibility_from_park_url.py
  5. Write BEP rows (one per gold_fid sharing that URL) with
     source='operator_url'
  6. After --apply: fire resolver + promote sweep so consumer surface
     populates per [[non-dogs-bep-needs-manual-resolver]]

Usage:
   python scripts/extract_accessibility_from_operator_url.py --state DE          # preview
   python scripts/extract_accessibility_from_operator_url.py --state DE --apply  # write
   python scripts/extract_accessibility_from_operator_url.py --states MA,MI,WA --apply
   python scripts/extract_accessibility_from_operator_url.py --dry-run --state DE
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bs4 import BeautifulSoup

from scripts.common.db import connect
from scripts.common.llm import HAIKU

ROOT = Path(__file__).resolve().parent.parent

ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = HAIKU
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
MAX_CONTENT_CHARS = 14000

ADA_KEYWORDS = r"\m(wheelchair|ADA|accessible|accessibility|mobi.?mat|handicap|disabled access)\M"

# Same prompt as extract_accessibility_from_park_url.py — proven; same BEP shape.
PROMPT = """Extract accessibility (ADA / wheelchair) signals from this park/beach website text.

Output ONE JSON object with these keys. Use null when not explicitly stated. DO NOT INFER. DO NOT HALLUCINATE.

{
  "accessible_parking":   true | false | null,
  "accessible_restrooms": true | false | null,
  "path_to_sand":         "boardwalk" | "ramp" | "mat" | "stairs_only" | "none" | null,
  "beach_mat":            true | false | null,
  "wheelchair_rental":    {"available": true, "provider": "...", "url": "..."} | null,
  "accessible_viewing":   true | false | null,
  "notes":                "short paraphrase of specific access detail" | null
}

Rules:
- "accessible_parking": true only if text mentions ADA/wheelchair/accessible parking specifically. Generic "parking available" -> null.
- "accessible_restrooms": true only if text mentions ADA/wheelchair restrooms specifically.
- "path_to_sand": pick ONE value if mentioned. "boardwalk" for boardwalk/wood paths to beach; "ramp" for paved ramps; "mat" for beach mats (Mobi-Mat etc.); "stairs_only" if only stairs; "none" if no accessible path.
- "beach_mat": true if text mentions beach mat / Mobi-Mat / sand mat installed. Specifically for sand access.
- "wheelchair_rental": fill if text mentions beach-wheelchair / floating-chair / similar rental program. Provider = organization name. URL if given.
- "accessible_viewing": true only if text mentions accessible overlook/viewing platform/ADA-accessible vista distinct from accessing the beach itself.
- "notes": short paraphrased detail. Only include if it adds specific accessible-access info not captured by other fields.

Output ONLY the JSON object. No commentary, no markdown code fence.

Text:
%s
"""


# ───────────────────────── HTTP + LLM ─────────────────────────

def fetch_html(url: str, timeout: int = 30) -> str | None:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "html" not in ct.lower():
                return None
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except Exception as e:
        print(f"    fetch failed: {str(e)[:120]}", flush=True)
        return None


def bs4_strip(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "header", "footer", "nav"]):
        tag.decompose()
    text = (soup.body or soup).get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    return "\n".join(lines)[:MAX_CONTENT_CHARS]


def call_haiku(text: str) -> tuple[dict | None, dict]:
    body = {
        "model": MODEL,
        "max_tokens": 500,
        "messages": [{"role": "user", "content": PROMPT % text}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(), method="POST",
        headers={"x-api-key": ANTHROPIC_KEY,
                 "anthropic-version": "2023-06-01",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            resp = json.loads(r.read())
    except Exception as e:
        return None, {"_error": str(e)[:120]}
    out = "".join(p["text"] for p in resp["content"] if p["type"] == "text").strip()
    if out.startswith("```"):
        out = out.split("```", 2)[1]
        if out.startswith("json\n"):
            out = out[5:]
        out = out.rstrip("` \n")
    try:
        return json.loads(out), resp.get("usage", {})
    except json.JSONDecodeError as e:
        return None, {**resp.get("usage", {}), "_parse_error": str(e)[:120]}


# ───────────────────────── Picker ─────────────────────────

def fetch_targets(states: list[str]) -> list[tuple[int, str]]:
    """Return [(gold_fid, source_url), ...] for active fids in target states
    that have an operator with a dog policy source URL."""
    with connect() as c, c.cursor() as cur:
        cur.execute("""
            SELECT bo.beach_fid, odp.source_url
              FROM beaches_gold g
              JOIN beach_operator bo ON bo.beach_fid = g.fid
              JOIN operator_dogs_policy odp ON odp.operator_id = bo.operator_id
             WHERE g.is_active
               AND g.state = ANY(%s::text[])
               AND odp.source_url IS NOT NULL
             ORDER BY odp.source_url, bo.beach_fid
        """, (states,))
        return [(int(r[0]), r[1]) for r in cur.fetchall()]


# ───────────────────────── BEP write ─────────────────────────

def write_bep(fids: list[int], url: str, claimed: dict) -> tuple[int, int]:
    """Upsert BEP rows for each fid sharing the URL."""
    cleaned = {k: v for k, v in claimed.items() if v is not None}
    if not cleaned:
        return 0, 0
    inserted = updated = 0
    with connect() as c, c.cursor() as cur:
        for fid in fids:
            cur.execute("""
                INSERT INTO public.beach_enrichment_provenance
                  (gold_fid, field_group, source, source_url, claimed_values,
                   confidence, is_canonical, extraction_type, updated_at)
                VALUES (%s, 'accessibility', 'operator_url', %s, %s::jsonb,
                        0.78, false, 'derived_url_crawl', now())
                ON CONFLICT (gold_fid, field_group, source) DO UPDATE
                  SET claimed_values = excluded.claimed_values,
                      confidence     = excluded.confidence,
                      source_url     = excluded.source_url,
                      updated_at     = now()
                  WHERE beach_enrichment_provenance.claimed_values IS DISTINCT FROM excluded.claimed_values
                RETURNING (xmax = 0) AS inserted
            """, (fid, url, json.dumps(cleaned)))
            r = cur.fetchone()
            if r is None:
                continue
            if r[0]:
                inserted += 1
            else:
                updated += 1
        c.commit()
    return inserted, updated


def fire_resolver_promote_sweep() -> None:
    """Per [[non-dogs-bep-needs-manual-resolver]]: trigger doesn't auto-flip canonical."""
    print("  Firing resolver + promote sweep for accessibility ...", flush=True)
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT public._resolve_field_group_gold('accessibility', NULL)")
        flipped = cur.fetchone()[0]
        cur.execute("SELECT rows_inserted, rows_updated FROM public.promote_canonical_accessibility_to_beach_amenities(NULL)")
        ins, upd = cur.fetchone()
        c.commit()
    print(f"  resolver: {flipped} canonical rows | promote: ins={ins} upd={upd}", flush=True)


# ───────────────────────── Main ─────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", help="single state code (e.g. DE)")
    ap.add_argument("--states", help="comma-separated states (e.g. MA,MI,WA)")
    ap.add_argument("--apply", action="store_true", help="write BEP rows")
    ap.add_argument("--dry-run", action="store_true", help="just print URL/fid map")
    ap.add_argument("--limit-urls", type=int, default=None, help="cap URLs processed")
    args = ap.parse_args()

    if args.state and args.states:
        sys.exit("pass --state OR --states, not both")
    if not args.state and not args.states:
        sys.exit("must pass --state or --states")
    states = [args.state.upper()] if args.state else [s.strip().upper() for s in args.states.split(",")]

    print(f"  picker scope: states={states}", flush=True)
    pairs = fetch_targets(states)
    if not pairs:
        print("  no (fid, url) pairs found.")
        return 0
    by_url: dict[str, list[int]] = defaultdict(list)
    for fid, url in pairs:
        by_url[url].append(fid)
    urls = sorted(by_url.keys())
    if args.limit_urls:
        urls = urls[: args.limit_urls]
    total_fids = sum(len(by_url[u]) for u in urls)
    print(f"  {len(urls)} unique URLs covering {total_fids} fids", flush=True)

    if args.dry_run:
        for url in urls[:30]:
            print(f"    [{len(by_url[url])} fids]  {url}")
        return 0

    # Pre-filter URLs by ADA-keyword test on the fetched page
    import re
    ada_re = re.compile(ADA_KEYWORDS.replace(r"\m", r"\b").replace(r"\M", r"\b"), re.IGNORECASE)

    in_tot = out_tot = 0
    pages_with_ada = pages_no_ada = pages_failed = 0
    haiku_called = haiku_parsed = 0
    emitted_fids = 0
    inserts_tot = updates_tot = 0
    skipped_no_keyword = 0
    t0 = time.time()

    for i, url in enumerate(urls, 1):
        fids = by_url[url]
        print(f"  [{i:3d}/{len(urls)}] {url}  fids={len(fids)}", flush=True)
        html = fetch_html(url)
        if not html:
            pages_failed += 1
            continue
        page = bs4_strip(html)
        if not ada_re.search(page):
            pages_no_ada += 1
            skipped_no_keyword += len(fids)
            print(f"    no ADA keyword in {len(page)} chars, skip", flush=True)
            continue
        pages_with_ada += 1
        haiku_called += 1
        parsed, usage = call_haiku(page)
        in_tot += usage.get("input_tokens", 0) or 0
        out_tot += usage.get("output_tokens", 0) or 0
        if not parsed:
            err = usage.get("_error") or usage.get("_parse_error") or "unknown"
            print(f"    HAIKU FAIL: {err}", flush=True)
            continue
        haiku_parsed += 1
        non_null = {k: v for k, v in parsed.items() if v is not None}
        if not non_null:
            print(f"    haiku returned all-null, skip", flush=True)
            continue
        print(f"    haiku ok  fields={list(non_null.keys())}", flush=True)
        if args.apply:
            ins, upd = write_bep(fids, url, parsed)
            inserts_tot += ins
            updates_tot += upd
            emitted_fids += ins + upd
        else:
            emitted_fids += len(fids)  # preview-mode estimate

    cost = in_tot * 1 / 1e6 + out_tot * 5 / 1e6  # Haiku 4.5 pricing
    print(f"\n  === DONE ===")
    print(f"  urls: {len(urls)} total | ada_keyword:{pages_with_ada} no_ada:{pages_no_ada} fetch_fail:{pages_failed}")
    print(f"  haiku: called={haiku_called} parsed={haiku_parsed}")
    print(f"  fids: {'inserted/updated' if args.apply else 'would_emit'}={emitted_fids} skipped_no_keyword={skipped_no_keyword}")
    if args.apply:
        print(f"  BEP: ins={inserts_tot} upd={updates_tot}")
    print(f"  tokens: in={in_tot} out={out_tot}  est_cost=${cost:.4f}  wall={time.time()-t0:.1f}s")

    if args.apply and (inserts_tot + updates_tot) > 0:
        fire_resolver_promote_sweep()

    return 0


if __name__ == "__main__":
    sys.exit(main())
