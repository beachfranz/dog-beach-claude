"""
scrape_bringfido.py
-------------------
Scrape BringFido.com's California dog-friendly-beaches directory into
public.truth_external for COMPARISON ONLY (never merged into the model).

Pipeline:
  1. Discover attraction IDs by walking ?page=N on the CA index until empty
  2. For each attraction: Tavily-extract the detail page
  3. Haiku classifies dog rule (off_leash / leash / yes / no / unknown)
     from the AI-generated summary + first review snippets
  4. Upsert into truth_external (source='bringfido', source_id=<id>)

Idempotent — re-running skips IDs already present unless --refresh.
"""
from __future__ import annotations
import argparse, json, os, re, sys, time
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "pipeline" / ".env")
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SERVICE_KEY       = os.environ["SUPABASE_SERVICE_KEY"]
TAVILY_API_KEY    = os.environ["TAVILY_API_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
HAIKU             = "claude-haiku-4-5-20251001"

INDEX_URL = "https://www.bringfido.com/attraction/beaches/state/california/"
SLEEP_S   = 1.0  # politeness


def tavily_extract(url: str) -> str:
    try:
        r = httpx.post("https://api.tavily.com/extract",
            json={"api_key": TAVILY_API_KEY, "urls":[url]}, timeout=90)
        r.raise_for_status()
        res = r.json().get("results", [])
        return res[0].get("raw_content", "") if res else ""
    except Exception as e:
        print(f"    tavily error on {url}: {type(e).__name__}: {e}", file=sys.stderr)
        return ""


def discover_ids(max_pages: int = 30) -> list[str]:
    """Walk paginated index. Stop on empty page."""
    seen: set[str] = set()
    ordered: list[str] = []
    for p in range(1, max_pages + 1):
        url = INDEX_URL + (f"?page={p}" if p > 1 else "")
        text = tavily_extract(url)
        ids_on_page = re.findall(r'/attraction/(\d+)', text)
        ids_on_page = [i for i in ids_on_page if i not in seen]
        if not ids_on_page:
            print(f"  page {p}: empty — stopping discovery")
            break
        for i in ids_on_page:
            seen.add(i); ordered.append(i)
        print(f"  page {p}: +{len(ids_on_page)} (total {len(ordered)})")
        time.sleep(SLEEP_S)
    return ordered


CLASSIFY_SYSTEM = """You read a BringFido.com beach detail page and classify the dog-access rule.

CONTEXT: BringFido is a dog-friendly travel directory. If a beach is listed at all, dogs are allowed there — that's the editorial bar. So default to "yes" unless evidence points elsewhere.

Return ONLY JSON: {"rule": "off_leash"|"leash"|"yes"|"no"|"unknown", "evidence": "<one short sentence quoted from the page>"}

Rules:
- "off_leash" → page or reviews mention off-leash, no-leash, free-running, dog-park-like
- "leash" → page or reviews explicitly state leash required / dogs must be leashed
- "yes" → beach is positively listed but leash specifics aren't mentioned. DEFAULT for listed beaches.
- "no" → page explicitly states dogs are prohibited (rare on BringFido)
- "unknown" → no usable signal AND not a clear listing (e.g., text is empty/error)

If reviews mention dogs running off-leash repeatedly, pick "off_leash".
If the page describes BOTH leashed and off-leash zones, pick "off_leash" — it's the more-permissive distinguishing fact.
If the page is thin but the beach IS listed (has a name/address/reviews), prefer "yes" over "unknown".
The "evidence" must be a verbatim quote (≤180 chars) from the page text, or "(listed without explicit policy)" if no quote applies."""


def call_haiku(page_text: str, name: str) -> dict:
    # Skip UI chrome before the H1; sample around the first H1 + Reviews block
    h1_idx = page_text.find(f"# {name}")
    body = page_text[h1_idx:] if h1_idx >= 0 else page_text
    snippet = body[:10000]
    user = f"Beach name: {name}\n\nPage text:\n{snippet}"
    for attempt in range(3):
        try:
            r = httpx.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY,
                         "anthropic-version":"2023-06-01",
                         "content-type":"application/json"},
                json={"model": HAIKU, "max_tokens": 200, "system": CLASSIFY_SYSTEM,
                      "messages":[{"role":"user","content":user}]},
                timeout=60)
            if r.status_code >= 500 or r.status_code == 429:
                time.sleep(2 ** attempt * 2); continue
            r.raise_for_status()
            text = r.json()["content"][0]["text"].strip()
            text = re.sub(r"^```(?:json)?\s*|\s*```\s*$", "", text)
            # Extract first JSON object — Haiku sometimes adds trailing prose
            m = re.search(r'\{[^{}]*\}', text, re.DOTALL)
            if m:
                text = m.group(0)
            return json.loads(text)
        except Exception as e:
            print(f"    haiku error: {type(e).__name__}: {e}", file=sys.stderr)
            time.sleep(2 ** attempt)
    return {"rule": "unknown", "evidence": ""}


CITY_PATTERNS = [
    re.compile(r'(?:in|to)\s+([A-Z][A-Za-z\s\.\']{2,30}?),\s*California\b'),
    re.compile(r'(?:dog\'s favorite spot in|spot in)\s+([A-Z][A-Za-z\s\.\']{2,30}?)\.?\s'),
    re.compile(r'\bin\s+([A-Z][A-Za-z\s\.\']{2,30}?)\s*\([A-Z]{2}\)'),
]


def parse_meta(text: str) -> dict:
    """Extract name (H1) and city if mentioned in body."""
    name_m = re.search(r'^#\s+(.+?)\s*$', text, re.M)
    name = name_m.group(1).strip() if name_m else ""
    city = ""
    for rx in CITY_PATTERNS:
        m = rx.search(text)
        if m:
            city = m.group(1).strip()
            break
    return {"name": name, "city": city}


def db_upsert(rows: list[dict]) -> None:
    if not rows: return
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    r = httpx.post(f"{SUPABASE_URL}/rest/v1/truth_external",
                   headers=headers, json=rows, timeout=60,
                   params={"on_conflict": "source,source_id"})
    r.raise_for_status()


def db_existing_ids() -> set[str]:
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Range": "0-9999"}
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/truth_external",
                  headers=headers,
                  params={"select":"source_id", "source":"eq.bringfido"},
                  timeout=30)
    r.raise_for_status()
    return {row["source_id"] for row in r.json()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="0 = all")
    ap.add_argument("--refresh", action="store_true", help="re-scrape existing IDs")
    ap.add_argument("--ids", type=str, help="comma-separated attraction IDs to scrape (skips discovery)")
    args = ap.parse_args()

    if args.ids:
        ids = [x.strip() for x in args.ids.split(",") if x.strip()]
        print(f"Using explicit IDs: {ids}")
    else:
        print("=== DISCOVERY ===")
        ids = discover_ids()
        print(f"discovered {len(ids)} attraction IDs")

    if not args.refresh:
        existing = db_existing_ids()
        before = len(ids)
        ids = [i for i in ids if i not in existing]
        print(f"skipping {before - len(ids)} already-scraped IDs")

    if args.limit:
        ids = ids[:args.limit]

    print(f"\n=== SCRAPING {len(ids)} ===")
    batch: list[dict] = []
    for n, aid in enumerate(ids, 1):
        url = f"https://www.bringfido.com/attraction/{aid}"
        text = tavily_extract(url)
        if not text or len(text) < 200:
            print(f"  [{n}/{len(ids)}] {aid}: empty/short — skip")
            continue
        meta = parse_meta(text)
        if not meta["name"]:
            print(f"  [{n}/{len(ids)}] {aid}: no name — skip")
            continue
        cls = call_haiku(text, meta["name"])
        rule = cls.get("rule", "unknown")
        evidence = cls.get("evidence", "")[:500]
        print(f"  [{n}/{len(ids)}] {aid}: {meta['name']!r} ({meta['city'] or '?'}) -> {rule}")
        batch.append({
            "source": "bringfido",
            "source_id": aid,
            "source_url": url,
            "name": meta["name"],
            "city": meta["city"] or None,
            "state": "CA",
            "dogs_rule": rule,
            "raw_dog_text": evidence,
            "description": text[:4000],
            "scraped_at": "now()",  # PostgREST accepts string for default; better to omit
        })
        if len(batch) >= 10:
            for r in batch: r.pop("scraped_at", None)
            db_upsert(batch); batch.clear()
        time.sleep(SLEEP_S)

    if batch:
        for r in batch: r.pop("scraped_at", None)
        db_upsert(batch)

    print(f"\nDone. Upserted batch totals.")


if __name__ == "__main__":
    main()
