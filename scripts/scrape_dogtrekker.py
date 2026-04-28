"""
scrape_dogtrekker.py
--------------------
Scrape DogTrekker.com's California beach directory into
public.truth_external for COMPARISON ONLY (never merged).

DogTrekker is a CA-only hand-curated dog-travel directory. Detail pages
have rich free-text descriptions with per-beach dog-policy nuance, plus
lat/lng encoded in a Google Maps directions URL.

Pipeline:
  1. Walk /directory-category/beaches/page/N/ until empty (~3 pages, 174 slugs)
  2. For each: Tavily-extract detail page
  3. Parse name (H1), address, lat/lng (from Google Maps destination param)
  4. Haiku classifies dog rule from full description
  5. Upsert into truth_external (source='dogtrekker', source_id=<slug>)

Idempotent — skips already-scraped slugs unless --refresh.
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

INDEX_URL = "https://dogtrekker.com/directory-category/beaches/"
SLEEP_S   = 1.5


def tavily_extract(url: str) -> str:
    try:
        r = httpx.post("https://api.tavily.com/extract",
            json={"api_key": TAVILY_API_KEY, "urls":[url]}, timeout=90)
        r.raise_for_status()
        res = r.json().get("results", [])
        return res[0].get("raw_content", "") if res else ""
    except Exception as e:
        print(f"    tavily error on {url}: {type(e).__name__}", file=sys.stderr)
        return ""


def discover_slugs(max_pages: int = 12) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for p in range(1, max_pages + 1):
        url = INDEX_URL + (f"page/{p}/" if p > 1 else "")
        text = tavily_extract(url)
        slugs = re.findall(r'/directories/([a-z0-9\-]+)/', text)
        new = [s for s in slugs if s not in seen]
        if not new:
            print(f"  page {p}: no new — stopping")
            break
        for s in new: seen.add(s); ordered.append(s)
        print(f"  page {p}: +{len(new)} (total {len(ordered)})")
        time.sleep(SLEEP_S)
    return ordered


CLASSIFY_SYSTEM = """You read a DogTrekker.com beach detail page and classify the dog-access rule.

CONTEXT: DogTrekker is a hand-curated dog-travel directory for California. Pages have detailed free-text descriptions with per-beach nuance (off-leash zones, time/season restrictions, leash requirements).

Return ONLY JSON: {"rule": "off_leash"|"leash"|"yes"|"no"|"unknown", "evidence": "<verbatim quote from the page>"}

Rules:
- "off_leash" → page describes off-leash, no-leash, dogs running free, dog-park-style
- "leash" → page explicitly states leash required / dogs must be leashed
- "yes" → dogs allowed but leash specifics not mentioned
- "no" → page explicitly states dogs prohibited
- "unknown" → page content is too thin or ambiguous

If the page describes BOTH a leashed area and an off-leash zone, pick "off_leash" — that's the distinguishing feature.
If reviews/description mention dogs running off-leash repeatedly, lean "off_leash".
The "evidence" must be a verbatim quote (≤180 chars) from the page text."""


def call_haiku(page_text: str, name: str) -> dict:
    snippet = page_text[:8000]
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
            m = re.search(r'\{[^{}]*\}', text, re.DOTALL)
            if m:
                text = m.group(0)
            return json.loads(text)
        except Exception as e:
            print(f"    haiku error: {type(e).__name__}", file=sys.stderr)
            time.sleep(2 ** attempt)
    return {"rule": "unknown", "evidence": ""}


# Lat/lng from Google Maps "destination=lat,lng" URL param
LATLNG_RX = re.compile(r'destination=([+-]?\d+\.\d+),([+-]?\d+\.\d+)')
# Address: "[<street>, <city>, California <zip>]"
ADDR_RX = re.compile(r'\[([^\[\]]+,\s*California[^\[\]]*?)\]\(https?://(?:www\.)?google\.com/maps')


def parse_meta(text: str, slug: str) -> dict:
    name_m = re.search(r'^#\s+(.+?)\s*$', text, re.M)
    name = name_m.group(1).strip() if name_m else slug.replace('-', ' ').title()

    lat = lng = None
    m = LATLNG_RX.search(text)
    if m:
        lat = float(m.group(1))
        lng = float(m.group(2))

    address = None
    city = None
    am = ADDR_RX.search(text)
    if am:
        address = am.group(1).strip()
        # extract city: ", <City>, California"
        cm = re.search(r',\s*([A-Z][\w\s\.\']+?),\s*California', address)
        if cm: city = cm.group(1).strip()

    return {"name": name, "lat": lat, "lng": lng, "address": address, "city": city}


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
                  params={"select":"source_id", "source":"eq.dogtrekker"},
                  timeout=30)
    r.raise_for_status()
    return {row["source_id"] for row in r.json()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--slugs", type=str, help="comma-separated slugs (skip discovery)")
    args = ap.parse_args()

    if args.slugs:
        slugs = [s.strip() for s in args.slugs.split(",") if s.strip()]
    else:
        print("=== DISCOVERY ===")
        slugs = discover_slugs()
        print(f"discovered {len(slugs)} slugs")

    if not args.refresh:
        existing = db_existing_ids()
        before = len(slugs)
        slugs = [s for s in slugs if s not in existing]
        print(f"skipping {before - len(slugs)} already-scraped")

    if args.limit:
        slugs = slugs[:args.limit]

    print(f"\n=== SCRAPING {len(slugs)} ===")
    batch: list[dict] = []
    for n, slug in enumerate(slugs, 1):
        url = f"https://dogtrekker.com/directories/{slug}/"
        text = tavily_extract(url)
        if not text or len(text) < 200:
            print(f"  [{n}/{len(slugs)}] {slug}: empty — skip")
            continue
        meta = parse_meta(text, slug)
        cls = call_haiku(text, meta["name"])
        rule = cls.get("rule", "unknown")
        latlng = f"{meta['lat']},{meta['lng']}" if meta["lat"] else "?"
        print(f"  [{n}/{len(slugs)}] {slug}: {meta['name']!r} ({meta['city'] or '?'} @ {latlng}) -> {rule}")
        row = {
            "source": "dogtrekker",
            "source_id": slug,
            "source_url": url,
            "name": meta["name"],
            "city": meta["city"] or None,
            "state": "CA",
            "dogs_rule": rule,
            "raw_dog_text": cls.get("evidence", "")[:500],
            "address": meta["address"] or None,
            "description": text[:4000],
        }
        if meta["lat"] is not None:
            row["lat"] = meta["lat"]
            row["lng"] = meta["lng"]
        batch.append(row)
        if len(batch) >= 10:
            db_upsert(batch); batch.clear()
        time.sleep(SLEEP_S)

    if batch:
        db_upsert(batch)

    print("\nDone.")


if __name__ == "__main__":
    main()
