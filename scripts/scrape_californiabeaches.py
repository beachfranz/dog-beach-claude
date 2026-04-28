"""
scrape_californiabeaches.py
---------------------------
Scrape CaliforniaBeaches.com's curated dog-friendly hub into
public.truth_external for COMPARISON ONLY (never merged).

Editorial framing: the hub is off-leash-focused — "Almost all of these
beaches allow dogs to be off leash all day every day." Some have time
or season restrictions. We LLM-classify each detail page so nuance
isn't lost.

Pipeline:
  1. Fetch /dog-friendly-beaches-in-california/ + SoCal regional hub
  2. Extract /beach/<slug>/ links
  3. For each: Tavily-extract detail page, Haiku classifies dog rule
  4. Upsert into truth_external (source='californiabeaches', source_id=<slug>)

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

HUB_URLS = [
    "https://www.californiabeaches.com/dog-friendly-beaches-in-california/",
    "https://www.californiabeaches.com/dog-friendly-beaches-southern-california/",
]
SLEEP_S = 10.0  # robots.txt requests 10s crawl-delay


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


def discover_slugs() -> list[str]:
    seen: set[str] = set()
    for url in HUB_URLS:
        text = tavily_extract(url)
        slugs = re.findall(r'/beach/([a-z0-9\-]+)/', text)
        new = [s for s in slugs if s not in seen]
        for s in new:
            seen.add(s)
        print(f"  {url.split('/')[-2]}: +{len(new)} (total {len(seen)})")
        time.sleep(SLEEP_S)
    return sorted(seen)


CLASSIFY_SYSTEM = """You read a CaliforniaBeaches.com beach detail page and classify the dog-access rule the page describes.

CONTEXT: The page is from CaliforniaBeaches.com's curated dog-friendly hub — these beaches allow dogs. The editorial bar is high: most are off-leash-friendly. Some have time/season restrictions. A few are leash-only.

Return ONLY JSON: {"rule": "off_leash"|"leash"|"yes"|"no"|"unknown", "evidence": "<one short verbatim quote from the page>"}

Rules:
- "off_leash" → page mentions off-leash, no-leash, dogs can run free, dog-park-style
- "leash" → page explicitly states leash required / dogs must be leashed
- "yes" → dogs allowed but leash specifics aren't mentioned
- "no" → page explicitly states dogs prohibited (rare in this set)
- "unknown" → page is too thin / content extraction failed

If page describes off-leash with time/season restrictions (e.g., "off-leash before 10am only"), pick "off_leash" — the distinguishing feature is off-leash-ness.
The "evidence" must be a verbatim quote (≤180 chars) or "(listed in dog-friendly hub)" if no policy quote present."""


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


def parse_meta(text: str, slug: str) -> dict:
    name_m = re.search(r'^#\s+(.+?)\s*$', text, re.M)
    name = name_m.group(1).strip() if name_m else slug.replace('-', ' ').title()
    # CaliforniaBeaches usually has "## About <Name> in <City>, California"
    city = ""
    m = re.search(r'About\s+.+?\s+in\s+([A-Z][\w\s\.\']+?),\s*California', text)
    if m:
        city = m.group(1).strip()
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
                  params={"select":"source_id", "source":"eq.californiabeaches"},
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
        print(f"Using explicit slugs: {slugs}")
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
        url = f"https://www.californiabeaches.com/beach/{slug}/"
        text = tavily_extract(url)
        if not text or len(text) < 200:
            print(f"  [{n}/{len(slugs)}] {slug}: empty — skip")
            continue
        meta = parse_meta(text, slug)
        cls = call_haiku(text, meta["name"])
        rule = cls.get("rule", "unknown")
        print(f"  [{n}/{len(slugs)}] {slug}: {meta['name']!r} ({meta['city'] or '?'}) -> {rule}")
        batch.append({
            "source": "californiabeaches",
            "source_id": slug,
            "source_url": url,
            "name": meta["name"],
            "city": meta["city"] or None,
            "state": "CA",
            "dogs_rule": rule,
            "raw_dog_text": cls.get("evidence", "")[:500],
            "description": text[:4000],
        })
        if len(batch) >= 10:
            db_upsert(batch); batch.clear()
        time.sleep(SLEEP_S)

    if batch:
        db_upsert(batch)

    print(f"\nDone.")


if __name__ == "__main__":
    main()
