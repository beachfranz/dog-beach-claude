"""
scrape_websearch.py
-------------------
4th source: LLM-with-web-search verdict for individual beaches.
Independent of BringFido / CaliforniaBeaches / DogTrekker — uses a
generic Tavily web search and Haiku classification of result snippets.

Used for the long tail: beaches no curated directory covers, or where
existing sources disagree. Reads candidate beach names/cities from
truth_comparison_v (us_unknown / mixed / no_external_coverage outcomes)
and stores results as source='websearch' in truth_external.

Idempotent — skips already-scraped origin_keys unless --refresh.
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


def tavily_search(query: str, max_results: int = 5) -> list[dict]:
    try:
        r = httpx.post("https://api.tavily.com/search",
            json={"api_key": TAVILY_API_KEY, "query": query,
                  "search_depth": "basic", "max_results": max_results},
            timeout=30)
        r.raise_for_status()
        return r.json().get("results", [])
    except Exception as e:
        print(f"    tavily search error: {type(e).__name__}", file=sys.stderr)
        return []


CLASSIFY_SYSTEM = """You read web-search results about a specific California beach and decide the dog-access rule.

The user gives you a beach name and 3–5 search-result snippets from various sources (city pages, blog posts, forums, directories). Synthesize across them.

Return ONLY JSON: {"rule": "off_leash"|"leash"|"yes"|"no"|"unknown", "evidence": "<verbatim quote from one snippet>", "agreement": "consistent"|"mixed"|"thin"}

Rules:
- "off_leash" → snippets describe off-leash, no-leash, dogs run free
- "leash" → snippets explicitly mention leash required / dogs must be leashed
- "yes" → dogs allowed but leash specifics not mentioned
- "no" → snippets explicitly state dogs prohibited
- "unknown" → snippets don't actually answer the question or are off-topic

agreement field: "consistent" = all snippets agree, "mixed" = snippets disagree, "thin" = only 1 useful snippet or low confidence

Pick the strongest signal. If 2 say leash and 1 says off-leash, prefer leash (more conservative + more sources).
The "evidence" must be a verbatim quote (≤180 chars) from a snippet."""


def call_haiku(snippets: list[dict], beach_name: str, city: str | None) -> dict:
    bullets = "\n\n".join(
        f"[{s.get('title','')}] {s.get('url','')}\n{s.get('content','')[:500]}"
        for s in snippets[:5]
    )
    user = f"Beach: {beach_name}" + (f" in {city}, CA" if city else " in California") + f"\n\nSearch results:\n{bullets}"
    for attempt in range(3):
        try:
            r = httpx.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY,
                         "anthropic-version":"2023-06-01",
                         "content-type":"application/json"},
                json={"model": HAIKU, "max_tokens": 250, "system": CLASSIFY_SYSTEM,
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
    return {"rule": "unknown", "evidence": "", "agreement": "thin"}


def db_select_candidates() -> list[dict]:
    """Pull beaches from comparison view where we want a 4th opinion."""
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Range": "0-9999"}
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/truth_comparison_v",
                  headers=headers,
                  params={"select":"origin_key,name,outcome",
                          "outcome":"in.(us_unknown,mixed,no_external_coverage)"},
                  timeout=30)
    r.raise_for_status()
    return r.json()


def db_existing_ids() -> set[str]:
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Range": "0-9999"}
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/truth_external",
                  headers=headers,
                  params={"select":"source_id", "source":"eq.websearch"},
                  timeout=30)
    r.raise_for_status()
    return {row["source_id"] for row in r.json()}


def db_upsert(rows: list[dict]) -> None:
    if not rows: return
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    r = httpx.post(f"{SUPABASE_URL}/rest/v1/truth_external",
                   headers=headers, json=rows, timeout=60,
                   params={"on_conflict": "source,source_id"})
    r.raise_for_status()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()

    candidates = db_select_candidates()
    print(f"=== {len(candidates)} candidate rows from comparison view ===")

    if not args.refresh:
        existing = db_existing_ids()
        before = len(candidates)
        candidates = [c for c in candidates if c["origin_key"] not in existing]
        print(f"skipping {before - len(candidates)} already-scraped")

    if args.limit:
        candidates = candidates[:args.limit]

    batch: list[dict] = []
    for n, c in enumerate(candidates, 1):
        origin_key = c["origin_key"]
        name = c["name"]
        if not name:
            continue
        query = f"are dogs allowed at {name} California beach"
        results = tavily_search(query, max_results=5)
        if not results:
            print(f"  [{n}/{len(candidates)}] {origin_key}: no search results")
            continue
        cls = call_haiku(results, name, None)
        rule = cls.get("rule", "unknown")
        agreement = cls.get("agreement", "thin")
        print(f"  [{n}/{len(candidates)}] {name!r} ({origin_key}) -> {rule} [{agreement}]")
        batch.append({
            "source": "websearch",
            "source_id": origin_key,
            "source_url": "tavily://" + query.replace(" ","+"),
            "name": name,
            "state": "CA",
            "dogs_rule": rule,
            "raw_dog_text": cls.get("evidence","")[:500] + (f" | agreement={agreement}" if agreement else ""),
            "matched_origin_key": origin_key,
            "match_method": "websearch_direct",
            "match_score": 1.0,
        })
        if len(batch) >= 10:
            db_upsert(batch); batch.clear()
        time.sleep(1.0)

    if batch:
        db_upsert(batch)

    print("\nDone.")


if __name__ == "__main__":
    main()
