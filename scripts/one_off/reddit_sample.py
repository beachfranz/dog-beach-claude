"""
reddit_sample.py
----------------
Probe Reddit for posts about specific beaches without any API key.
Reddit's public JSON endpoint (reddit.com/...json) is rate-limited but
free and unauthenticated for read-only access.

Just prints sampled results — no DB writes, no LLM calls. Sanity test
to see what kind of signal Reddit actually carries per beach.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any

import httpx

# A polite custom User-Agent is required — Reddit blocks default UAs.
HEADERS = {"User-Agent": "DogBeachScout/0.1 by u/franzfunk"}

BEACHES = [
    "Huntington Dog Beach",
    "Coronado Dog Beach",
    "Rosie's Dog Beach Long Beach",
    "Carmel Beach dogs",
    "Mandalay State Beach dogs",
]

def search_reddit(query: str, limit: int = 5) -> list[dict[str, Any]]:
    url = "https://www.reddit.com/search.json"
    params = {
        "q":     query,
        "limit": limit,
        "sort":  "relevance",
        "t":     "all",
        "type":  "link",
    }
    r = httpx.get(url, params=params, headers=HEADERS, timeout=20.0,
                  follow_redirects=True)
    if not r.is_success:
        return []
    data = r.json()
    return [post["data"] for post in data.get("data", {}).get("children", [])]

def fmt_post(p: dict) -> str:
    title    = p.get("title", "")
    sub      = p.get("subreddit_name_prefixed", "")
    score    = p.get("score", 0)
    nc       = p.get("num_comments", 0)
    age_days = int((time.time() - p.get("created_utc", 0)) / 86400) if p.get("created_utc") else "?"
    body     = (p.get("selftext", "") or "").strip().replace("\n", " ")
    body_snip = (body[:240] + "…") if len(body) > 240 else body
    permalink = "https://reddit.com" + p.get("permalink", "")
    return (f"  {sub}  ↑{score}  💬{nc}  ({age_days}d old)\n"
            f"    {title}\n"
            + (f"    {body_snip}\n" if body_snip else "")
            + f"    {permalink}")

def main() -> int:
    for beach in BEACHES:
        print(f"\n========== {beach} ==========")
        posts = search_reddit(beach, limit=5)
        if not posts:
            print("  (no results)")
            continue
        for p in posts:
            print(fmt_post(p))
            print()
        time.sleep(2)  # respect rate limits (~30 req/min unauthenticated)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
