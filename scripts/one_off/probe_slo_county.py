"""Inspect SLO County's parks site + Tavily search for beach-specific
dog policy pages. Diagnose why the picker missed and what URL would
actually answer the question."""
import os, sys, json
sys.stdout.reconfigure(encoding="utf-8")
import httpx
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
TAVILY = os.environ["TAVILY_API_KEY"]

def tavily_search(query, include_domains=None, n=5):
    body = {"api_key": TAVILY, "query": query, "search_depth": "basic", "max_results": n}
    if include_domains: body["include_domains"] = include_domains
    r = httpx.post("https://api.tavily.com/search", json=body, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

print("=" * 72)
print("Source B re-pick — broad search with sharper query")
print("=" * 72)
for q in [
    'San Luis Obispo County beach dogs leash ordinance',
    'slocounty.ca.gov beach dogs policy',
    '"San Luis Obispo County" county code dogs beach',
]:
    print(f"\nQUERY: {q}")
    hits = tavily_search(q, n=5)
    for h in hits:
        print(f"  {h.get('url')}")
        print(f"    {h.get('title')}")
        print(f"    {(h.get('content') or '')[:160]}")

print("\n" + "=" * 72)
print("Site-restricted search on slocountyparks.com — sharper queries")
print("=" * 72)
for q in [
    'beach rules dogs',
    'pet policy beach',
    'rules and regulations',
    'beaches',
]:
    print(f"\nQUERY (slocountyparks.com): {q}")
    hits = tavily_search(q, include_domains=["slocountyparks.com"], n=5)
    for h in hits:
        print(f"  {h.get('url')}")
        print(f"    {h.get('title')}")
        print(f"    {(h.get('content') or '')[:160]}")

print("\n" + "=" * 72)
print("Site-restricted on slocounty.ca.gov (parent county site)")
print("=" * 72)
for q in [
    'beach dogs leash policy',
    'animal regulations beach',
]:
    print(f"\nQUERY (slocounty.ca.gov): {q}")
    hits = tavily_search(q, include_domains=["slocounty.ca.gov"], n=5)
    for h in hits:
        print(f"  {h.get('url')}")
        print(f"    {h.get('title')}")
        print(f"    {(h.get('content') or '')[:160]}")
