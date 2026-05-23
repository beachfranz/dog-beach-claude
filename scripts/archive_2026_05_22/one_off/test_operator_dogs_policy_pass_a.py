"""
test_operator_dogs_policy_pass_a.py
------------------------------------
Calibration test for Pass A (Headline) of the operator dog policy
extractor. Hand-picked URLs for two test operators:
  - city-of-long-beach (mixed policy with Rosie's exception)
  - los-angeles-county-department-of-beaches-and-harbors (blanket no)

Fetches the operator's policy page, cleans to text, runs Pass A with
Haiku, prints the structured output. Doesn't write to the database
yet — purely calibration eyeball.
"""

from __future__ import annotations
import json, os, re, sys
from pathlib import Path
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

# Hand-picked test operators + candidate URLs
TESTS = [
    {
        "operator_id": 26,
        "canonical_name": "City of Coronado",
        "operator_level": "city",
        "n_beaches": 16,
        "url": "https://www.coronado.ca.us/757/Dogs",
    },
    {
        "operator_id": 1322,
        "canonical_name": "Los Angeles County Department of Beaches and Harbors",
        "operator_level": "county",
        "n_beaches": 73,
        "url": "https://beaches.lacounty.gov/rules/",
    },
    {
        "operator_id": 0,
        "canonical_name": "City of Carmel-by-the-Sea",
        "operator_level": "city",
        "n_beaches": 6,
        "url": "https://ci.carmel.ca.us/post/beach-rules-updated-march-2019",
    },
    {
        "operator_id": 0,
        "canonical_name": "California Department of Parks and Recreation",
        "operator_level": "state",
        "n_beaches": 511,
        "url": "https://www.parks.ca.gov/Dogs",
    },
]


def fetch_and_clean(url: str) -> tuple[str, str]:
    """Return (status, cleaned_text). Status='ok' or error message."""
    try:
        r = httpx.get(url, follow_redirects=True, timeout=45,
                      headers={"User-Agent": "dog-beach-scout/1.0 (admin policy extraction)"})
        if r.status_code != 200:
            return (f"HTTP {r.status_code}", "")
        soup = BeautifulSoup(r.text, "html.parser")
        # Strip script, style, nav, footer
        for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        # Cap at 12k chars to keep prompt reasonable
        return ("ok", text[:12000])
    except Exception as e:
        return (f"fetch failed: {e}", "")


def build_pass_a_prompt(t: dict, page_text: str) -> tuple[str, str]:
    """Returns (system, user)."""
    system = f"""You are extracting beach dog policy from a single source page for the California operator "{t['canonical_name']}" ({t['operator_level']}, manages {t['n_beaches']} beaches).

ABSOLUTE RULES:
- Only assert facts directly supported by the <page> content provided.
- Quote 1-2 short spans (≤120 chars each) for every populated field.
- If the page does not address dogs at all, return all fields null and confidence=0.
- Do NOT use any prior knowledge about California beaches or this operator. If you don't see it on the page, you don't know it.
- Empty array / null is correct when the page is silent on that field.
- Return ONLY a single JSON object — no prose, no markdown fences."""

    user = f"""Source URL: {t['url']}

<page>
{page_text}
</page>

Extract ONLY four fields from the page:

1. policy_found (bool): does the page meaningfully address whether dogs are allowed on this operator's beaches?
2. default_rule (one of "yes" | "no" | "restricted" | null):
   - "yes": dogs allowed without significant time/zone restrictions
   - "no": dogs prohibited
   - "restricted": dogs allowed only in specific zones, times, or under specific conditions (leash always, summer-only, etc.)
3. applies_to_all (bool|null): does the rule apply uniformly to every beach this operator manages, or are there per-beach exceptions?
4. leash_required (bool|null): does the page say leashes are required?

Return ONLY:
{{
  "policy_found": <bool>,
  "default_rule": "yes" | "no" | "restricted" | null,
  "applies_to_all": <bool|null>,
  "leash_required": <bool|null>,
  "source_quotes": [<text>, ...],
  "confidence": <0.0-1.0>
}}"""

    return (system, user)


def call_llm(model: str, system: str, user: str, max_tokens: int = 1500) -> dict:
    r = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [{"role": "user", "content": user}],
        },
        timeout=90,
    )
    r.raise_for_status()
    body = r.json()
    text = body["content"][0]["text"].strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    try:
        return {"json": json.loads(text), "usage": body.get("usage", {}), "raw": text}
    except json.JSONDecodeError as e:
        return {"error": str(e), "raw": text, "usage": body.get("usage", {})}


def build_pass_b_prompt(t: dict, page_text: str) -> tuple[str, str]:
    system = f"""You are extracting beach dog policy from a single source page for the California operator "{t['canonical_name']}" ({t['operator_level']}, manages {t['n_beaches']} beaches).

ABSOLUTE RULES:
- Only assert facts directly supported by the <page> content provided.
- Quote 1-2 short spans (≤120 chars each) for every populated field.
- If the page is silent on a field, return null or empty array. Empty is correct when the page is silent.
- Do NOT use any prior knowledge about California beaches or this operator. If you don't see it on the page, you don't know it.
- Return ONLY a single JSON object — no prose, no markdown fences."""

    user = f"""Source URL: {t['url']}

<page>
{page_text}
</page>

Extract the structured restriction details:

1. time_windows: array of allowed time windows. Each item:
   {{
     "before": "HH:MM" | null,    // dogs allowed BEFORE this time
     "after":  "HH:MM" | null,    // dogs allowed AFTER this time
     "season": "summer" | "winter" | "year_round" | null,
     "leashed": <bool|null>
   }}
   Empty array if the page is silent on time windows.

2. seasonal_closures: array of date-range closures. Each item:
   {{
     "reason": "snowy_plover" | "harbor_seal_pupping" | "other",
     "from": "MM-DD",
     "to": "MM-DD",
     "policy": "prohibited" | "restricted_zones"
   }}
   Empty array if no seasonal closure language.

3. spatial_zones: where on the property dogs ARE / AREN'T allowed:
   {{
     "allowed_in": [<short text>, ...],
     "prohibited_in": [<short text>, ...]
   }}
   Empty arrays if the page doesn't differentiate zones.

Return ONLY:
{{
  "time_windows": [...],
  "seasonal_closures": [...],
  "spatial_zones": {{"allowed_in": [...], "prohibited_in": [...]}},
  "source_quotes": [<text>, ...],
  "confidence": <0.0-1.0>
}}"""
    return (system, user)


def build_pass_c_prompt(t: dict, page_text: str) -> tuple[str, str]:
    system = f"""You are extracting beach dog policy from a single source page for the California operator "{t['canonical_name']}" ({t['operator_level']}, manages {t['n_beaches']} beaches).

ABSOLUTE RULES:
- Only assert facts directly supported by the <page> content provided.
- Quote 1-2 short spans (≤120 chars each) for every populated field.
- Do NOT use any prior knowledge about California beaches or this operator. If you don't see it on the page, you don't know it.
- Return ONLY a single JSON object — no prose, no markdown fences."""

    user = f"""Source URL: {t['url']}

<page>
{page_text}
</page>

Extract per-beach exceptions and document references:

1. exceptions: per-beach overrides — specific named beaches with rules that differ from the operator's default. Each item:
   {{
     "beach_name": <text>,
     "rule": "off_leash" | "prohibited" | "allowed",
     "source_quote": <text>
   }}
   Empty array if the page doesn't name specific beaches with different rules.

2. ordinance_reference: the formal municipal/county code reference if the page cites one (e.g. "LA County Code §17.12.080" or "Long Beach Municipal Code 6.16.080"). Null otherwise. Do NOT invent a citation.

3. summary: ONE short sentence (≤140 chars) capturing the headline policy + the most important exception or restriction, written for a dog owner deciding whether to visit.

Return ONLY:
{{
  "exceptions": [...],
  "ordinance_reference": <text|null>,
  "summary": <text>,
  "source_quotes": [<text>, ...],
  "confidence": <0.0-1.0>
}}"""
    return (system, user)


def run_pass(label: str, model: str, system: str, user: str, max_tokens: int = 1500):
    print(f"\n  -- Pass {label} ({model}) --")
    result = call_llm(model, system, user, max_tokens)
    if "error" in result:
        print(f"  JSON parse error: {result['error']}")
        print(f"  raw: {result['raw'][:500]}")
        return None
    usage = result.get("usage", {})
    print(f"  tokens: in={usage.get('input_tokens')} out={usage.get('output_tokens')}")
    print(json.dumps(result["json"], indent=2))
    return result["json"]


def main():
    HAIKU  = "claude-haiku-4-5-20251001"
    SONNET = "claude-sonnet-4-6"
    for t in TESTS:
        print(f"\n{'='*72}")
        print(f"OPERATOR #{t['operator_id']}  {t['canonical_name']}")
        print(f"URL: {t['url']}")
        print(f"{'='*72}")

        status, page_text = fetch_and_clean(t["url"])
        if status != "ok":
            print(f"  fetch failed: {status}")
            continue
        print(f"  fetched {len(page_text):,} chars of cleaned text")

        sa, ua = build_pass_a_prompt(t, page_text)
        run_pass("A — Headline",     HAIKU,  sa, ua, 1024)

        sb, ub = build_pass_b_prompt(t, page_text)
        run_pass("B — Restrictions", SONNET, sb, ub, 1500)

        sc, uc = build_pass_c_prompt(t, page_text)
        run_pass("C — Exceptions",   SONNET, sc, uc, 1500)


if __name__ == "__main__":
    main()
