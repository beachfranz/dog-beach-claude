"""
test_dog_zones_prompt.py — one-shot test of a structured per-zone dog
policy extraction prompt before adding it as a real variant.

Hits 8 beaches that exercise different zone patterns:
  - Coronado Dog Beach     (sand off-leash, parking on-leash)
  - Rosie's Dog Beach      (zoned off-leash + on-leash paths)
  - Bolsa Chica State      (sand prohibited, trail+parking leashed)
  - Malibu Lagoon State    (sand prohibited, lawns/parking leashed, lagoon prohibited)
  - Pismo State Beach      (beach+trails leashed, dunes prohibited)
  - Refugio State          (parking+picnic only, beach prohibited)
  - Will Rogers State      (CDPR no-dogs-on-sand)
  - Limantour Beach        (PRNS off-leash exception, NPS thin page)

Uses the existing fetch+strip pipeline from extract_for_orphans.py.
"""
from __future__ import annotations
import json
import os
import sys
import time
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

sys.path.insert(0, str(SCRIPT_DIR))
from extract_for_orphans import fetch_html, bs4_strip_loose, GAP_B_BEACHES, TIER2_BEACHES, ORPHANS  # noqa

PROMPT = """You are extracting structured dog-zone policy data from a beach webpage.

For each of the EIGHT zone types below, decide the most permissive dog status the source explicitly supports, with evidence:

ZONES (closed list):
  - sand          (the actual beach itself: sand or shingle area)
  - trails        (multi-use trails, hiking trails, bike paths through the park)
  - picnic_area   (designated picnic / day-use lawn / table areas)
  - parking_lot   (parking lots, the road in/out)
  - campground    (overnight camping zones)
  - dunes         (dune systems, dune preserves, vegetated sand)
  - lagoon        (wetland/lagoon/estuary edges and trails through them)
  - boardwalk     (boardwalk, promenade, paved path along the beach)

STATUS for each zone (closed list):
  - off_leash         — explicit text says dogs can be off-leash here
  - on_leash          — explicit text says dogs allowed here on leash
  - prohibited        — explicit text says no dogs / dogs not allowed
  - unclear           — zone is mentioned but the dog rule is ambiguous
  - not_applicable    — zone is not mentioned, or this beach has no such zone

Be conservative: only mark off_leash if the source explicitly says off-leash, leash-free, or unleashed for that zone. Do NOT infer off-leash from "dogs allowed" without leash specifics.

Return ONLY a JSON object of this exact shape (no prose, no markdown fencing):

{
  "sand":        {"status": "...", "evidence": "verbatim quote from source or null"},
  "trails":      {"status": "...", "evidence": "..."},
  "picnic_area": {"status": "...", "evidence": "..."},
  "parking_lot": {"status": "...", "evidence": "..."},
  "campground":  {"status": "...", "evidence": "..."},
  "dunes":       {"status": "...", "evidence": "..."},
  "lagoon":      {"status": "...", "evidence": "..."},
  "boardwalk":   {"status": "...", "evidence": "..."}
}

Source webpage content:
---
{page}
---
"""

# Test set: 8 beaches that span the diversity
TEST_FIDS = [6202, 6411, 8606, 8475, 8394, 5939, 8472, 8865]

ALL_BEACHES = list(ORPHANS) + list(GAP_B_BEACHES) + list(TIER2_BEACHES)
BY_FID = {b["arena_fid"]: b for b in ALL_BEACHES}


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        sys.exit("ANTHROPIC_API_KEY missing")
    client = Anthropic(api_key=api_key)

    total_in = total_out = 0
    for fid in TEST_FIDS:
        b = BY_FID[fid]
        print(f"\n══ {b['name']} (fid {fid}) ══")
        print(f"   url: {b['url']}")
        html = fetch_html(b["url"])
        if not html:
            print(f"   ⚠ fetch failed")
            continue
        page = bs4_strip_loose(html)
        prompt = PROMPT.replace("{page}", page)
        t0 = time.time()
        resp = client.messages.create(
            model="claude-sonnet-4-5-20250929",  # sonnet-4-6 cap — match active variants
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        latency = (time.time() - t0) * 1000
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.strip("`").lstrip("json").strip()
        try:
            obj = json.loads(raw)
            for zone in ["sand","trails","picnic_area","parking_lot","campground","dunes","lagoon","boardwalk"]:
                z = obj.get(zone, {})
                ev = (z.get("evidence") or "")[:80]
                print(f"     {zone:12} {z.get('status','?'):14} {ev}")
        except Exception as e:
            print(f"   ⚠ parse failed: {e}")
            print(f"   raw: {raw[:300]}")
        in_t = resp.usage.input_tokens
        out_t = resp.usage.output_tokens
        total_in += in_t
        total_out += out_t
        print(f"   tokens: in={in_t}  out={out_t}  latency={latency:.0f}ms")

    # Sonnet 4.6 pricing: $3/MT input, $15/MT output (no caching this test)
    cost = total_in/1_000_000*3 + total_out/1_000_000*15
    print(f"\nTotals: input={total_in:,}  output={total_out:,}  cost≈${cost:.2f}")


if __name__ == "__main__":
    main()
