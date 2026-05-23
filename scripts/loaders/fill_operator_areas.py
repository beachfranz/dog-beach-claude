"""
fill_operator_areas.py
----------------------
For each operator_dogs_policy row, decompose spatial_zones (allowed_in[]
and prohibited_in[]) + summary into the 6-area enum (sand, water,
picnic_area, parking_lot, trails, campground) plus designated_dog_zones
and prohibited_areas free text. Mirror of fill_cpad_areas_from_quotes
but at the agency grain.

Skips operators that already have area_sand populated. Idempotent.
"""
from __future__ import annotations
import argparse, json, os, re, sys, time
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SERVICE_KEY       = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
HAIKU             = "claude-haiku-4-5-20251001"


SYSTEM = """You decompose an agency's blanket dog-policy zone description into per-area rules.

You are given an agency name and three pieces of evidence:
  - default_rule (yes / no / restricted)
  - spatial_zones.allowed_in: free-text list of where dogs ARE allowed
  - spatial_zones.prohibited_in: free-text list of where dogs are NOT allowed
  - summary: a one-sentence headline written for a dog owner

For each of these 6 fixed AREAS, decide the agency's blanket rule:
  sand, water, picnic_area, parking_lot, trails, campground

Each area's rule is one of:
- "off_leash"  — explicitly off-leash allowed in this area at agency level
- "on_leash"   — dogs allowed but must be leashed (default for cities/CDPR/most agencies in CA)
- "forbidden"  — dogs explicitly not allowed in this area
- "unknown"    — agency policy doesn't address this area

Rules of thumb:
- If default_rule="no" and no overriding mentions, set ALL 6 to forbidden.
- If default_rule="yes" with no zone detail, set all 6 to on_leash UNLESS specific zones are forbidden in prohibited_in.
- "Day-use areas" → picnic_area = on_leash. Day-use does NOT cover sand/water/trails.
- "Multiuse trail / bike path" → trails = on_leash.
- "Beach" / "sand" / "shoreline" mentioned → sand. "ocean" / "swimming areas" → water.
- "Parking lot" → parking_lot.
- "Campground" / "RV park" → campground.
- "Trails" / "hiking" / "boardwalk" → trails.
- "Service animals only" → forbidden for all areas (with note).
- NEVER infer off_leash unless explicitly stated.

ADDITIONAL FREE-TEXT FIELDS:
- designated_dog_zones: verbatim short text describing any specifically-named dog-permitted zones (from allowed_in entries that name specific places). Empty string if none.
- prohibited_areas: verbatim text from prohibited_in. Empty string if none.

Return ONLY a JSON object with all 8 fields:
{
  "sand": "...", "water": "...", "picnic_area": "...",
  "parking_lot": "...", "trails": "...", "campground": "...",
  "designated_dog_zones": "...",
  "prohibited_areas": "..."
}
NO other prose. NO markdown fences."""


def call_haiku(operator_name: str, default_rule: str | None,
               allowed_in: list, prohibited_in: list,
               summary: str | None) -> dict | None:
    user = (
        f"Operator: {operator_name}\n"
        f"default_rule: {default_rule or 'unknown'}\n"
        f"allowed_in: {json.dumps(allowed_in or [])}\n"
        f"prohibited_in: {json.dumps(prohibited_in or [])}\n"
        f"summary: {summary or '(none)'}"
    )
    for attempt in range(3):
        try:
            r = httpx.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY,
                         "anthropic-version":"2023-06-01",
                         "content-type":"application/json"},
                json={"model": HAIKU, "max_tokens": 400, "system": SYSTEM,
                      "messages":[{"role":"user","content":user}]},
                timeout=60)
            if r.status_code >= 500 or r.status_code == 429:
                time.sleep(2 ** attempt * 2); continue
            r.raise_for_status()
            text = r.json()["content"][0]["text"].strip()
            text = re.sub(r"^```(?:json)?\s*|\s*```\s*$", "", text)
            return json.loads(text)
        except Exception as e:
            print(f"    haiku error: {type(e).__name__}: {e}", file=sys.stderr)
            time.sleep(2 ** attempt)
    return None


def fetch_target_rows() -> list[dict]:
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Range": "0-9999"}
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/operator_dogs_policy",
                  headers=headers,
                  params={"select":"operator_id,default_rule,spatial_zones,summary,operators(canonical_name)",
                          "area_sand":"is.null",
                          "default_rule":"not.is.null"},
                  timeout=30)
    r.raise_for_status()
    return r.json()


def db_update(operator_id: int, fields: dict) -> bool:
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Content-Type": "application/json"}
    r = httpx.patch(f"{SUPABASE_URL}/rest/v1/operator_dogs_policy",
                    headers=headers,
                    params={"operator_id": f"eq.{operator_id}"},
                    json=fields, timeout=30)
    if r.status_code >= 400:
        print(f"    update {operator_id} {r.status_code}: {r.text[:200]}", file=sys.stderr)
        return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    rows = fetch_target_rows()
    if args.limit:
        rows = rows[:args.limit]
    print(f"Loaded {len(rows)} rows needing area fills")

    valid_rules = {"off_leash","on_leash","forbidden","unknown"}
    n_ok = 0
    for n, row in enumerate(rows, 1):
        op_id   = row["operator_id"]
        op_name = (row.get("operators") or {}).get("canonical_name") or f"#{op_id}"
        default = row.get("default_rule")
        sz = row.get("spatial_zones") or {}
        if isinstance(sz, str):
            try: sz = json.loads(sz)
            except: sz = {}
        allowed_in    = sz.get("allowed_in")    or []
        prohibited_in = sz.get("prohibited_in") or []
        summary       = row.get("summary")

        result = call_haiku(op_name, default, allowed_in, prohibited_in, summary)
        if not result:
            print(f"  [{n}/{len(rows)}] op {op_id} {op_name!r}: classify failed")
            continue

        fields = {}
        for area in ("sand","water","picnic_area","parking_lot","trails","campground"):
            v = (result.get(area) or "").strip().lower().replace("-","_")
            fields[f"area_{area}"] = v if v in valid_rules else "unknown"
        fields["designated_dog_zones"] = (result.get("designated_dog_zones") or "").strip() or None
        fields["prohibited_areas"]     = (result.get("prohibited_areas") or "").strip() or None

        if db_update(op_id, fields):
            n_ok += 1
            ar = " ".join(f"{a[0]}={fields[f'area_{a}'][0:1]}" for a in ("sand","water","picnic_area","parking_lot","trails","campground"))
            print(f"  [{n}/{len(rows)}] op {op_id} {op_name!r}: default={default} -> {ar}")
        time.sleep(0.4)

    print(f"\nDone. {n_ok}/{len(rows)} updated.")


if __name__ == "__main__":
    main()
