"""
fill_cpad_areas_from_quotes.py
------------------------------
For cpad_unit_dogs_policy rows with a populated source_quote (e.g.,
the 16 CDPR rows from the master-table parse), use Haiku to fill the
6 area columns + free text fields. No re-fetching — works off the
existing source_quote.

Scope: --counties flag for LA/Orange/San Diego restriction.
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


SYSTEM = """You decompose a per-park dog-policy quote into per-area rules.

CONTEXT: The quote comes from CDPR's master pet-policy table (parks.ca.gov/dogs).
Each park has a "Yes" or "No" verdict and a restriction string.

For each of these 6 fixed AREAS, decide the rule:
  sand, water, picnic_area, parking_lot, trails, campground

Each area's rule is one of:
- "off_leash"  — explicitly off-leash allowed in this area
- "on_leash"   — dogs allowed but must be leashed (CDPR default — CA state law)
- "forbidden"  — dogs explicitly not allowed in this area
- "unknown"    — quote does not address this area at all

Default rules of thumb for CDPR:
- If the quote says "Yes: Dogs allowed only in X" → X = on_leash; all OTHER named areas = forbidden; areas not in the named list = unknown.
- If the quote says "No" with no detail → all 6 areas = forbidden.
- "Dogs allowed in day-use areas" → picnic_area = on_leash. Day-use does NOT cover sand/water/trails.
- "Dogs allowed in campground" → campground = on_leash; everything else unknown unless explicitly stated.
- "Dogs allowed on multiuse trail / bike path" → trails = on_leash. Sand explicitly forbidden if mentioned.
- "Dogs not allowed on sand" / "not on the beach" → sand = forbidden, water = forbidden.
- CDPR almost always requires leash. NEVER infer off_leash unless the quote literally says "off-leash".

ADDITIONAL TWO TEXT FIELDS:
- designated_dog_zones: verbatim free-text describing any specifically-named dog-permitted zones (e.g., "North Beach (north of Lifeguard Tower 3)", "historic zone and Inspiration loop"). Empty string if none.
- prohibited_areas: verbatim free-text describing specifically-named off-limits zones (e.g., "Backbone, Temescal or Rustic Canyon Trails", "South Beach (south of Lifeguard Tower 3)"). Empty string if none.

Return ONLY a JSON object with all 8 fields:
{
  "sand": "...", "water": "...", "picnic_area": "...",
  "parking_lot": "...", "trails": "...", "campground": "...",
  "designated_dog_zones": "...",
  "prohibited_areas": "..."
}
NO other prose. NO markdown fences."""


def call_haiku(quote: str, unit_name: str) -> dict | None:
    user = f"Park: {unit_name}\n\nQuote: {quote}"
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


def fetch_target_rows(counties: list[str] | None) -> list[dict]:
    """Rows with extraction_model='table_parse' (CDPR master) optionally
    scoped to counties via beach_locations intersection."""
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Range": "0-9999"}
    if counties:
        # Use a SQL via supabase-cli for the spatial join
        import subprocess
        county_list = ",".join(f"'{c}'" for c in counties)
        sql = f"""
          with bl_sc as (
            select bl.geom from public.beach_locations bl
            join public.counties c on st_intersects(c.geom, bl.geom)
            where c.name in ({county_list})
          ),
          target_units as (
            select distinct cu.unit_id from bl_sc bl
            join public.cpad_units cu on st_intersects(cu.geom, bl.geom)
          )
          select p.cpad_unit_id, p.unit_name, p.source_quote, p.dogs_allowed
          from public.cpad_unit_dogs_policy p
          where p.extraction_model = 'table_parse'
            and p.cpad_unit_id in (select unit_id from target_units)
            and p.area_sand is null
          order by p.cpad_unit_id;
        """
        r = subprocess.run(["supabase","db","query","--linked",sql],
                           capture_output=True, text=True, timeout=60,
                           cwd=str(Path(__file__).parent.parent.parent))
        out = r.stdout
        i = out.find('[', out.find('"rows"'))
        depth = 0
        for k in range(i, len(out)):
            if out[k]=='[': depth+=1
            elif out[k]==']':
                depth-=1
                if depth==0:
                    return json.loads(out[i:k+1])
        return []
    else:
        r = httpx.get(f"{SUPABASE_URL}/rest/v1/cpad_unit_dogs_policy",
                      headers=headers,
                      params={"select":"cpad_unit_id,unit_name,source_quote,dogs_allowed",
                              "extraction_model":"eq.table_parse",
                              "area_sand":"is.null"},
                      timeout=30)
        r.raise_for_status()
        return r.json()


def db_update(unit_id: int, fields: dict) -> bool:
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Content-Type": "application/json"}
    r = httpx.patch(f"{SUPABASE_URL}/rest/v1/cpad_unit_dogs_policy",
                    headers=headers,
                    params={"cpad_unit_id": f"eq.{unit_id}"},
                    json=fields, timeout=30)
    if r.status_code >= 400:
        print(f"    update {unit_id} {r.status_code}: {r.text[:200]}", file=sys.stderr)
        return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--counties", type=str, default=None,
                    help="comma-separated, e.g. 'Los Angeles,Orange,San Diego'")
    args = ap.parse_args()

    counties = [c.strip() for c in args.counties.split(",")] if args.counties else None
    rows = fetch_target_rows(counties)
    print(f"Loaded {len(rows)} rows needing area fills")

    valid_rules = {"off_leash","on_leash","forbidden","unknown"}
    n_ok = 0
    for n, row in enumerate(rows, 1):
        unit_id = row["cpad_unit_id"]
        unit_name = row["unit_name"] or ""
        quote = row["source_quote"] or ""
        if not quote:
            print(f"  [{n}/{len(rows)}] unit {unit_id}: no quote, skip")
            continue
        result = call_haiku(quote, unit_name)
        if not result:
            print(f"  [{n}/{len(rows)}] unit {unit_id}: classify failed")
            continue
        # Sanitize
        fields = {}
        for area in ("sand","water","picnic_area","parking_lot","trails","campground"):
            v = (result.get(area) or "").strip().lower().replace("-","_")
            if v not in valid_rules:
                v = "unknown"
            fields[f"area_{area}"] = v
        fields["designated_dog_zones"] = (result.get("designated_dog_zones") or "").strip() or None
        fields["prohibited_areas"]     = (result.get("prohibited_areas") or "").strip() or None

        if db_update(unit_id, fields):
            n_ok += 1
            ar_summary = " ".join(f"{a[0]}={fields[f'area_{a}'][0:1]}" for a in ("sand","water","picnic_area","parking_lot","trails","campground"))
            print(f"  [{n}/{len(rows)}] unit {unit_id} {unit_name!r}: {ar_summary}")
        time.sleep(0.5)

    print(f"\nDone. {n_ok}/{len(rows)} updated.")


if __name__ == "__main__":
    main()
