"""
extract_cdpr_master_pet_table.py
--------------------------------
Single-fetch alternative to per-park CDPR scraping.

CDPR publishes a master "Visiting State Parks With Your Dog" page at
https://www.parks.ca.gov/dogs that contains a structured markdown
table: park name, dogs allowed (Yes/No), restrictions.

This bypasses the JS-rendered per-park dog-policy widget entirely
(which Tavily can't see). One fetch, parse the table, match rows to
our CPAD units by unit_name trigram similarity, upsert into
cpad_unit_dogs_policy with url_used=this master page.

Idempotent — overwrites existing CDPR rows in cpad_unit_dogs_policy.
"""
from __future__ import annotations
import json, os, re, sys, time
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "pipeline" / ".env")
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SERVICE_KEY       = os.environ["SUPABASE_SERVICE_KEY"]
TAVILY_API_KEY    = os.environ["TAVILY_API_KEY"]
MASTER_URL        = "https://www.parks.ca.gov/dogs"


def tavily_extract(url: str) -> str:
    r = httpx.post("https://api.tavily.com/extract",
        json={"api_key": TAVILY_API_KEY, "urls":[url]}, timeout=90)
    r.raise_for_status()
    res = r.json().get("results", [])
    return res[0].get("raw_content","") if res else ""


# Markdown table row: | [Park Name](/?page_id=N) | Yes/No | Restrictions |
TABLE_ROW_RX = re.compile(
    r'\|\s*\[(?P<name>[^\]]+)\]\((?P<href>[^)]+)\)\s*\|\s*(?P<allowed>[A-Za-z]+)\s*\|(?P<restr>.*?)\|',
    re.DOTALL
)


def parse_table(text: str) -> list[dict]:
    rows = []
    for m in TABLE_ROW_RX.finditer(text):
        allowed_raw = m.group("allowed").strip().lower()
        if allowed_raw == "yes":
            rule = "yes"
        elif allowed_raw == "no":
            rule = "no"
        else:
            rule = "unknown"
        rows.append({
            "name": m.group("name").strip(),
            "href": m.group("href").strip(),
            "allowed_raw": m.group("allowed").strip(),
            "rule": rule,
            "restrictions": " ".join(m.group("restr").split()).strip(),
        })
    return rows


def fetch_cdpr_units_in_805_la_oc_sd() -> list[dict]:
    """CDPR units that intersect 805 in LA/OC/SD."""
    import subprocess
    sql = """
      with bl_sc as (
        select bl.geom from public.beach_locations bl
        join public.counties c on st_intersects(c.geom, bl.geom)
        where c.name in ('Los Angeles','Orange','San Diego')
      )
      select distinct cu.unit_id, cu.unit_name, cu.agncy_name
      from bl_sc bl
      join public.cpad_units cu on st_intersects(cu.geom, bl.geom)
      where cu.agncy_name = 'California Department of Parks and Recreation';
    """
    r = subprocess.run(["supabase","db","query","--linked",sql],
                       capture_output=True, text=True, timeout=60,
                       cwd=str(Path(__file__).parent.parent))
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


def trigram_match(target_name: str, threshold: float = 0.6) -> dict | None:
    """Pure-Python trigram-ish: name overlap. For best match against CPAD unit names."""
    # Use Postgres trigram match via a quick query for accuracy
    import subprocess
    safe = target_name.replace("'","''")
    sql = f"""
      select unit_id, unit_name, similarity('{safe}', unit_name) as sim
      from cpad_units
      where agncy_name = 'California Department of Parks and Recreation'
        and similarity('{safe}', unit_name) >= {threshold}
      order by sim desc limit 1;
    """
    r = subprocess.run(["supabase","db","query","--linked",sql],
                       capture_output=True, text=True, timeout=30,
                       cwd=str(Path(__file__).parent.parent))
    out = r.stdout
    i = out.find('[', out.find('"rows"'))
    depth = 0
    for k in range(i, len(out)):
        if out[k]=='[': depth+=1
        elif out[k]==']':
            depth-=1
            if depth==0:
                rows = json.loads(out[i:k+1])
                return rows[0] if rows else None
    return None


def db_upsert(rows: list[dict]) -> int:
    if not rows: return 0
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    ok = 0
    # Per-row to avoid one bad row killing the batch
    for row in rows:
        r = httpx.post(f"{SUPABASE_URL}/rest/v1/cpad_unit_dogs_policy",
                       headers=headers, json=[row], timeout=30,
                       params={"on_conflict": "cpad_unit_id"})
        if r.status_code < 400:
            ok += 1
        else:
            print(f"    SKIP unit {row.get('cpad_unit_id')}: {r.status_code} {r.text[:200]}", file=sys.stderr)
    return ok


def main():
    print(f"Fetching {MASTER_URL}")
    text = tavily_extract(MASTER_URL)
    print(f"  {len(text)} chars")

    rows = parse_table(text)
    print(f"Parsed {len(rows)} table rows")
    by_rule = {}
    for r in rows:
        by_rule[r["rule"]] = by_rule.get(r["rule"], 0) + 1
    print(f"  rule mix: {by_rule}")

    # Restrict to LA-OC-SD CDPR units (focus area for this run)
    target_units = fetch_cdpr_units_in_805_la_oc_sd()
    target_names = {u["unit_name"] for u in target_units}
    print(f"\nTarget LA-OC-SD CDPR units: {len(target_units)}")

    matched = []
    for tr in rows:
        m = trigram_match(tr["name"])
        if not m:
            continue
        # Only emit for units in our LA-OC-SD focus
        if m["unit_name"] not in target_names:
            continue
        # Build the upsert row
        leash_required = None
        rest = tr["restrictions"].lower()
        if any(p in rest for p in ('leash','leashed','must be on')):
            leash_required = True
        if tr["rule"] == "yes" and tr["restrictions"]:
            # downgrade to "restricted" if restrictions exist
            rule = "restricted" if any(p in rest for p in ('not allowed','prohibited','only','except','no dogs')) else "yes"
        else:
            rule = tr["rule"]

        matched.append({
            "cpad_unit_id": m["unit_id"],
            "unit_name": m["unit_name"],
            "agency_name": "California Department of Parks and Recreation",
            "url_used": MASTER_URL,
            "url_kind": "agncy_web",
            "dogs_allowed": rule,
            "default_rule": rule,
            "leash_required": leash_required,
            "exceptions": None,
            "time_windows": None,
            "seasonal_rules": None,
            "source_quote": (f"{tr['allowed_raw']}: {tr['restrictions']}" if tr["restrictions"] else tr["allowed_raw"])[:1000],
            "ordinance_ref": None,
            "extraction_model": "table_parse",
            "extraction_confidence": 0.95,
        })
        print(f"  matched: {tr['name']!r} -> unit {m['unit_id']} ({m['unit_name']!r}, sim={m['sim']:.2f}) -> {rule}")
        time.sleep(0.05)

    print(f"\nUpserting {len(matched)} CDPR rows...")
    ok = db_upsert(matched)
    print(f"Done. {ok}/{len(matched)} upserted.")


if __name__ == "__main__":
    main()
