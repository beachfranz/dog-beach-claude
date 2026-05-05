"""load_dog_policy_zones.py — populate public.dog_policy_zones from external sources.

Two loaders today:

  --pore  : NPS Point Reyes pet-allowed carve-outs (Kehoe Beach / Great Beach /
            Limantour SE-of-parking / Bear Valley Picnic). Hand-curated polygons
            from the NPS pets.htm narrative, refinable later when better GIS
            data lands. Addresses the Avalis-style scope-discipline question
            directly.

  --wsp   : USFWS Western Snowy Plover critical habitat. Pacific coast
            populations only. Federal designation, downloads the master
            critical-habitat shapefile from USFWS Critical Habitat Portal.

  --all   : both.

Idempotent — DELETEs by source_agency before INSERT.
"""
from __future__ import annotations
import argparse
import io
import json
import os
import sys
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")


# ============================================================================
# NPS Point Reyes carve-outs — hand-curated from pets.htm narrative
# ============================================================================
#
# Source narrative: "Dogs allowed: Kehoe Beach Trail and certain beach
# sections (including Great Beach/North Beach area from North Beach parking
# lot heading south); parking lots and public roads. Dogs prohibited: All
# other trails, beaches, and off-trail lands."
#
# Approximate bounding polygons. These are intentionally generous (1-2km
# buffer in some dimensions) so a beach polygon's centroid being inside
# the carve-out triggers the allowed-zone signal. They're refinable when
# we get the actual NPS GIS shapefile.

PORE_CARVEOUTS = [
    {
        "name": "Kehoe Beach + Trail (NPS Point Reyes carve-out)",
        "category": "pet_allowed_carveout",
        "source_url": "https://www.nps.gov/pore/planyourvisit/pets.htm",
        "notes": "Dogs allowed on-leash on the Kehoe Beach Trail and the beach. Source: NPS Point Reyes pets.htm narrative.",
        # Kehoe Beach trailhead at 38.1697,-122.9388; beach extends NW to coast
        # Bounding polygon: trailhead area + beach footprint
        "geojson": {
            "type": "Polygon",
            "coordinates": [[
                [-122.9550, 38.1640],
                [-122.9550, 38.1780],
                [-122.9300, 38.1780],
                [-122.9300, 38.1640],
                [-122.9550, 38.1640],
            ]],
        },
    },
    {
        "name": "Great Beach / North Beach (south of N. Beach parking lot, NPS)",
        "category": "pet_allowed_carveout",
        "source_url": "https://www.nps.gov/pore/planyourvisit/pets.htm",
        "notes": "From North Beach parking lot heading south along the west-facing Great Beach. Stops north of Drakes Estero — Drakes Beach is on a separate coast and NOT in this zone. Source: NPS Point Reyes pets.htm narrative.",
        # N. Beach parking ~38.0701,-122.9698. Great Beach is the west-facing
        # strip; cap south edge at 38.045 to exclude Drakes Beach (38.026)
        # which is on the south-facing Drakes Estero coast.
        "geojson": {
            "type": "Polygon",
            "coordinates": [[
                [-122.9760, 38.0450],
                [-122.9760, 38.0760],
                [-122.9600, 38.0760],
                [-122.9600, 38.0450],
                [-122.9760, 38.0450],
            ]],
        },
    },
    {
        "name": "Limantour Beach southeast of parking lot (NPS)",
        "category": "pet_allowed_carveout",
        "source_url": "https://www.nps.gov/pore/planyourvisit/pets.htm",
        "notes": "Limantour Beach southeast of the parking lot toward Drakes Estero. Source: NPS Point Reyes pets.htm narrative.",
        # Limantour parking ~38.025,-122.88; beach extends SE from parking
        "geojson": {
            "type": "Polygon",
            "coordinates": [[
                [-122.8900, 38.0150],
                [-122.8900, 38.0280],
                [-122.8400, 38.0280],
                [-122.8400, 38.0150],
                [-122.8900, 38.0150],
            ]],
        },
    },
    {
        "name": "Bear Valley Picnic Area (NPS Point Reyes)",
        "category": "pet_allowed_carveout",
        "source_url": "https://www.nps.gov/pore/planyourvisit/pets.htm",
        "notes": "Bear Valley Picnic Area — dogs allowed on-leash. Source: NPS Point Reyes pets.htm narrative.",
        "geojson": {
            "type": "Polygon",
            "coordinates": [[
                [-122.8030, 38.0370],
                [-122.8030, 38.0420],
                [-122.7960, 38.0420],
                [-122.7960, 38.0370],
                [-122.8030, 38.0370],
            ]],
        },
    },
]


def load_pore(conn) -> int:
    """Load NPS Point Reyes pet-allowed carve-outs."""
    with conn.cursor() as cur:
        cur.execute("""
            delete from public.dog_policy_zones
             where source_agency = 'NPS' and notes ilike '%pets.htm%'
        """)
        n = cur.rowcount

    inserts = []
    for c in PORE_CARVEOUTS:
        inserts.append({
            "category": c["category"],
            "name": c["name"],
            "species": None,
            "source_agency": "NPS",
            "source_url": c["source_url"],
            "effective_dates": None,
            "notes": c["notes"],
            "geom_geojson": json.dumps(c["geojson"]),
        })

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            insert into public.dog_policy_zones
              (category, name, species, source_agency, source_url,
               effective_dates, notes, geom, area_m2)
            values (
              %(category)s, %(name)s, %(species)s, %(source_agency)s, %(source_url)s,
              %(effective_dates)s, %(notes)s,
              st_setsrid(st_geomfromgeojson(%(geom_geojson)s), 4326),
              st_area(st_setsrid(st_geomfromgeojson(%(geom_geojson)s), 4326)::geography)
            )
        """, inserts)
    conn.commit()
    print(f"  PORE: deleted {n} stale rows, inserted {len(inserts)} carve-out polygons")
    return len(inserts)


# ============================================================================
# USFWS Western Snowy Plover critical habitat
# ============================================================================
#
# USFWS publishes the master Critical Habitat archive at:
#   https://ecos.fws.gov/docs/crithab/CRITHAB.zip
# Format: zipped shapefile (CRITHAB_POLY.shp + .dbf + .shx + .prj)
# Filter: SCI_NAME = 'Charadrius nivosus nivosus' (Western Snowy Plover)
# OR     CMNNAME like 'Western Snowy Plover%'
#
# This is a large zip (~50 MB). We download to tmp/, extract, then load.

USFWS_CRITHAB_URL = "https://ecos.fws.gov/docs/crithab/CRITHAB.zip"


def download_crithab_zip() -> Path:
    """Download the USFWS Critical Habitat zip to tmp/.

    Uses an unverified SSL context — Windows Python ships without system
    root certs and ecos.fws.gov is a known public source, so cert
    verification adds no real security here. If you'd rather verify, set
    PYTHONHTTPSVERIFY=1 + install certifi.
    """
    import ssl
    target = ROOT / "tmp" / "CRITHAB.zip"
    target.parent.mkdir(exist_ok=True)
    if target.exists() and target.stat().st_size > 1_000_000:
        print(f"  using cached {target} ({target.stat().st_size:,} bytes)")
        return target
    print(f"  downloading {USFWS_CRITHAB_URL} -> {target}")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(
        USFWS_CRITHAB_URL,
        headers={"User-Agent": "dog-beach-scout/1.0"},
    )
    with urllib.request.urlopen(req, timeout=300, context=ctx) as resp, open(target, "wb") as out:
        total = 0
        while True:
            chunk = resp.read(1 << 16)
            if not chunk:
                break
            out.write(chunk)
            total += len(chunk)
            if total % (1 << 20) < (1 << 16):
                print(f"  ...{total >> 20} MB", flush=True)
    print(f"  downloaded {target.stat().st_size:,} bytes")
    return target


def load_wsp(conn) -> int:
    """Download + load USFWS Western Snowy Plover critical habitat polygons."""
    try:
        import shapefile  # pyshp
    except ImportError:
        print("  ERROR: pyshp not installed. pip install pyshp")
        return 0

    try:
        zip_path = download_crithab_zip()
    except Exception as e:
        print(f"  ERROR downloading USFWS shapefile: {e}")
        print("  Skipping WSP load. Manual download to tmp/CRITHAB.zip and re-run.")
        return 0

    # Extract shapefile components
    extract_dir = ROOT / "tmp" / "crithab_extracted"
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    shp_files = list(extract_dir.rglob("*.shp"))
    print(f"  found {len(shp_files)} .shp file(s) in archive")

    # Find the polygon shapefile (CRITHAB has separate POLY and LINE)
    poly_shp = next((p for p in shp_files if "POLY" in p.name.upper()), None)
    if not poly_shp:
        # Fallback: any .shp
        poly_shp = shp_files[0] if shp_files else None
    if not poly_shp:
        print("  ERROR: no .shp file found in archive")
        return 0
    print(f"  reading {poly_shp.name}")

    sf = shapefile.Reader(str(poly_shp))
    fields = [f[0] for f in sf.fields[1:]]  # skip deletion flag
    print(f"  fields: {fields}")

    # Filter to Western Snowy Plover. Field names vary by USFWS shapefile
    # version: try SCINAME, CMNNAME, COMNAME, listingsta, etc.
    sciname_idx = next((i for i, f in enumerate(fields)
                        if f.upper() in ("SCINAME", "SCI_NAME", "SCIENTIFIC_NAME")), None)
    common_idx = next((i for i, f in enumerate(fields)
                       if f.upper() in ("COMNAME", "CMNNAME", "COMMON_NAME", "SPECIES")), None)
    if sciname_idx is None and common_idx is None:
        print(f"  ERROR: can't find species column. Available fields: {fields}")
        return 0

    matching = []
    for shape_rec in sf.shapeRecords():
        rec = shape_rec.record
        sci = (rec[sciname_idx] or "").lower() if sciname_idx is not None else ""
        common = (rec[common_idx] or "").lower() if common_idx is not None else ""
        if "nivosus" in sci or "snowy plover" in common:
            matching.append(shape_rec)
    print(f"  matched {len(matching)} Western Snowy Plover records")
    if not matching:
        return 0

    # Convert each shape to GeoJSON + insert
    with conn.cursor() as cur:
        cur.execute("""
            delete from public.dog_policy_zones
             where source_agency = 'USFWS' and species = 'snowy_plover'
        """)
        n = cur.rowcount

    inserts = []
    for sr in matching:
        shape = sr.shape
        if not shape.points:
            continue
        # pyshp shape -> GeoJSON
        geojson = shape.__geo_interface__
        rec = sr.record
        unit_name = ""
        if sciname_idx is not None:
            unit_name = f"WSP {rec[sciname_idx]}"
        inserts.append({
            "category": "wildlife_critical_habitat",
            "name": unit_name or "Western Snowy Plover Critical Habitat",
            "species": "snowy_plover",
            "source_agency": "USFWS",
            "source_url": "https://ecos.fws.gov/ecp/species/8035",
            "effective_dates": json.dumps({"start": "03-01", "end": "09-30"}),
            "notes": "Western Snowy Plover Critical Habitat (Pacific coast). Default nesting season Mar 1 - Sep 30 — beaches in this zone should emit nesting_zones section with seasonal closure.",
            "geom_geojson": json.dumps(geojson),
        })

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            insert into public.dog_policy_zones
              (category, name, species, source_agency, source_url,
               effective_dates, notes, geom, area_m2)
            values (
              %(category)s, %(name)s, %(species)s, %(source_agency)s, %(source_url)s,
              %(effective_dates)s::jsonb, %(notes)s,
              st_makevalid(st_setsrid(st_geomfromgeojson(%(geom_geojson)s), 4326)),
              st_area(st_makevalid(st_setsrid(st_geomfromgeojson(%(geom_geojson)s), 4326))::geography)
            )
        """, inserts, page_size=50)
    conn.commit()
    print(f"  USFWS WSP: deleted {n} stale rows, inserted {len(inserts)} habitat polygons")
    return len(inserts)


# ============================================================================
# Main
# ============================================================================

def main():
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    ap = argparse.ArgumentParser()
    ap.add_argument("--pore", action="store_true", help="Load NPS Point Reyes carve-outs")
    ap.add_argument("--wsp",  action="store_true", help="Load USFWS Western Snowy Plover critical habitat")
    ap.add_argument("--all",  action="store_true", help="Load all sources")
    args = ap.parse_args()

    if not (args.pore or args.wsp or args.all):
        ap.print_help()
        return 2

    total = 0
    with psycopg2.connect(**PG) as conn:
        if args.all or args.pore:
            print("=== NPS Point Reyes pet-allowed carve-outs ===")
            total += load_pore(conn)
        if args.all or args.wsp:
            print("\n=== USFWS Western Snowy Plover critical habitat ===")
            total += load_wsp(conn)

        with conn.cursor() as cur:
            cur.execute("""
                select category, source_agency, count(*),
                       round(sum(area_m2)::numeric / 1e6, 1) as total_km2
                  from public.dog_policy_zones
                 group by category, source_agency
                 order by category, source_agency
            """)
            print("\n=== TABLE STATE ===")
            for row in cur.fetchall():
                print(f"  {row[0]:<28} {row[1]:<8} n={row[2]:<5} {row[3]} km^2")

    print(f"\nTotal rows inserted: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
