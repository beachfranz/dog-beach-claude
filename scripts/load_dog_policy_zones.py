"""load_dog_policy_zones.py — populate public.dog_policy_zones from external sources.

Loaders:

  --pore  : NPS Point Reyes pet-allowed carve-outs (hand-curated)
  --wsp   : USFWS Western Snowy Plover critical habitat (Living Atlas REST)
  --lt    : CDFW California least tern monitoring sites (CA Open Data REST)
  --all   : everything above

The original --wsp loader pulled a zip from ecos.fws.gov; that URL went
404 in 2026-05 when USFWS reorganized into FWS Open Data. Replaced with
the Living Atlas FeatureServer query API — same pattern as
scripts/load_pad_us_state.py.

Idempotent — DELETEs by source_agency + species before INSERT.
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
sys.path.insert(0, str(ROOT))
from scripts.common.ssl_compat import get_ssl_context  # noqa: E402
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
# Generic ArcGIS REST FeatureServer query helper
# ============================================================================
#
# Used by both --wsp (USFWS Critical Habitat) and --lt (CDFW Least Tern).
# Replaces the dead `ecos.fws.gov/docs/crithab/CRITHAB.zip` download path.

_SSL_CTX = get_ssl_context()  # Py 3.13+ strictness; see scripts/common/ssl_compat.py


def fetch_rest_features(endpoint: str, where: str, page_size: int = 1000) -> list[dict]:
    """Page through an ArcGIS REST FeatureServer query, return all GeoJSON features.

    endpoint: base URL of the layer (no trailing /query).
    where: SQL-style WHERE clause; pass '1=1' for all rows.
    """
    feats: list[dict] = []
    offset = 0
    while True:
        qs = urllib.parse.urlencode({
            "where": where,
            "outFields": "*",
            "f": "geojson",
            "outSR": 4326,
            "resultOffset": offset,
            "resultRecordCount": page_size,
        })
        url = f"{endpoint}/query?{qs}"
        req = urllib.request.Request(url, headers={"User-Agent": "dog-beach-scout/1.0"})
        with urllib.request.urlopen(req, context=_SSL_CTX, timeout=120) as r:
            page = json.loads(r.read())
        page_feats = page.get("features", []) or []
        feats.extend(page_feats)
        if len(page_feats) < page_size:
            break
        offset += page_size
    return feats


# ============================================================================
# USFWS Western Snowy Plover critical habitat (REST)
# ============================================================================

USFWS_CRITHAB_FS = (
    "https://services.arcgis.com/QVENGdaPbd4LUkLV/ArcGIS/rest/services/"
    "USFWS_Critical_Habitat/FeatureServer/0"
)


def load_wsp(conn) -> int:
    """Pull WSP critical habitat polygons from USFWS Living Atlas FeatureServer."""
    print("  source:", USFWS_CRITHAB_FS)
    try:
        feats = fetch_rest_features(
            USFWS_CRITHAB_FS,
            where="UPPER(comname) LIKE '%WESTERN SNOWY PLOVER%'",
        )
    except Exception as e:
        print(f"  ERROR fetching WSP from REST: {e}")
        return 0
    print(f"  fetched {len(feats)} WSP features from REST")
    if not feats:
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            delete from public.dog_policy_zones
             where source_agency = 'USFWS' and species = 'snowy_plover'
        """)
        n = cur.rowcount

    inserts = []
    for feat in feats:
        geom = feat.get("geometry")
        if not geom:
            continue
        props = feat.get("properties") or {}
        unit = props.get("unitname") or props.get("unit") or "Critical Habitat Unit"
        inserts.append({
            "category": "wildlife_critical_habitat",
            "name": f"WSP {unit}",
            "species": "snowy_plover",
            "source_agency": "USFWS",
            "source_url": "https://ecos.fws.gov/ecp/species/8035",
            "effective_dates": json.dumps({"start": "03-01", "end": "09-30"}),
            "notes": ("Western Snowy Plover Critical Habitat (Pacific coast). "
                      "Default nesting season Mar 1 - Sep 30 — beaches in this zone "
                      "should emit nesting_zones section with seasonal closure."),
            "geom_geojson": json.dumps(geom),
        })

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            insert into public.dog_policy_zones
              (category, name, species, source_agency, source_url,
               effective_dates, notes, geom, area_m2)
            values (
              %(category)s, %(name)s, %(species)s, %(source_agency)s, %(source_url)s,
              %(effective_dates)s::jsonb, %(notes)s,
              st_multi(st_makevalid(st_setsrid(st_geomfromgeojson(%(geom_geojson)s), 4326))),
              st_area(st_makevalid(st_setsrid(st_geomfromgeojson(%(geom_geojson)s), 4326))::geography)
            )
        """, inserts, page_size=50)
    conn.commit()
    print(f"  USFWS WSP: deleted {n} stale rows, inserted {len(inserts)} habitat polygons")
    return len(inserts)


# ============================================================================
# CDFW California Least Tern monitoring sites (REST)
# ============================================================================
#
# CDFW's BIOS dataset ds3146 — generalized colony footprints. Polygons.
# Range: Tijuana river mouth -> Sacramento. ~50 records.
# Nesting season is roughly Apr 15 - Sep 15.

CDFW_LT_FS = (
    "https://services2.arcgis.com/Uq9r85Potqm3MfRV/arcgis/rest/services/"
    "biosds3146_fpu/FeatureServer/0"
)


def load_lt(conn) -> int:
    """Pull CDFW California Least Tern monitoring polygons from BIOS ds3146."""
    print("  source:", CDFW_LT_FS)
    try:
        feats = fetch_rest_features(CDFW_LT_FS, where="1=1")
    except Exception as e:
        print(f"  ERROR fetching LT from REST: {e}")
        return 0
    print(f"  fetched {len(feats)} LT features from REST")
    if not feats:
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            delete from public.dog_policy_zones
             where source_agency = 'CDFW' and species = 'least_tern'
        """)
        n = cur.rowcount

    inserts = []
    for feat in feats:
        geom = feat.get("geometry")
        if not geom:
            continue
        props = feat.get("properties") or {}
        site = props.get("Site_Name") or props.get("site_name") or "LT colony"
        region = props.get("Region") or props.get("region") or ""
        inserts.append({
            "category": "wildlife_critical_habitat",
            "name": f"LT colony: {site}" + (f" ({region})" if region else ""),
            "species": "least_tern",
            "source_agency": "CDFW",
            "source_url": "https://gis.data.ca.gov/maps/CDFW::california-least-tern-monitoring-sites-generalized-cdfw-ds3146-1",
            "effective_dates": json.dumps({"start": "04-15", "end": "09-15"}),
            "notes": ("California Least Tern (Sternula antillarum browni) colony. "
                      "State + federally endangered. Nesting season Apr 15 - Sep 15 — "
                      "beaches in this zone should emit nesting_zones section with "
                      "seasonal closure."),
            "geom_geojson": json.dumps(geom),
        })

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            insert into public.dog_policy_zones
              (category, name, species, source_agency, source_url,
               effective_dates, notes, geom, area_m2)
            values (
              %(category)s, %(name)s, %(species)s, %(source_agency)s, %(source_url)s,
              %(effective_dates)s::jsonb, %(notes)s,
              st_multi(st_makevalid(st_setsrid(st_geomfromgeojson(%(geom_geojson)s), 4326))),
              st_area(st_makevalid(st_setsrid(st_geomfromgeojson(%(geom_geojson)s), 4326))::geography)
            )
        """, inserts, page_size=50)
    conn.commit()
    print(f"  CDFW LT: deleted {n} stale rows, inserted {len(inserts)} colony polygons")
    return len(inserts)


# ============================================================================
# Main
# ============================================================================

def main():
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    ap = argparse.ArgumentParser()
    ap.add_argument("--pore", action="store_true", help="Load NPS Point Reyes carve-outs")
    ap.add_argument("--wsp",  action="store_true", help="Load USFWS Western Snowy Plover critical habitat (Living Atlas REST)")
    ap.add_argument("--lt",   action="store_true", help="Load CDFW California Least Tern colonies (BIOS REST)")
    ap.add_argument("--all",  action="store_true", help="Load all sources")
    args = ap.parse_args()

    if not (args.pore or args.wsp or args.lt or args.all):
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
        if args.all or args.lt:
            print("\n=== CDFW California Least Tern colonies ===")
            total += load_lt(conn)

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
