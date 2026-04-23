"""
audit_datasets.py
-----------------
Data-integrity + cross-reference audit for every PostGIS dataset we
loaded over the last two days. Three passes:

  1. Per-table health   — row count, size, geometry validity, null counts
  2. CA cross-reference — for each of the 955 CA points in
     us_beach_points, how many hit each spatial layer (with buffer)
  3. Named examples     — five well-known CA beaches with every layer
     they intersect, so you can eyeball whether signals compose correctly
"""

import json
import subprocess
from pathlib import Path

ROOT = Path(r"C:\Users\beach\Documents\dog-beach-claude")
TMP  = ROOT / "supabase" / ".temp" / "q.sql"


def run_sql(sql):
    TMP.parent.mkdir(parents=True, exist_ok=True)
    TMP.write_text(sql, encoding="utf-8")
    r = subprocess.run(
        ["supabase", "db", "query", "--linked", "-f", str(TMP)],
        capture_output=True, text=True, timeout=300, cwd=str(ROOT),
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    s, e = r.stdout.find("{"), r.stdout.rfind("}")
    return json.loads(r.stdout[s:e+1])["rows"]


# ── Pass 1: per-table health ───────────────────────────────────────────────
TABLES = [
    ("states",             "geom"),
    ("counties",           "geom"),
    ("us_beach_points",    "geom"),
    ("cpad_units",         "geom"),
    ("ccc_access_points",  "geom"),
    ("mpas",               "geom"),
    ("military_bases",     "geom"),
    ("tribal_lands",       "geom"),
    ("csp_parks",          "geom"),
    ("waterbodies",        "geom"),
    ("noaa_stations",      "geom"),
]

print("=" * 80)
print("Pass 1: Per-table health")
print("=" * 80)
print(f"  {'table':<22} {'rows':>8}  {'size':>10}  {'invalid':>8}  {'null geom':>10}")
print(f"  {'-'*22} {'-'*8}  {'-'*10}  {'-'*8}  {'-'*10}")
for tbl, geomcol in TABLES:
    q = f"""
      select
        (select count(*) from public.{tbl})                                            as rows,
        pg_size_pretty(pg_total_relation_size('public.{tbl}'::regclass))               as size,
        (select count(*) from public.{tbl} where {geomcol} is null)                    as null_geom,
        (select count(*) from public.{tbl} where {geomcol} is not null and not ST_IsValid({geomcol})) as invalid
    """
    for r in run_sql(q):
        print(f"  {tbl:<22} {r['rows']:>8,}  {r['size']:>10}  {r['invalid']:>8}  {r['null_geom']:>10}")

# ── Pass 2: CA cross-reference matrix ──────────────────────────────────────
print()
print("=" * 80)
print("Pass 2: CA cross-reference — how many of the 955 CA beach points")
print("        hit each spatial layer (with 100m geom-level buffer)")
print("=" * 80)
layers = [
    ("counties",          "in any county polygon",            "0.001"),
    ("cpad_units",        "in CPAD protected area",           "0.001"),
    ("ccc_access_points", "has nearby CCC access point",      "0.002"),  # ~200m pt-pt
    ("mpas",              "in/near a Marine Protected Area",  "0.001"),
    ("military_bases",    "in military installation",         "0.001"),
    ("tribal_lands",      "in tribal land",                   "0.001"),
    ("csp_parks",         "in CA State Park",                 "0.001"),
    ("waterbodies",       "on/near lake or reservoir (1km)",  "0.01"),   # lake parking often 500m+ inland
]

for tbl, desc, buf in layers:
    q = f"""
      select count(distinct b.fid) as n
      from public.us_beach_points b
      join public.{tbl} x on ST_DWithin(x.geom, b.geom, {buf})
      where b.state = 'CA'
    """
    for r in run_sql(q):
        print(f"  {r['n']:>4d} / 955   {desc}  ({tbl})")

# ── Pass 3: Named examples ─────────────────────────────────────────────────
print()
print("=" * 80)
print("Pass 3: Spatial attribution for 5 well-known CA beaches")
print("=" * 80)

samples = [
    ("Will Rogers State Beach (LA)",  34.034411,  -118.535359),
    ("Torrey Pines State Beach (SD)", 32.928157,  -117.259870),
    ("Tijuana Beach Outlet (SD)",     32.539091,  -117.126722),   # USFWS
    ("Pfeiffer Beach (Big Sur)",      36.238083,  -121.814111),   # USFS
    ("El Dorado Beach (Tahoe)",       38.939167,  -119.986389),   # lake
]

for name, lat, lon in samples:
    print(f"\n{name}  ({lat}, {lon})")
    q = f"""
      select
        (select name_full from counties c where ST_Contains(c.geom, ST_SetSRID(ST_MakePoint({lon},{lat}), 4326))) as county,
        (select unit_name from cpad_units u where ST_DWithin(u.geom, ST_SetSRID(ST_MakePoint({lon},{lat}), 4326), 0.001) order by u.geom <-> ST_SetSRID(ST_MakePoint({lon},{lat}), 4326) limit 1) as cpad,
        (select agncy_name from cpad_units u where ST_DWithin(u.geom, ST_SetSRID(ST_MakePoint({lon},{lat}), 4326), 0.001) order by u.geom <-> ST_SetSRID(ST_MakePoint({lon},{lat}), 4326) limit 1) as cpad_agncy,
        (select unit_name from csp_parks u where ST_DWithin(u.geom, ST_SetSRID(ST_MakePoint({lon},{lat}), 4326), 0.001) order by u.geom <-> ST_SetSRID(ST_MakePoint({lon},{lat}), 4326) limit 1) as csp,
        (select subtype from csp_parks u where ST_DWithin(u.geom, ST_SetSRID(ST_MakePoint({lon},{lat}), 4326), 0.001) order by u.geom <-> ST_SetSRID(ST_MakePoint({lon},{lat}), 4326) limit 1) as csp_subtype,
        (select name from mpas m where ST_DWithin(m.geom, ST_SetSRID(ST_MakePoint({lon},{lat}), 4326), 0.001) limit 1) as mpa,
        (select site_name from military_bases mb where ST_DWithin(mb.geom, ST_SetSRID(ST_MakePoint({lon},{lat}), 4326), 0.001) limit 1) as military,
        (select gnis_name from waterbodies w where ST_DWithin(w.geom, ST_SetSRID(ST_MakePoint({lon},{lat}), 4326), 0.001) and w.gnis_name is not null limit 1) as lake,
        (select name from ccc_access_points c where ST_DWithin(c.geom, ST_SetSRID(ST_MakePoint({lon},{lat}), 4326), 0.002) order by c.geom <-> ST_SetSRID(ST_MakePoint({lon},{lat}), 4326) limit 1) as ccc,
        (select name from noaa_stations n where n.reference_id is null order by n.geom <-> ST_SetSRID(ST_MakePoint({lon},{lat}), 4326) limit 1) as noaa_nearest
    """
    for r in run_sql(q):
        def p(k, lbl):
            v = r.get(k)
            if v: print(f"  {lbl:<20s} {v}")
        p("county",       "county")
        p("cpad",         "CPAD unit")
        p("cpad_agncy",   "CPAD agency")
        p("csp",          "CSP park")
        p("csp_subtype",  "CSP subtype")
        p("mpa",          "MPA")
        p("military",     "military base")
        p("lake",         "lake/reservoir")
        p("ccc",          "CCC access")
        p("noaa_nearest", "nearest NOAA stn")
