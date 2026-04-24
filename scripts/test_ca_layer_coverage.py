"""
test_ca_layer_coverage.py
-------------------------
For the 955 CA points in us_beach_points, compute spatial coverage
against the six loaded-but-unused polygon layers:
  cpad_units, csp_parks, mpas, military_bases, tribal_lands, waterbodies

Per-layer buffers follow project_buffer_convention:
  100m (0.001°) for point-sits-inside layers
  1km  (0.01°)  for waterbodies (lake parking typically 200–600m inland)

Nine outputs:
  1. Per-layer hit count (sorted desc)
  2. Distribution of layers-per-beach (0–6)
  3. Mean layers per beach
  4. Layer × layer crossover grid
  5. Top multi-layer beaches (named)
  6. Unique-coverage per layer (beaches ONLY that layer catches)
  7. Per-county coverage density (top 10)
  8. CSP subtype breakdown for beaches in csp_parks
  9. CPAD agency-level breakdown for beaches in cpad_units
"""

import json
import subprocess
from pathlib import Path

ROOT = Path(r"C:\Users\beach\Documents\dog-beach-claude")
TMP  = ROOT / "supabase" / ".temp" / "q.sql"

# (layer, buffer_degrees) — buffer per convention
LAYERS = [
    ("cpad_units",     0.001),
    ("csp_parks",      0.001),
    ("mpas",           0.001),
    ("military_bases", 0.001),
    ("tribal_lands",   0.001),
    ("waterbodies",    0.01),
]


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


# ── Build the core hits table once — every summary reads from it ───────────
LAYER_EXISTS_SQL = ",\n".join(
    f"    exists(select 1 from public.{tbl} x where ST_DWithin(x.geom, b.geom, {buf})) as in_{tbl}"
    for tbl, buf in LAYERS
)

COUNT_EXPR = " + ".join(f"(in_{tbl})::int" for tbl, _ in LAYERS)

SETUP = f"""
drop table if exists tmp_ca_hits;
create table tmp_ca_hits as
with hits as (
  select b.fid, b.name, b.county_name, b.county_fips,
{LAYER_EXISTS_SQL}
  from public.us_beach_points b
  where b.state = 'CA'
)
select *, ({COUNT_EXPR}) as n_layers from hits;

create index tmp_ca_hits_fid on tmp_ca_hits(fid);
"""
run_sql(SETUP)

# ── 1. Per-layer hit count ─────────────────────────────────────────────────
print("=" * 80)
print("1. Per-layer hit count (of 955 CA beach points, buffer per layer)")
print("=" * 80)
layer_counts = run_sql(f"""
  select layer, n, round(n::numeric * 100.0 / 955, 1) as pct
  from ( select 'cpad_units'     as layer, count(*) filter (where in_cpad_units)     as n from tmp_ca_hits
    union all select 'csp_parks',      count(*) filter (where in_csp_parks)      as n from tmp_ca_hits
    union all select 'mpas',           count(*) filter (where in_mpas)           as n from tmp_ca_hits
    union all select 'military_bases', count(*) filter (where in_military_bases) as n from tmp_ca_hits
    union all select 'tribal_lands',   count(*) filter (where in_tribal_lands)   as n from tmp_ca_hits
    union all select 'waterbodies',    count(*) filter (where in_waterbodies)    as n from tmp_ca_hits
  ) t order by n desc
""")
for r in layer_counts:
    print(f"  {r['layer']:<18} {r['n']:>4}   {r['pct']}%")

# Save ordered list for later (used in grid)
layer_order = [r["layer"] for r in layer_counts]

# ── 2. Distribution: layers per beach ──────────────────────────────────────
print()
print("=" * 80)
print("2. Distribution — how many layers does each beach hit?")
print("=" * 80)
dist = run_sql("select n_layers, count(*) as beaches from tmp_ca_hits group by n_layers order by n_layers")
for r in dist:
    bar = "█" * int(int(r["beaches"]) / 20)
    print(f"  {r['n_layers']} layer(s): {r['beaches']:>4}  {bar}")

# ── 3. Mean layers per beach ───────────────────────────────────────────────
print()
mean_row = run_sql("select round(avg(n_layers)::numeric, 2) as mean_layers, max(n_layers) as max_layers from tmp_ca_hits")[0]
print("=" * 80)
print(f"3. Mean layers per beach: {mean_row['mean_layers']}  (max: {mean_row['max_layers']})")
print("=" * 80)

# ── 4. Crossover grid ──────────────────────────────────────────────────────
print()
print("=" * 80)
print("4. Crossover grid — beaches that hit BOTH row-layer AND column-layer")
print("   (diagonal = total hits for that layer; values ≥1 = intersection)")
print("=" * 80)
pairs_sql_parts = []
for a in layer_order:
    for b in layer_order:
        pairs_sql_parts.append(
            f"select '{a}' as a, '{b}' as b, count(*) filter (where in_{a} and in_{b}) as n from tmp_ca_hits"
        )
pairs_data = run_sql(" union all ".join(pairs_sql_parts))
grid = {(r["a"], r["b"]): r["n"] for r in pairs_data}

# Print header
short = {"cpad_units": "cpad", "csp_parks": "csp", "mpas": "mpa",
         "military_bases": "mil", "tribal_lands": "trib", "waterbodies": "water"}
print(f"  {'':<14}" + "".join(f"{short[c]:>7}" for c in layer_order))
for row in layer_order:
    cells = "".join(f"{grid[(row, col)]:>7}" for col in layer_order)
    print(f"  {row:<14}{cells}")

# ── 5. Top multi-layer beaches ─────────────────────────────────────────────
print()
print("=" * 80)
print("5. Top multi-layer beaches (most layers stacked at one point)")
print("=" * 80)
top_multi = run_sql("""
  select name, county_name, n_layers,
    in_cpad_units, in_csp_parks, in_mpas, in_military_bases, in_tribal_lands, in_waterbodies
  from tmp_ca_hits
  where n_layers >= 3
  order by n_layers desc, name
  limit 20
""")
for r in top_multi:
    flags = []
    if r["in_cpad_units"]:     flags.append("cpad")
    if r["in_csp_parks"]:      flags.append("csp")
    if r["in_mpas"]:           flags.append("mpa")
    if r["in_military_bases"]: flags.append("mil")
    if r["in_tribal_lands"]:   flags.append("trib")
    if r["in_waterbodies"]:    flags.append("water")
    print(f"  [{r['n_layers']}]  {r['name']:<45}  {r['county_name']:<22}  {','.join(flags)}")

# ── 6. Unique-coverage per layer ───────────────────────────────────────────
print()
print("=" * 80)
print("6. Unique-coverage per layer — beaches that ONLY this layer catches")
print("   (high unique count → layer is indispensable)")
print("=" * 80)
unique_sql_inner = " union all ".join(
    f"select '{tbl}' as layer, count(*) filter (where n_layers = 1 and in_{tbl}) as n from tmp_ca_hits"
    for tbl, _ in LAYERS
)
unique_rows = run_sql(f"select layer, n from ({unique_sql_inner}) t order by n desc")
for r in unique_rows:
    print(f"  {r['layer']:<18} {r['n']:>4}")

# ── 7. Per-county coverage density ─────────────────────────────────────────
print()
print("=" * 80)
print("7. Top CA counties by mean-layers-per-beach (densest enrichment potential)")
print("=" * 80)
counties = run_sql("""
  select county_name,
         count(*) as beaches,
         round(avg(n_layers)::numeric, 2) as mean_layers,
         count(*) filter (where n_layers = 0) as uncovered
  from tmp_ca_hits
  group by county_name
  having count(*) >= 10
  order by mean_layers desc
  limit 12
""")
print(f"  {'county':<28} {'beaches':>8} {'mean':>6} {'uncovered':>10}")
for r in counties:
    print(f"  {r['county_name']:<28} {r['beaches']:>8} {r['mean_layers']:>6} {r['uncovered']:>10}")

# ── 8. CSP subtype breakdown for beaches in CSP ────────────────────────────
print()
print("=" * 80)
print("8. CSP subtype distribution — beaches that land in a CA State Park")
print("=" * 80)
csp_subtype = run_sql("""
  select p.subtype, count(distinct h.fid) as beaches
  from tmp_ca_hits h
  join public.csp_parks p on ST_DWithin(p.geom, (select geom from us_beach_points where fid = h.fid), 0.001)
  where h.in_csp_parks
  group by p.subtype
  order by beaches desc
""")
for r in csp_subtype:
    print(f"  {r['subtype']:<40} {r['beaches']:>4}")

# ── 9. CPAD agency-level breakdown for beaches in CPAD ─────────────────────
print()
print("=" * 80)
print("9. CPAD agency-level distribution — beaches in a CPAD polygon")
print("=" * 80)
cpad_agency = run_sql("""
  with beach_to_cpad as (
    select distinct on (h.fid) h.fid, c.agncy_lev, c.mng_ag_lev
    from tmp_ca_hits h
    join public.cpad_units c on ST_DWithin(c.geom, (select geom from us_beach_points where fid = h.fid), 0.001)
    where h.in_cpad_units
    order by h.fid,
      case c.agncy_lev
        when 'Federal' then 1 when 'State' then 2 when 'County' then 3 when 'City' then 4 else 5 end
  )
  select agncy_lev, count(*) as beaches
  from beach_to_cpad
  group by agncy_lev
  order by beaches desc
""")
for r in cpad_agency:
    print(f"  {r['agncy_lev']:<25} {r['beaches']:>4}")

# ── Cleanup ────────────────────────────────────────────────────────────────
run_sql("drop table if exists tmp_ca_hits;")
