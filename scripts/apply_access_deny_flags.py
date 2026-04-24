"""
apply_access_deny_flags.py
--------------------------
Applies the four public-access deny rules (finalized 2026-04-24) as
validation flags on public.us_beach_points via flag_beach_point().

Rule set (CA beach points, with "Open Access" CPAD overriding ownership):
  1. access_military_base   — inside military_bases AND NOT in Open Access CPAD
  2. access_cpad_denied     — in CPAD with access_typ = 'No Public Access'
  3. access_ccc_not_public  — nearest CCC access point has open_to_public = 'No'
  4. access_hoa_private     — in CPAD agncy_lev = 'Home Owners Association'
                              AND NOT in Open Access CPAD

Expected counts (pre-flag): 19 / 11 / 3 / 5 — 38 distinct beaches total.

Idempotent: flag_beach_point replaces any prior flag with the same check name.
Sets validation_status = 'invalid' on each flagged row.
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
    if s < 0:
        return []
    return json.loads(r.stdout[s:e+1]).get("rows", [])


# ── Precompute: CA beaches that sit in at least one Open Access CPAD polygon ──
# This set is the escape clause for rules 1 and 4. Materialized once so each
# rule query can subtract against it cheaply.
SETUP = """
drop table if exists tmp_open_access_fids;
create table tmp_open_access_fids as
select distinct b.fid
from public.us_beach_points b
join public.cpad_units c on ST_DWithin(c.geom, b.geom, 0.001)
where b.state = 'CA' and c.access_typ = 'Open Access';
create index tmp_open_access_fids_idx on tmp_open_access_fids(fid);
"""
run_sql(SETUP)

# ── Rule definitions ───────────────────────────────────────────────────────
RULES = {
    "access_military_base": {
        "expected": "public_access",
        "query": """
            select b.fid,
                   b.name,
                   string_agg(distinct coalesce(m.site_name, 'unknown') ||
                              case when m.component is not null
                                   then ' (' || m.component || ')' else '' end,
                              '; ') as detail
            from public.us_beach_points b
            join public.military_bases m on ST_DWithin(m.geom, b.geom, 0.001)
            where b.state = 'CA'
              and not exists (select 1 from tmp_open_access_fids o where o.fid = b.fid)
            group by b.fid, b.name
            order by b.fid
        """,
        "detail_prefix": "inside military installation: ",
    },
    "access_cpad_denied": {
        "expected": "public_access",
        "query": """
            select b.fid,
                   b.name,
                   string_agg(distinct c.unit_name, '; ') as detail
            from public.us_beach_points b
            join public.cpad_units c on ST_DWithin(c.geom, b.geom, 0.001)
            where b.state = 'CA'
              and c.access_typ = 'No Public Access'
            group by b.fid, b.name
            order by b.fid
        """,
        "detail_prefix": "CPAD access_typ='No Public Access': ",
    },
    "access_ccc_not_public": {
        "expected": "public_access",
        "query": """
            -- Nearest CCC point within 200m (project_buffer_convention) with open_to_public='No'
            select b.fid, b.name, nearest.name as detail
            from public.us_beach_points b
            cross join lateral (
              select c.name, c.open_to_public,
                     ST_Distance(c.geom::geography, b.geom::geography) as m
              from public.ccc_access_points c
              where ST_DWithin(c.geom, b.geom, 0.002)
              order by c.geom <-> b.geom
              limit 1
            ) nearest
            where b.state = 'CA'
              and nearest.open_to_public = 'No'
            order by b.fid
        """,
        "detail_prefix": "CCC nearest access point open_to_public='No': ",
    },
    "access_hoa_private": {
        "expected": "public_access",
        "query": """
            select b.fid,
                   b.name,
                   string_agg(distinct c.unit_name, '; ') as detail
            from public.us_beach_points b
            join public.cpad_units c on ST_DWithin(c.geom, b.geom, 0.001)
            where b.state = 'CA'
              and c.agncy_lev = 'Home Owners Association'
              and not exists (select 1 from tmp_open_access_fids o where o.fid = b.fid)
            group by b.fid, b.name
            order by b.fid
        """,
        "detail_prefix": "HOA-managed CPAD polygon: ",
    },
}

PROCESS = "apply_access_deny_flags.py"

all_flagged_fids = set()

for check, cfg in RULES.items():
    print("=" * 78)
    print(f"Rule: {check}")
    print("=" * 78)
    rows = run_sql(cfg["query"])
    print(f"  {len(rows)} beaches matched")
    for r in rows:
        print(f"    [{r['fid']:>5}] {r['name']:<40}  -> {r['detail']}")
        all_flagged_fids.add(r["fid"])

    if not rows:
        continue

    # Apply flags in one statement per rule via values() table
    values_sql = ",\n".join(
        "("
        f"{r['fid']}, "
        f"$flag${check}$flag$, "
        f"$exp${cfg['expected']}$exp$, "
        f"$det${cfg['detail_prefix']}{(r['detail'] or '').replace(chr(36), '')}$det$, "
        f"$p${PROCESS}::{check}$p$"
        ")"
        for r in rows
    )
    apply_sql = f"""
        with v(fid, chk, expected, details, process) as (values
          {values_sql}
        )
        select public.flag_beach_point(fid, chk, expected, details, process)
        from v;
    """
    run_sql(apply_sql)
    print(f"  applied {len(rows)} flag(s)")
    print()

# ── Cleanup + post-flag summary ────────────────────────────────────────────
run_sql("drop table if exists tmp_open_access_fids;")

print("=" * 78)
print("Post-flag summary")
print("=" * 78)
print(f"  Total distinct flagged beaches: {len(all_flagged_fids)}")

summary = run_sql("""
  select validation_status, count(*) as n
  from public.us_beach_points
  where state = 'CA'
  group by validation_status
  order by validation_status
""")
for r in summary:
    print(f"  validation_status={r['validation_status']:<10} {r['n']}")

per_check = run_sql("""
  select f->>'check' as check_name, count(*) as n
  from public.us_beach_points,
       jsonb_array_elements(validation_flags) f
  where state = 'CA' and (f->>'check') like 'access_%'
  group by f->>'check'
  order by check_name
""")
print()
print("Per-check fanout (some beaches fire multiple rules):")
for r in per_check:
    print(f"  {r['check_name']:<28} {r['n']}")
