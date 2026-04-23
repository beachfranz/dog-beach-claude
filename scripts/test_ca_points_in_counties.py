"""Quick test: do the 955 CA beach points all resolve to a county via
strict ST_Contains against the new counties table? If some miss (coastal
offshore), try a small buffer."""

import csv, json, re, subprocess
from pathlib import Path

CSV = Path(r"C:\Users\beach\Documents\dog-beach-claude\share\Dog_Beaches\US_beaches_with_state.csv")
TMP = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\q.sql")
ROOT = Path(r"C:\Users\beach\Documents\dog-beach-claude")


def load_ca():
    pts = []
    with open(CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("STATE") != "CA": continue
            m = re.match(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", r.get("WKT",""), re.I)
            if m:
                pts.append((int(r["fid"]), float(m.group(2)), float(m.group(1))))
    return pts


def run_sql(sql):
    TMP.parent.mkdir(parents=True, exist_ok=True)
    TMP.write_text(sql, encoding="utf-8")
    r = subprocess.run(["supabase", "db", "query", "--linked", "-f", str(TMP)],
                       capture_output=True, text=True, timeout=300, cwd=str(ROOT))
    if r.returncode != 0: raise RuntimeError(r.stderr)
    s, e = r.stdout.find("{"), r.stdout.rfind("}")
    return json.loads(r.stdout[s:e+1])["rows"]


pts = load_ca()
values = "values " + ",\n".join(f"({f},{la},{lo})" for f,la,lo in pts)

print(f"{len(pts)} CA points")

for label, predicate in (
    ("strict ST_Contains",                 "ST_Contains(c.geom, ST_SetSRID(ST_MakePoint(p.lon,p.lat), 4326))"),
    ("100m buffered ST_DWithin (0.001°)",  "ST_DWithin(c.geom, ST_SetSRID(ST_MakePoint(p.lon,p.lat), 4326), 0.001)"),
):
    q = f"""
      with points(fid, lat, lon) as ({values})
      select count(distinct p.fid) as hits from points p
      join public.counties c on {predicate}
    """
    for r in run_sql(q):
        print(f"  {label:<40s} {r['hits']}/{len(pts)}")

# By county, with buffer
q = f"""
  with points(fid, lat, lon) as ({values})
  select c.name_full, count(distinct p.fid) as n
  from points p
  join public.counties c on ST_DWithin(c.geom, ST_SetSRID(ST_MakePoint(p.lon,p.lat), 4326), 0.001)
  group by c.name_full order by n desc limit 20
"""
print("\nTop counties by beach count (100m buffer):")
for r in run_sql(q):
    print(f"  {r['name_full']:<30s} {r['n']:>3d}")
