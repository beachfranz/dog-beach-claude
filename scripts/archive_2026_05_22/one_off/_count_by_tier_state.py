"""Ad hoc: count beaches by tier by state. Drops itself after one use."""
from __future__ import annotations
import os, urllib.parse
from pathlib import Path
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')

SQL = """
select g.state,
       public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text) tier,
       count(*) filter (where g.is_active) active,
       count(*) filter (where g.is_active and g.is_scoreable) scoreable,
       count(*) total
  from public.beaches_gold g
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
 group by 1, 2
 order by g.state, tier
"""

with psycopg2.connect(**PG) as c:
    with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL)
        rows = cur.fetchall()

# Pivot: state -> {tier: count}
from collections import defaultdict
by_state = defaultdict(dict)
totals_by_state = defaultdict(lambda: {'active': 0, 'scoreable': 0, 'total': 0})
for r in rows:
    by_state[r['state']][r['tier']] = r
    totals_by_state[r['state']]['active'] += r['active']
    totals_by_state[r['state']]['scoreable'] += r['scoreable']
    totals_by_state[r['state']]['total'] += r['total']

TIERS = ['1_off-leash', '2_on-leash', '3_limited_access', '4_no_dogs', 'unknown']
states = sorted(by_state)

# Tiers as rows, states as columns
header = f"{'tier':<22}" + ''.join(f"{s:>8}" for s in states)
print('\n' + header)
print('-' * len(header))
for t in TIERS:
    cells = ''.join(f"{(by_state[s].get(t) or {}).get('active', 0):>8}" for s in states)
    print(f"{t:<22}{cells}")
print('-' * len(header))
print(f"{'active':<22}" + ''.join(f"{totals_by_state[s]['active']:>8}" for s in states))
print(f"{'scoreable':<22}" + ''.join(f"{totals_by_state[s]['scoreable']:>8}" for s in states))
print(f"{'total (incl inactive)':<22}" + ''.join(f"{totals_by_state[s]['total']:>8}" for s in states))
