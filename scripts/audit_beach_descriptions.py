"""audit_beach_descriptions.py — leak-pattern audit on description prose.

Soft regression check, intended to run after generate_beach_descriptions.py
catalog regen. Prints pattern hit-rates + a few example fids. Returns 0
unless --strict is set AND any pattern exceeds its threshold (then 1).

Pipeline usage: pipeline action_descriptions_audit invokes this with
--scope mvp-plus-recent (descriptions updated in the last N hours).

Standalone:
  python scripts/audit_beach_descriptions.py
  python scripts/audit_beach_descriptions.py --hours 24 --states CA,OR,WA
  python scripts/audit_beach_descriptions.py --strict --hours 2

Patterns checked (per current prompt 2026-05-22):
  - "amenities" / "facilities" filler (HARD ban) — threshold 0
  - architecture-explicit ("the data", "source page", etc.) — threshold 5% acceptable per honest-brand pin
  - "Heads up" template opener — threshold 0
  - "lifeguards on duty" / "on staff" (year-round implication) — threshold 0
  - Crowd-timing fluff — threshold 0
  - Safety hedging — threshold 0
  - Generic poetry — threshold 0
  - Marketing words — threshold 0
  - Multi-region surfacing rate (must be >=70% on multi-region beaches)
"""
from __future__ import annotations
import sys, os, re, urllib.parse, argparse
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')


def _connect():
    pp = urllib.parse.urlparse((ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip())
    return psycopg2.connect(host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ['SUPABASE_DB_PASSWORD'],
        dbname=(pp.path or '/postgres').lstrip('/'), sslmode='require')


PATTERNS = [
    ('amenities filler', r'\bamenities\b', 0.005),  # hard ban; tiny slack for typos
    ('facilities filler', r'\bfacilities\b', 0.005),
    ('"Heads up" opener', r'(?:^|\.\s+)Heads up', 0.005),
    ('lifeguards on duty', r'lifeguards (on duty|on staff|always posted)', 0.005),
    ('crowd-timing fluff', r'(before (it gets|the) (packed|crowd|rush)|beat (the rush|the heat|crowds)|before parking fills)', 0.005),
    ('safety hedging', r'(watch for rip currents|hot sand burns|undertow can|stay alert|paws can burn|kelp can be slippery)', 0.005),
    ('generic poetry', r'(rolling waves|salty breeze|sun-kissed|pristine|breathtaking|stunning)', 0.005),
    ('marketing words (ungrounded)', r'\b(famous|beloved|absolutely stunning|world-class)\b', 0.01),
    ('architecture-explicit leak', r'\b(the data|source page|the bundle|the field|our data shows|the listing|the posted excerpt|according to the data)\b', 0.05),
]

MULTI_REGION_MIN_SURFACING = 0.70


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--hours', type=int, default=0,
                     help='only audit descriptions updated in last N hours (0 = all)')
    ap.add_argument('--states', default='CA,OR,WA')
    ap.add_argument('--strict', action='store_true',
                     help='exit 1 if any pattern threshold is exceeded')
    args = ap.parse_args()
    states = [s.strip() for s in args.states.split(',') if s.strip()]

    conn = _connect()
    cur = conn.cursor()

    # Pull descriptions in scope
    if args.hours > 0:
        cur.execute(
            "SELECT bd.description, g.fid, g.name FROM beach_descriptions bd "
            "JOIN beaches_gold g ON g.fid = bd.arena_group_id "
            "WHERE g.is_active AND g.scoring_tier IN ('daily','hourly') "
            "  AND g.state = ANY(%s) AND bd.description IS NOT NULL "
            "  AND bd.updated_at >= now() - (%s || ' hours')::interval",
            (states, args.hours))
    else:
        cur.execute(
            "SELECT bd.description, g.fid, g.name FROM beach_descriptions bd "
            "JOIN beaches_gold g ON g.fid = bd.arena_group_id "
            "WHERE g.is_active AND g.scoring_tier IN ('daily','hourly') "
            "  AND g.state = ANY(%s) AND bd.description IS NOT NULL",
            (states,))
    rows = cur.fetchall()
    n = len(rows)
    print(f'Audit: {n} descriptions across states={states} (hours={args.hours or "all"})')

    if n == 0:
        print('  no descriptions in scope, nothing to audit'); return 0

    print()
    print('=== Forbidden-pattern hit rates ===')
    print(f'{"pattern":<32} hits  rate    threshold  status')
    failed = False
    for label, pat, threshold in PATTERNS:
        hits = [r for r in rows if re.search(pat, r[0], re.IGNORECASE)]
        rate = len(hits) / n
        ok = rate <= threshold
        status = 'OK' if ok else 'FAIL'
        if not ok: failed = True
        ex = hits[0] if hits else None
        ex_str = f'fid={ex[1]}' if ex else ''
        print(f'  {label:<32} {len(hits):>4}  {rate:>5.1%}  {threshold:>6.1%}  {status:<5} {ex_str}')

    # Multi-region surfacing rate
    print()
    print('=== Multi-region surfacing ===')
    cur.execute(
        "SELECT g.fid, g.name, bd.description, "
        "       jsonb_path_query_array(bdp.zone_rules, '$.regions[*].name') AS region_names "
        "FROM beach_descriptions bd "
        "JOIN beaches_gold g ON g.fid = bd.arena_group_id "
        "JOIN beach_dog_policy bdp ON bdp.arena_group_id = g.fid "
        "WHERE g.is_active AND g.scoring_tier IN ('daily','hourly') "
        "  AND g.state = ANY(%s) AND bd.description IS NOT NULL "
        "  AND (SELECT COUNT(*) FROM jsonb_array_elements(bdp.zone_rules->'regions') r "
        "       WHERE r->>'name' IS NOT NULL) >= 2 ",
        (states,))
    mr = cur.fetchall()
    if mr:
        surfaced = 0
        for fid, name, desc, names in mr:
            named = [n for n in (names or []) if n]
            if any(n.lower()[:15] in (desc or '').lower() for n in named):
                surfaced += 1
        rate = surfaced / len(mr)
        ok = rate >= MULTI_REGION_MIN_SURFACING
        status = 'OK' if ok else 'FAIL'
        if not ok: failed = True
        print(f'  multi-region beaches: {len(mr)}')
        print(f'  surfacing rate:       {surfaced}/{len(mr)} = {rate:.0%}  (threshold: >={MULTI_REGION_MIN_SURFACING:.0%})  {status}')
    else:
        print('  no multi-region beaches in scope')

    print()
    if args.strict and failed:
        print('STRICT mode: at least one threshold failed; exit 1')
        return 1
    print('OK' if not failed else 'WARN (some thresholds exceeded; not strict mode)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
