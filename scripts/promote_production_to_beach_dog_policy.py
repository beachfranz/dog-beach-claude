"""
promote_production_to_beach_dog_policy.py — bulk-promote production-table
extractions into beach_dog_policy (the consumer cascade source).

For each scoreable beach in beaches_gold WITHOUT an existing beach_dog_policy
row: gather available signals from park_url_extractions, cpad_unit_dogs_policy,
operator_dogs_policy, policy_research_extractions. Compute consensus per field.
Write a beach_dog_policy row tagged source='auto_promoted_from_production'
with a note listing the sources that voted.

Confidence:
  - 2+ sources agree → HIGH
  - 1 source from park_url or cpad_unit → MEDIUM
  - 1 source from research_v1 only → LOW
  - sources conflict → field stays NULL (logged for manual review)

Usage:
  python scripts/promote_production_to_beach_dog_policy.py            # dry-run
  python scripts/promote_production_to_beach_dog_policy.py --apply
  python scripts/promote_production_to_beach_dog_policy.py --apply --sample 25
  python scripts/promote_production_to_beach_dog_policy.py --apply --counties "San Diego,Los Angeles"
"""
from __future__ import annotations
import argparse
import os
import sys
import urllib.parse
from collections import Counter, defaultdict
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


# Mappers from production-table column values to canonical enum
def map_dogs_allowed(v):
    if v is None: return None
    s = str(v).strip().lower()
    if s in ('yes', 'restricted', 'seasonal', 'mixed'): return 'yes'
    if s in ('no',): return 'no'
    return None


def map_leash(v):
    if v is None: return None
    if isinstance(v, bool):
        return 'on_leash' if v else 'off_leash'
    s = str(v).strip().lower()
    if 'off-leash' in s or 'off leash' in s or 'voice control' in s: return 'off_leash'
    if 'required' in s or 'leashed' in s or 'on-leash' in s or 'on leash' in s: return 'on_leash'
    if s in ('true',): return 'on_leash'
    if s in ('false',): return 'off_leash'
    return None


def map_yn(v):
    if v is None: return None
    if v is True: return 'yes'
    if v is False: return 'none'
    return None


def map_lifeguard(v):
    if v is None: return None
    if v is True: return 'full_time'
    if v is False: return 'none'
    return None


def map_parking_lot(v):
    """Production tables had a coarse parking_type. Mapping to free/paid is hacky."""
    if v is None: return None
    s = str(v).strip().lower()
    # We don't have free/paid signal — old enum was just lot/street/both/none
    if s in ('lot', 'both'): return None  # can't distinguish free/paid; skip
    if s == 'none': return 'none'
    return None


def gather_signals(cur, fid: int, cpad_unit_id: int | None) -> dict:
    """Return per-field per-source values for one beach.
    Shape: {field_name: [(source_label, value), ...]}"""
    signals = defaultdict(list)

    # park_url_extractions
    cur.execute("""
        select dogs_allowed, dogs_leash_required, has_restrooms, has_showers,
               has_lifeguards, parking_type
          from public.park_url_extractions
         where arena_group_id = %s
    """, (fid,))
    for r in cur.fetchall():
        if (v := map_dogs_allowed(r['dogs_allowed'])) is not None:
            signals['dogs_allowed'].append(('park_url', v))
        if (v := map_leash(r['dogs_leash_required'])) is not None:
            signals['leash_policy'].append(('park_url', v))
        if (v := map_yn(r['has_restrooms'])) is not None:
            signals['restrooms'].append(('park_url', v))
        if (v := map_yn(r['has_showers'])) is not None:
            signals['outdoor_showers'].append(('park_url', v))
        if (v := map_lifeguard(r['has_lifeguards'])) is not None:
            signals['lifeguard'].append(('park_url', v))

    # cpad_unit_dogs_policy
    if cpad_unit_id:
        cur.execute("""
            select dogs_allowed, default_rule, leash_required
              from public.cpad_unit_dogs_policy
             where cpad_unit_id = %s
        """, (cpad_unit_id,))
        for r in cur.fetchall():
            v = map_dogs_allowed(r['dogs_allowed'] or r['default_rule'])
            if v is not None:
                signals['dogs_allowed'].append(('cpad_unit', v))
            if r['leash_required'] is True:
                signals['leash_policy'].append(('cpad_unit', 'on_leash'))
            elif r['leash_required'] is False:
                signals['leash_policy'].append(('cpad_unit', 'off_leash'))

    # operator_dogs_policy (joined via cpad_units.mng_agncy → operators.cpad_agncy_name)
    if cpad_unit_id:
        cur.execute("""
            select op.default_rule, op.leash_required
              from public.cpad_units cu
              join public.operators o on o.cpad_agncy_name = cu.mng_agncy
              join public.operator_dogs_policy op on op.operator_id = o.id
             where cu.unit_id = %s
        """, (cpad_unit_id,))
        for r in cur.fetchall():
            v = map_dogs_allowed(r['default_rule'])
            if v is not None:
                signals['dogs_allowed'].append(('operator', v))
            if r['leash_required'] is True:
                signals['leash_policy'].append(('operator', 'on_leash'))
            elif r['leash_required'] is False:
                signals['leash_policy'].append(('operator', 'off_leash'))

    # policy_research_extractions
    cur.execute("""
        select dogs_allowed, dogs_leash_required, has_restrooms, has_showers,
               has_lifeguards
          from public.policy_research_extractions
         where arena_group_id = %s
    """, (fid,))
    for r in cur.fetchall():
        if (v := map_dogs_allowed(r['dogs_allowed'])) is not None:
            signals['dogs_allowed'].append(('research', v))
        if (v := map_leash(r['dogs_leash_required'])) is not None:
            signals['leash_policy'].append(('research', v))
        if (v := map_yn(r['has_restrooms'])) is not None:
            signals['restrooms'].append(('research', v))
        if (v := map_yn(r['has_showers'])) is not None:
            signals['outdoor_showers'].append(('research', v))
        if (v := map_lifeguard(r['has_lifeguards'])) is not None:
            signals['lifeguard'].append(('research', v))

    return signals


HIGH_TRUST = {'park_url', 'cpad_unit', 'operator'}


def consensus(values: list[tuple[str, str]]) -> tuple[str | None, str | None, str]:
    """Return (winning_value, confidence, sources_str). None if conflict."""
    if not values:
        return (None, None, "")
    vote = Counter(v for _, v in values)
    sources = sorted({s for s, _ in values})
    sources_str = "+".join(sources)
    if len(vote) == 1:
        # Single value across all sources
        v, n = vote.most_common(1)[0]
        if n >= 2: return (v, 'high', sources_str)
        if any(s in HIGH_TRUST for s in sources): return (v, 'medium', sources_str)
        # Single low-trust source (research-only) — skip.
        # Tightened 2026-05-02 after curator review of 25-beach sample.
        return (None, None, sources_str + "(research-only,skipped)")
    # Conflict
    top, second = vote.most_common(2)
    if top[1] > second[1]:
        # Clear majority
        return (top[0], 'medium', sources_str + "(majority)")
    return (None, None, sources_str + "(conflict)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Write rows to beach_dog_policy")
    ap.add_argument("--sample", type=int, default=None,
                    help="Limit to N beaches across diverse counties (default: all)")
    ap.add_argument("--counties", default=None,
                    help="Comma-separated county filter (e.g. 'San Diego,Los Angeles')")
    args = ap.parse_args()

    conn = psycopg2.connect(**PG)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Pick scoreable beaches WITHOUT beach_dog_policy
    where_county = ""
    params = []
    if args.counties:
        cnames = [c.strip() for c in args.counties.split(',')]
        where_county = "and g.county_name = any(%s)"
        params.append(cnames)

    # Diverse sample: top N by county (rotate counties)
    if args.sample:
        cur.execute(f"""
            with cands as (
              select g.fid, g.name, g.county_name, g.cpad_unit_id,
                     row_number() over (partition by g.county_name order by g.fid) as rn
                from public.beaches_gold g
               where g.is_active and g.is_scoreable
                 and not exists (select 1 from public.beach_dog_policy d
                                  where d.arena_group_id = g.fid)
                 {where_county}
            )
            select fid, name, county_name, cpad_unit_id
              from cands
             where rn <= 5
             order by county_name, rn
             limit %s
        """, params + [args.sample])
    else:
        cur.execute(f"""
            select g.fid, g.name, g.county_name, g.cpad_unit_id
              from public.beaches_gold g
             where g.is_active and g.is_scoreable
               and not exists (select 1 from public.beach_dog_policy d
                                where d.arena_group_id = g.fid)
               {where_county}
             order by g.county_name, g.fid
        """, params)
    beaches = cur.fetchall()
    print(f"Candidate beaches (scoreable, no beach_dog_policy yet): {len(beaches)}")

    by_county = Counter(b['county_name'] for b in beaches)
    print(f"By county: {dict(by_county)}\n")

    promotions = []
    conflicts = []
    no_signal = []
    for b in beaches:
        signals = gather_signals(cur, b['fid'], b['cpad_unit_id'])
        if not signals:
            no_signal.append(b)
            continue
        row = {'fid': b['fid'], 'name': b['name'], 'county': b['county_name']}
        any_value = False
        for fname in ('dogs_allowed', 'leash_policy', 'restrooms',
                      'outdoor_showers', 'lifeguard'):
            v, conf, src = consensus(signals.get(fname, []))
            row[fname] = (v, conf, src)
            if v is not None: any_value = True
            elif src.endswith("(conflict)"):
                conflicts.append((b, fname, signals[fname]))
        if any_value:
            promotions.append(row)
        else:
            no_signal.append(b)

    print(f"=== plan ===")
    print(f"  beaches with at least one consensus value: {len(promotions)}")
    print(f"  beaches with no usable signal:             {len(no_signal)}")
    print(f"  per-cell conflicts (skipped fields):       {len(conflicts)}")
    print()
    print(f"{'fid':>5}  {'beach':<30}  {'county':<14}  dogs  leash  restrm  shower  lifegrd  sources")
    print('-'*120)
    for r in promotions[:50]:
        cols = []
        for f in ('dogs_allowed', 'leash_policy', 'restrooms', 'outdoor_showers', 'lifeguard'):
            v, c, s = r[f]
            cols.append(f"{(v or '-'):<6}")
        srcs = []
        for f in ('dogs_allowed', 'leash_policy', 'restrooms', 'outdoor_showers', 'lifeguard'):
            _, _, s = r[f]
            if s and s not in srcs: srcs.append(s)
        print(f"{r['fid']:>5}  {r['name'][:30]:<30}  {r['county'][:14]:<14}  " +
              "  ".join(cols) + f"  {','.join(srcs)[:60]}")
    if len(promotions) > 50:
        print(f"  ... and {len(promotions)-50} more")

    if not args.apply:
        print("\n(dry-run; rerun with --apply to write rows)")
        return 0

    # Write
    n_inserted = 0
    for r in promotions:
        cells = {f: r[f] for f in ('dogs_allowed', 'leash_policy', 'restrooms',
                                   'outdoor_showers', 'lifeguard')}
        # Build notes citing sources per field with values
        note_lines = []
        for f, (v, c, s) in cells.items():
            if v is not None:
                note_lines.append(f"{f}={v} (conf {c}, sources: {s})")
        notes = " | ".join(note_lines)

        cur.execute("""
            insert into public.beach_dog_policy
              (arena_group_id, dogs_allowed, leash_policy, source, notes)
            values (%s, %s, %s, 'auto_promoted_from_production', %s)
            on conflict (arena_group_id) do nothing
        """, (
            r['fid'],
            cells['dogs_allowed'][0],
            cells['leash_policy'][0],
            notes,
        ))
        if cur.rowcount: n_inserted += 1
    conn.commit()
    print(f"\nINSERTed {n_inserted} new beach_dog_policy rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
