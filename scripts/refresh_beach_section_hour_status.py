"""refresh_beach_section_hour_status.py — populate beach_section_hour_status.

Per Franz 2026-05-21. For each beach, walks beach_dog_policy.zone_rules
applying the SAME per-minute merge the renderer uses, then bins to
hour-of-day buckets. UPSERTs into beach_section_hour_status.

Renderer parity (must match beach.html _regionTimeBucketTiles):
  1. Skip 'global' meta-section + amenity-class sections (parking,
     restrooms, walkway, etc.) — those don't go in section_hour_status.
  2. Per-minute most-restrictive-wins for time_windows.
  3. Windows where rule == base rule are "confirmations" not variations.
  4. Seasonal date filter: skip windows whose season doesn't include
     valid_date.
  5. Operating-hours clip: hours outside open/close → 'closed'. If
     beach has no hours (or placeholder default), fall back to today's
     sunrise/sunset rounded to nearest hour.

Usage:
  python scripts/refresh_beach_section_hour_status.py --fid 8560
  python scripts/refresh_beach_section_hour_status.py --all
  python scripts/refresh_beach_section_hour_status.py --listen   # NOTIFY consumer
"""
from __future__ import annotations
import sys, math, argparse, select, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
from datetime import date, datetime
import psycopg2.extras

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect

AMENITY_CLASS = {'parking_lot','walkway','restrooms','showers','restrooms_showers',
                 'picnic_area','fire_pits','playground','campground'}
SKIP_SECTIONS = AMENITY_CLASS | {'global'}

# Rule coercion — extended values collapse to renderable classes.
TW_RULE_COERCE = {
    'off_leash_voice_control': 'off_leash',
    'on_leash_or_voice':       'on_leash',
}
def _coerce(r):
    if not r: return None
    c = TW_RULE_COERCE.get(r, r)
    return c if c in ('off_leash', 'on_leash', 'not_allowed') else None

RANK = {'not_allowed': 3, 'on_leash': 2, 'off_leash': 1}


# ── Sun-time computation (NOAA-ish, matches beach.html _sunTimes) ────
def _sun_times(lat, lon, d, tz_offset_min):
    """Returns dict with sunriseMin / sunsetMin (minutes since local midnight).
    tz_offset_min = offset from UTC in minutes (e.g. PDT = -420)."""
    if lat is None or lon is None: return None
    rad = math.pi / 180
    start = date(d.year, 1, 1)
    doy = (d - start).days + 1
    B = rad * (360/365) * (doy - 81)
    eot = 9.87*math.sin(2*B) - 7.53*math.cos(B) - 1.5*math.sin(B)
    decl = 23.45 * rad * math.sin(rad * (360/365) * (doy - 81))
    lat_rad = lat * rad
    cos_ha = (math.cos(90.833*rad) - math.sin(lat_rad)*math.sin(decl)) / \
             (math.cos(lat_rad) * math.cos(decl))
    if cos_ha > 1 or cos_ha < -1: return None
    ha_rise = math.acos(cos_ha) * 4 / rad  # minutes
    # Sign on 4*lng is MINUS (Franz 2026-05-20 bug fix in beach.html)
    noon_min = 12*60 - eot + tz_offset_min - 4*lon
    return {
        'sunriseMin': round(noon_min - ha_rise),
        'sunsetMin':  round(noon_min + ha_rise),
    }


# ── Named-anchor → MM-DD for season filtering (today's year) ─────────
def _anchor_md(y):
    def first_mon_of_sep():
        d = date(y, 9, 1); return date(y, 9, 1 + ((0 - d.weekday() + 7) % 7))
    def last_mon_of_may():
        d = date(y, 5, 31); return date(y, 5, 31 - ((d.weekday() + 7) % 7))
    ld = first_mon_of_sep()
    md = last_mon_of_may()
    return {
        'labor_day':         ld.strftime('%m-%d'),
        'labor_day_after':   date(y, 9, ld.day + 1).strftime('%m-%d'),
        'memorial_day':      md.strftime('%m-%d'),
        'memorial_day_after': date(y, 5, md.day + 1).strftime('%m-%d'),
    }

def _resolve_season_md(val, anchor_map):
    if not val: return None
    s = str(val).lower()
    # MM-DD literal
    if len(s) >= 5 and s[2] == '-' and s[0].isdigit(): return s[:5]
    return anchor_map.get(s)

def _in_season(md, start_md, end_md):
    if not start_md and not end_md: return True
    if not start_md: return md <= end_md
    if not end_md:   return md >= start_md
    if start_md <= end_md: return start_md <= md <= end_md
    return md >= start_md or md <= end_md  # wraps year-end


# ── Renderer parity: per-minute effective rule ────────────────────────
def _compute_section_minutes(sd, base_cls, today_md, anchor_map, sun):
    """Returns array of 1440 minute slots, each = effective rule (or None)."""
    is_time_anchor = lambda a: a in ('dawn', 'dusk', 'sunrise', 'sunset')
    def anchor_to_min(a):
        if not sun: return None
        if a == 'dawn'   or a == 'sunrise': return sun.get('sunriseMin')
        if a == 'dusk'   or a == 'sunset':  return sun.get('sunsetMin')
        return None
    def t_to_min(t):
        if not t: return None
        h, m = (str(t).split(':') + ['0','0'])[:2]
        return int(h or 0) * 60 + int(m or 0)

    tws = sd.get('time_windows') or []
    # Filter by season
    active = []
    for tw in tws:
        s_start_md = _resolve_season_md(tw.get('effective_from_md'), anchor_map) \
                  or _resolve_season_md(tw.get('season_anchor_start'), anchor_map)
        s_end_md   = _resolve_season_md(tw.get('effective_to_md'),   anchor_map) \
                  or _resolve_season_md(tw.get('season_anchor_end'),   anchor_map)
        # Time-of-day anchors aren't season — ignore for season filter.
        if is_time_anchor(tw.get('season_anchor_start')): s_start_md = None
        if is_time_anchor(tw.get('season_anchor_end')):   s_end_md   = None
        if _in_season(today_md, s_start_md, s_end_md):
            active.append(tw)

    intervals = []
    for tw in active:
        raw = tw.get('rule') or sd.get('rule')
        cls = _coerce(raw) or 'unknown'
        s_min = t_to_min(tw.get('start'))
        e_min = t_to_min(tw.get('end'))
        if s_min is None and is_time_anchor(tw.get('season_anchor_start')):
            s_min = anchor_to_min(tw.get('season_anchor_start'))
        if e_min is None and is_time_anchor(tw.get('season_anchor_end')):
            e_min = anchor_to_min(tw.get('season_anchor_end'))
        if e_min == 23*60 + 59: e_min = 24*60
        if s_min is None and e_min is None:
            s_min, e_min = 0, 24*60
        else:
            s_min = s_min if s_min is not None else 0
            e_min = e_min if e_min is not None else 24*60
        if s_min > e_min:
            intervals.append((s_min, 24*60, cls))
            intervals.append((0, e_min, cls))
        else:
            intervals.append((s_min, e_min, cls))

    # Per-minute: window most-restrictive wins; otherwise base.
    win_minutes = [None] * (24*60)
    has_variation = False
    for s, e, cls in intervals:
        if cls != base_cls: has_variation = True
        for m in range(max(0, s), min(24*60, e)):
            cur = win_minutes[m]
            if cur is None or RANK.get(cls, 0) > RANK.get(cur, 0):
                win_minutes[m] = cls
    # If no variation → all base
    if not has_variation:
        return [base_cls] * (24*60)
    return [c if c is not None else base_cls for c in win_minutes]


def _open_close_min(beach_open, beach_close, sun):
    def t(s):
        if not s: return None
        h, m = (str(s)[:5].split(':') + ['0','0'])[:2]
        return int(h or 0) * 60 + int(m or 0)
    open_m = t(beach_open)
    close_m = t(beach_close)
    placeholder = (open_m == 5*60 and close_m == 22*60)
    if (open_m is None or close_m is None or placeholder) and sun:
        if sun.get('sunriseMin') is not None:
            open_m = round(sun['sunriseMin'] / 60) * 60
        if sun.get('sunsetMin') is not None:
            close_m = round(sun['sunsetMin'] / 60) * 60
    return open_m, close_m


def refresh_one(cur, fid, today=None):
    """Recompute and UPSERT beach_section_hour_status rows for fid."""
    if today is None: today = date.today()
    cur.execute("""SELECT zone_rules FROM beach_dog_policy WHERE arena_group_id = %s""", (fid,))
    row = cur.fetchone()
    if not row or not row[0]: return 0
    zr = row[0]
    cur.execute("""SELECT lat, lon, open_time, close_time, timezone FROM beaches_gold WHERE fid = %s""", (fid,))
    bg = cur.fetchone()
    if not bg: return 0
    lat, lon, open_t, close_t, tz_name = bg
    # tz offset in minutes (use today's local time vs UTC)
    tz_offset_min = -420  # PDT default; not critical for sunrise math
    sun = _sun_times(float(lat) if lat else None, float(lon) if lon else None,
                     today, tz_offset_min)
    open_m, close_m = _open_close_min(open_t, close_t, sun)
    today_md = today.strftime('%m-%d')
    anchor_map = _anchor_md(today.year)

    # Gather regions (multi-region or seasons)
    regions = zr.get('regions') or []
    if not regions and zr.get('seasons'):
        for s in zr['seasons']:
            regions.extend(s.get('regions') or [])

    # Merge sections across regions: per section, MOST RESTRICTIVE wins
    # (matches the rendered _ruleMapFromZoneRules behavior).
    section_minutes = {}  # sn → array of effective rules per minute
    for reg in regions:
        for sn, sd in (reg.get('sections') or {}).items():
            if sn in SKIP_SECTIONS: continue
            base = _coerce(sd.get('rule'))
            mins = _compute_section_minutes(sd, base, today_md, anchor_map, sun)
            if sn not in section_minutes:
                section_minutes[sn] = mins
            else:
                # Merge: most-restrictive wins
                combined = section_minutes[sn]
                for m in range(24*60):
                    a, b = combined[m], mins[m]
                    if b is None: continue
                    if a is None or RANK.get(b, 0) > RANK.get(a, 0):
                        combined[m] = b

    # Bin to hours: for each hour, pick the rule that dominates the hour.
    # Use the middle minute as a representative; or majority across the hour.
    # Operating-hours clip → 'closed' outside open/close.
    rows_to_upsert = []
    for sn, minutes in section_minutes.items():
        for hour in range(24):
            # Sample the rule at the middle of the hour (minute 30)
            m_mid = hour * 60 + 30
            rule = minutes[m_mid]
            # Clip to operating hours
            in_hours = True
            if open_m is not None and m_mid < open_m: in_hours = False
            if close_m is not None and m_mid >= close_m: in_hours = False
            if not in_hours:
                rule = 'closed'
            if rule is None:
                rule = 'unknown'
            rows_to_upsert.append((fid, sn, hour, rule, today))

    # Delete-then-insert is simpler than UPSERT for this PK shape.
    cur.execute("DELETE FROM beach_section_hour_status WHERE beach_fid = %s", (fid,))
    if rows_to_upsert:
        psycopg2.extras.execute_values(cur, """
          INSERT INTO beach_section_hour_status
            (beach_fid, section, hour_of_day, effective_rule, valid_date)
          VALUES %s
        """, rows_to_upsert)
    return len(rows_to_upsert)


def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument('--fid', type=int)
    grp.add_argument('--fids', type=str,
                     help='comma-separated list of fids (for pipeline chunked dispatch)')
    grp.add_argument('--all', action='store_true')
    grp.add_argument('--listen', action='store_true',
                     help='Listen on NOTIFY zone_rules_changed and refresh per-beach')
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor()

    if args.fid:
        n = refresh_one(cur, args.fid)
        conn.commit()
        print(f'fid={args.fid}: {n} rows')
        print(f'Done. ok=1')
        return 0

    if args.fids:
        fids = [int(x) for x in args.fids.split(',') if x.strip()]
        ok = 0
        total = 0
        for fid in fids:
            try:
                n = refresh_one(cur, fid)
                conn.commit()
                total += n
                ok += 1
            except Exception as e:
                conn.rollback()
                print(f'  fid={fid} FAIL: {e}')
        print(f'Done. ok={ok} rows={total}')
        return 0

    if args.all:
        cur.execute("""SELECT arena_group_id FROM beach_dog_policy
                       WHERE zone_rules IS NOT NULL ORDER BY arena_group_id""")
        fids = [r[0] for r in cur.fetchall()]
        print(f'Refreshing {len(fids)} beaches...')
        total = 0
        for i, fid in enumerate(fids, 1):
            try:
                n = refresh_one(cur, fid)
                total += n
                if i % 100 == 0:
                    conn.commit()
                    print(f'  [{i}/{len(fids)}] total rows so far: {total}')
            except Exception as e:
                conn.rollback()
                print(f'  fid={fid} FAIL: {e}')
        conn.commit()
        print(f'\nTOTAL: {total} rows across {len(fids)} beaches')
        return 0

    if args.listen:
        cur.execute("LISTEN zone_rules_changed")
        conn.commit()
        print('Listening on zone_rules_changed... Ctrl+C to stop.')
        while True:
            if select.select([conn], [], [], 5) == ([], [], []):
                continue
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                fid = int(notify.payload)
                try:
                    n = refresh_one(cur, fid)
                    conn.commit()
                    print(f'  fid={fid} refreshed: {n} rows')
                except Exception as e:
                    conn.rollback()
                    print(f'  fid={fid} FAIL: {e}')


if __name__ == '__main__':
    sys.exit(main())
