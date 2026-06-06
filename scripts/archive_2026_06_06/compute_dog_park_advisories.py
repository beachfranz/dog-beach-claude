"""Compute deterministic dog-park advisories from dog_park_day_hourly_scores.

Parallel of scripts/compute_weather_advisories.py — same shape, same
v2-band-driven derivation, writes to dog_park_advisory instead of
beach_advisory.

Reads:
  dog_park_day_hourly_scores (today + tomorrow, raw values only)
  dog_parks_gold (scope)

Writes:
  dog_park_advisory rows whose v2_signal_status >= 'advisory' on
  ≥1 future hour, for the metric set that applies to parks (no tide).

Source name: source='deterministic_weather' for hourly metric advisories.

Initial use: car_heat / car_cold (ASPCA-aligned) — driven by temp_air.
Also surfaces uv_neg, asphalt_neg, feels_like_hot/cold where bands exist
for entity_type='dog_park' in scoring_config_v2.

advisory_key includes the local date so re-runs same-day refresh the
same row; next day = new row.

Run:
  python scripts/compute_dog_park_advisories.py            # pilot 30 CA
  python scripts/compute_dog_park_advisories.py --all-scored
  python scripts/compute_dog_park_advisories.py --fid 518
  python scripts/compute_dog_park_advisories.py --dry-run
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import json
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect


V2_TO_SEVERITY = {"no_go": "severe", "caution": "moderate", "advisory": "minor"}
V2_RANK = {"clear": 0, "advisory": 1, "caution": 2, "no_go": 3}

# Subset of the beach HOURLY_METRICS that applies to dog parks. No tide /
# sand / crowd; parks don't have those signals (or have them under
# different shapes). Car-safety is the headline new feature.
HOURLY_METRICS = [
    ("asphalt_status",  "asphalt_neg",     "asphalt_temp",  "Hot asphalt", "🐾", "paws_warning",
     "Parking-lot asphalt {observed}°F — booties for the walk in.", "{:.0f}°F", "max"),
    ("uv_status",       "uv_neg",          "uv_index",      "High UV",     "☀️", "review_required",
     "UV peaks at {observed} — sunscreen for you, shade breaks for the pup.", "{:.0f}", "max"),
    ("wind_status",     "wind_harsh_neg",  "wind_speed",    "Strong wind", "💨", "blowing_sand",
     "Wind gusts {observed}mph — secure leashes when leaving; dusty conditions likely.", "{:.0f}mph", "max"),
    ("rain_status",     "precip_chance",   "precip_chance", "Rain",        "🌧️", "review_required",
     "Rain likely ({observed}% chance) — bring a towel.", "{:.0f}%", "max"),
    ("temp_hot_status", "feels_like_hot",  "feels_like",    "Heat",        "🌡️", "paws_warning",
     "Hot (feels like {observed}°F) — heat stress risk; short visits + plenty of water.", "{:.0f}°F", "max"),
    ("temp_cold_status","feels_like_cold", "feels_like",    "Cold",        "❄️", "cold_paws",
     "Cold (feels like {observed}°F) — short coats may need a jacket.", "{:.0f}°F", "min"),
    ("car_heat_status", "car_heat_neg",    "temp_air",      "Hot car",     "🚗", "skip_car",
     "{observed}°F outside — don't leave your dog in a parked car. Interior climbs to 100°F+ within 10 min, fatal heatstroke risk in 15.", "{:.0f}°F", "max"),
    ("car_cold_status", "car_cold_neg",    "temp_air",      "Cold car",    "🥶", "skip_car",
     "{observed}°F outside — don't leave your dog in a parked car. Interior cools to ambient quickly; hypothermia risk for short coats.", "{:.0f}°F", "min"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--pilot", action="store_true", help="(default) pilot 30 CA")
    grp.add_argument("--all-mvp", action="store_true", help="legacy CA+OR+WA")
    grp.add_argument("--all-scored", action="store_true",
                     help="every active dog park with scoring_tier IN ('daily','hourly')")
    grp.add_argument("--state")
    grp.add_argument("--fid", type=int, help="single park fid")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = connect()
    try:
        with conn.cursor() as cur:
            if args.fid:
                cur.execute("""
                    SELECT fid FROM public.dog_parks_gold
                     WHERE fid=%s AND is_active
                """, (args.fid,))
            elif args.state:
                cur.execute("""
                    SELECT fid FROM public.dog_parks_gold
                     WHERE state=%s AND is_active AND is_scoreable = true
                """, (args.state.upper(),))
            elif args.all_mvp:
                cur.execute("""
                    SELECT fid FROM public.dog_parks_gold
                     WHERE state IN ('CA','OR','WA') AND is_active AND is_scoreable = true
                """)
            elif args.all_scored:
                cur.execute("""
                    SELECT fid FROM public.dog_parks_gold
                     WHERE is_active AND is_scoreable = true
                """)
            else:
                cur.execute("""
                    SELECT fid FROM public.dog_parks_gold
                     WHERE state='CA' AND is_active AND is_scoreable = true
                     ORDER BY fid LIMIT 30
                """)
            scope = [r[0] for r in cur.fetchall()]
        print(f"Scope: {len(scope)} dog parks", flush=True)

        n_advisories = 0
        n_retired = 0
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        upd = conn.cursor()
        for i, fid in enumerate(scope, 1):
            cur.execute("""
                SELECT local_date, local_hour, forecast_ts,
                       asphalt_temp, uv_index, feels_like, temp_air, wind_speed, precip_chance,
                       public.v2_signal_status('dog_park','asphalt_neg',     asphalt_temp::numeric)  AS asphalt_v2,
                       public.v2_signal_status('dog_park','uv_neg',          uv_index::numeric)      AS uv_v2,
                       public.v2_signal_status('dog_park','wind_harsh_neg',  wind_speed::numeric)    AS wind_v2,
                       public.v2_signal_status('dog_park','precip_chance',   precip_chance::numeric) AS precip_v2,
                       public.v2_signal_status('dog_park','feels_like_hot',  feels_like::numeric)    AS feels_hot_v2,
                       public.v2_signal_status('dog_park','feels_like_cold', feels_like::numeric)    AS feels_cold_v2,
                       public.v2_signal_status('dog_park','car_heat_neg',    temp_air::numeric)      AS car_heat_v2,
                       public.v2_signal_status('dog_park','car_cold_neg',    temp_air::numeric)      AS car_cold_v2
                  FROM public.dog_park_day_hourly_scores
                 WHERE dog_park_fid = %s
                   AND local_date >= (now() at time zone 'UTC')::date
                   AND local_date <= ((now() at time zone 'UTC')::date + interval '1 day')
                 ORDER BY local_date, local_hour
            """, (fid,))
            hours = [dict(r) for r in cur.fetchall()]
            if not hours:
                continue

            by_date: dict[str, list[dict]] = {}
            for h in hours:
                by_date.setdefault(str(h["local_date"]), []).append(h)

            v2_alias = {
                "asphalt_neg":     "asphalt_v2",
                "uv_neg":          "uv_v2",
                "wind_harsh_neg":  "wind_v2",
                "precip_chance":   "precip_v2",
                "feels_like_hot":  "feels_hot_v2",
                "feels_like_cold": "feels_cold_v2",
                "car_heat_neg":    "car_heat_v2",
                "car_cold_neg":    "car_cold_v2",
            }

            now_utc = datetime.now(timezone.utc)
            for date_iso, day_hours in by_date.items():
                for (event_type, signal_key, raw_col, label, icon, klass, text_tmpl, unit_fmt, agg) in HOURLY_METRICS:
                    alias = v2_alias[signal_key]
                    triggered_all = [h for h in day_hours
                                     if V2_RANK.get(h.get(alias) or "clear", 0) >= 1]
                    if not triggered_all:
                        continue
                    worst = max((h[alias] for h in triggered_all),
                                key=lambda s: V2_RANK.get(s, 0))
                    advisory_key = f"det:{event_type}_{worst}:{date_iso}"

                    triggered = [h for h in triggered_all
                                 if h.get("forecast_ts") and h["forecast_ts"] >= now_utc]
                    if not triggered:
                        if not args.dry_run:
                            upd.execute("""
                                DELETE FROM public.dog_park_advisory
                                 WHERE dog_park_fid = %s AND advisory_key = %s
                            """, (fid, advisory_key))
                            n_retired += 1
                        continue

                    severity = V2_TO_SEVERITY[worst]
                    vals = [h[raw_col] for h in triggered if h[raw_col] is not None]
                    if not vals:
                        continue
                    extreme = (min if agg == "min" else max)(vals)
                    text = text_tmpl.format(observed=extreme)
                    value_str = unit_fmt.format(extreme)
                    first = triggered[0]; last = triggered[-1]
                    if args.dry_run:
                        print(f"  fid={fid:<10} {date_iso}  {event_type:<18} {worst:<8} val={extreme}  → {text[:80]}", flush=True)
                        continue
                    upd.execute("""
                        INSERT INTO public.dog_park_advisory (
                          dog_park_fid, advisory_key, source, event_type, severity,
                          valid_from, valid_to, dog_impact_class, dog_impact_text,
                          translation_source, label, value, icon, raw_data, fetched_at
                        ) VALUES (%s, %s, 'deterministic_weather', %s, %s,
                                  %s, %s, %s, %s, 'rule', %s, %s, %s, %s, now())
                        ON CONFLICT (dog_park_fid, advisory_key) DO UPDATE SET
                          severity         = EXCLUDED.severity,
                          valid_from       = EXCLUDED.valid_from,
                          valid_to         = EXCLUDED.valid_to,
                          dog_impact_text  = EXCLUDED.dog_impact_text,
                          value            = EXCLUDED.value,
                          raw_data         = EXCLUDED.raw_data,
                          fetched_at       = now();
                    """, (
                        fid, advisory_key, event_type, severity,
                        first["forecast_ts"], last["forecast_ts"],
                        klass, text, label, value_str, icon,
                        json.dumps({"hours_triggered": len(triggered),
                                    "extreme_value": float(extreme),
                                    "worst_v2_status": worst,
                                    "signal_key": signal_key,
                                    "scoring_version": "v2"}),
                    ))
                    n_advisories += 1

            # Retire pre-v2 rows
            if not args.dry_run:
                upd.execute("""
                    DELETE FROM public.dog_park_advisory
                     WHERE dog_park_fid = %s
                       AND source = 'deterministic_weather'
                       AND raw_data->>'scoring_version' IS DISTINCT FROM 'v2'
                       AND fetched_at < now() - interval '1 minute'
                """, (fid,))
                if upd.rowcount:
                    n_retired += upd.rowcount

            # Structural advisory: unfenced parks. Surfaced in the same
            # dog_park_advisory table so the consumer renderer is uniform.
            # Refreshed on each run; cleared when has_fence flips true.
            upd.execute("""
                SELECT COALESCE(p.has_fence, g.has_fence) AS fence
                  FROM public.dog_parks_gold g
                  LEFT JOIN public.dog_park_dog_policy p ON p.dog_park_fid = g.fid
                 WHERE g.fid = %s
            """, (fid,))
            fence_row = upd.fetchone()
            fence_val = fence_row[0] if fence_row else None
            if fence_val is False and not args.dry_run:
                upd.execute("""
                    INSERT INTO public.dog_park_advisory (
                      dog_park_fid, advisory_key, source, event_type, severity,
                      valid_from, valid_to, dog_impact_class, dog_impact_text,
                      translation_source, label, value, icon, raw_data, fetched_at
                    ) VALUES (%s, 'static:unfenced', 'static_attribute',
                              'unfenced_status', 'moderate',
                              now(), now() + interval '1 year',
                              'review_required',
                              'This park has no perimeter fence. Recall-trained dogs only; consider a long line.',
                              'rule', 'Unfenced', NULL, '⚠️', %s, now())
                    ON CONFLICT (dog_park_fid, advisory_key) DO UPDATE SET
                      valid_from = EXCLUDED.valid_from,
                      valid_to   = EXCLUDED.valid_to,
                      fetched_at = now();
                """, (fid, json.dumps({"static": True, "field": "has_fence", "value": False})))
                n_advisories += 1
            elif fence_val is not False and not args.dry_run:
                # Fence is true or null — retire any prior unfenced advisory
                upd.execute("""
                    DELETE FROM public.dog_park_advisory
                     WHERE dog_park_fid = %s AND advisory_key = 'static:unfenced'
                """, (fid,))
                if upd.rowcount:
                    n_retired += upd.rowcount

            if i % 10 == 0 and not args.dry_run:
                conn.commit()
        if not args.dry_run:
            conn.commit()

        print(f"\nDone. Deterministic advisories upserted: {n_advisories} · retired: {n_retired}", flush=True)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
