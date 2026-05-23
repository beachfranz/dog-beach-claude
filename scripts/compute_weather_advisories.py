"""Compute deterministic-weather advisories from beach_day_hourly_scores.

Per Franz 2026-05-19 unified advisory store (option 2: parallel Python
writer; scoring_config is shared source of truth so no logic duplication).

Reads:
  scoring_config (active row)
  beach_day_hourly_scores (today + tomorrow per pilot beach)
  beach_day_recommendations (today; bacteria_risk daily field)

Writes:
  beach_advisory rows for any hourly metric whose status >= 'caution'
  on >=1 hour, plus bacteria_risk in (moderate, high).

Source name in beach_advisory:
  source='deterministic_weather' for hourly metric advisories
  source='deterministic_bacteria' for daily bacteria

advisory_key includes the local date so re-runs same-day refresh the
same row; next day = new row.

Run:
  python scripts/compute_weather_advisories.py            # pilot 30 CA
  python scripts/compute_weather_advisories.py --all-mvp
  python scripts/compute_weather_advisories.py --dry-run
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import json
import os
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect


# Per-status severity mapping
STATUS_SEVERITY = {"no_go": "severe", "caution": "moderate", "advisory": "minor"}

# Metric specs — (status_col, value_col, label, icon, class, text_tmpl, unit_fmt)
HOURLY_METRICS = [
    ("sand_status",   "sand_temp",  "Hot sand",   "🏖️", "paws_warning",
     "Sand will hit {observed}°F — paws will burn. Go dawn or dusk.", "{:.0f}°F"),
    ("asphalt_status","asphalt_temp","Hot asphalt","🚶", "paws_warning",
     "Parking-lot asphalt {observed}°F — booties for the walk in.", "{:.0f}°F"),
    ("uv_status",     "uv_index",   "High UV",    "☀️", "review_required",
     "UV peaks at {observed} — sunscreen for you, shade breaks for the pup.", "{:.0f}"),
    ("wind_status",   "wind_speed", "Strong wind","💨", "blowing_sand",
     "Wind gusts {observed}mph — blowing sand will sting.", "{:.0f}mph"),
    ("tide_status",   "tide_height","High tide",  "🌊", "skip_swim",
     "High tide ≥{observed}ft — limited beach to walk on.", "{:.1f}ft"),
    ("rain_status",   "precip_chance","Rain",     "🌧️", "review_required",
     "Rain likely ({observed}% chance) — bring a towel.", "{:.0f}%"),
    ("temp_hot_status","temp_air",  "Heat",       "🥵", "paws_warning",
     "Hot day ({observed}°F) — dawn or dusk, plenty of water.", "{:.0f}°F"),
    ("temp_cold_status","temp_air", "Cold",       "🥶", "cold_paws",
     "Cold ({observed}°F) — short coats may need a jacket.", "{:.0f}°F"),
    ("crowd_status",  "busyness_score","Crowded", "👥", "review_required",
     "Beach is busy (score {observed}) — reactive dogs may struggle.", "{:.0f}"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--pilot", action="store_true", help="(default) pilot 30 CA")
    grp.add_argument("--all-mvp", action="store_true")
    grp.add_argument("--state")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = connect()
    try:
        with conn.cursor() as cur:
            if args.state:
                cur.execute("""
                    SELECT fid, location_id FROM public.beaches_gold
                     WHERE state=%s AND is_active AND scoring_tier IN ('daily','hourly')
                       AND location_id IS NOT NULL
                """, (args.state.upper(),))
            elif args.all_mvp:
                cur.execute("""
                    SELECT fid, location_id FROM public.beaches_gold
                     WHERE state IN ('CA','OR','WA') AND is_active AND scoring_tier IN ('daily','hourly')
                       AND location_id IS NOT NULL
                """)
            else:
                cur.execute("""
                    SELECT fid, location_id FROM public.beaches_gold
                     WHERE state='CA' AND is_active AND scoring_tier IN ('daily','hourly')
                       AND catchment_score IS NOT NULL AND location_id IS NOT NULL
                     ORDER BY catchment_score DESC LIMIT 30
                """)
            scope = cur.fetchall()
        print(f"Scope: {len(scope)} beaches", flush=True)

        n_advisories = 0
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        upd = conn.cursor()
        for i, (fid, location_id) in enumerate(scope, 1):
            # Read today + tomorrow's hourly rows
            cur.execute("""
                SELECT local_date, local_hour, forecast_ts,
                       sand_status, sand_temp, asphalt_status, asphalt_temp,
                       uv_status, uv_index, wind_status, wind_speed,
                       tide_status, tide_height, rain_status, precip_chance,
                       temp_hot_status, temp_cold_status, temp_air,
                       crowd_status, busyness_score
                  FROM public.beach_day_hourly_scores
                 WHERE location_id = %s
                   AND local_date >= (now() at time zone 'UTC')::date
                   AND local_date <= ((now() at time zone 'UTC')::date + interval '1 day')
                 ORDER BY local_date, local_hour
            """, (location_id,))
            hours = [dict(r) for r in cur.fetchall()]
            if not hours:
                continue

            # Group by local_date so we emit per-day advisories
            by_date: dict[str, list[dict]] = {}
            for h in hours:
                by_date.setdefault(str(h["local_date"]), []).append(h)

            # Time-aware filter: cautions only matter for the rest of the
            # day. If a high tide peaked at 7am and it's now 2pm, nobody
            # needs a warning about it — strip past hours before computing
            # the extreme + valid_from. Past-only triggered events are
            # actively retired so any row from an earlier run drops out.
            now_utc = datetime.now(timezone.utc)
            for date_iso, day_hours in by_date.items():
                for (status_col, value_col, label, icon, klass, text_tmpl, unit_fmt) in HOURLY_METRICS:
                    triggered_all = [h for h in day_hours if h.get(status_col) in ("caution","no_go","advisory")]
                    if not triggered_all:
                        continue
                    # Worst status — use FULL-day data so the advisory_key
                    # stays stable across runs (we drop the row entirely
                    # below if no future-only triggered hours remain).
                    worst = max((h[status_col] for h in triggered_all),
                                key=lambda s: {"advisory":1,"caution":2,"no_go":3}.get(s, 0))
                    advisory_key = f"det:{status_col}_{worst}:{date_iso}"

                    # Future-only triggered hours
                    triggered = [h for h in triggered_all
                                 if h.get("forecast_ts") and h["forecast_ts"] >= now_utc]
                    if not triggered:
                        # Spike already in the past — retire any prior row.
                        if not args.dry_run:
                            upd.execute("""
                                DELETE FROM public.beach_advisory
                                 WHERE beach_fid = %s AND advisory_key = %s
                            """, (fid, advisory_key))
                        continue

                    severity = STATUS_SEVERITY[worst]
                    # Observed extreme (max for "high X" metrics, min for cold)
                    # — computed over future-only hours so the displayed
                    # value is what the user can still act on.
                    vals = [h[value_col] for h in triggered if h[value_col] is not None]
                    if not vals: continue
                    extreme = (min if status_col == "temp_cold_status" else max)(vals)
                    text = text_tmpl.format(observed=extreme)
                    value_str = unit_fmt.format(extreme)
                    first = triggered[0]; last = triggered[-1]
                    if args.dry_run:
                        print(f"  fid={fid:<10} {date_iso}  {status_col:<18} {worst:<8} val={extreme}  → {text}", flush=True)
                        continue
                    upd.execute("""
                        INSERT INTO public.beach_advisory (
                          beach_fid, advisory_key, source, event_type, severity,
                          valid_from, valid_to, dog_impact_class, dog_impact_text,
                          translation_source, label, value, icon, raw_data, fetched_at
                        ) VALUES (%s, %s, 'deterministic_weather', %s, %s,
                                  %s, %s, %s, %s, 'rule', %s, %s, %s, %s, now())
                        ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
                          severity         = EXCLUDED.severity,
                          valid_from       = EXCLUDED.valid_from,
                          valid_to         = EXCLUDED.valid_to,
                          dog_impact_text  = EXCLUDED.dog_impact_text,
                          value            = EXCLUDED.value,
                          raw_data         = EXCLUDED.raw_data,
                          fetched_at       = now();
                    """, (
                        fid, advisory_key, status_col, severity,
                        first["forecast_ts"], last["forecast_ts"],
                        klass, text, label, value_str, icon,
                        json.dumps({"hours_triggered": len(triggered),
                                    "extreme_value": float(extreme),
                                    "worst_status": worst,
                                    "now_aware_filter": True}),
                    ))
                    n_advisories += 1

            # Bacteria from daily recommendations
            cur.execute("""
                SELECT local_date, bacteria_risk FROM public.beach_day_recommendations
                 WHERE location_id = %s
                   AND local_date >= (now() at time zone 'UTC')::date
                   AND local_date <= ((now() at time zone 'UTC')::date + interval '1 day')
                   AND bacteria_risk IN ('moderate','high')
            """, (location_id,))
            for row in cur.fetchall():
                date_iso = str(row["local_date"])
                risk = row["bacteria_risk"]
                advisory_key = f"det:bacteria_{risk}:{date_iso}"
                if args.dry_run:
                    print(f"  fid={fid:<10} {date_iso}  bacteria         {risk}", flush=True)
                    continue
                upd.execute("""
                    INSERT INTO public.beach_advisory (
                      beach_fid, advisory_key, source, event_type, severity,
                      valid_from, valid_to, dog_impact_class, dog_impact_text,
                      translation_source, label, value, icon, raw_data, fetched_at
                    ) VALUES (%s, %s, 'deterministic_bacteria', 'bacteria_risk', %s,
                              %s::date::timestamptz, (%s::date + interval '1 day')::timestamptz,
                              'skip_swim', %s, 'rule', 'Water-quality risk', %s, '⚠️', %s, now())
                    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
                      severity   = EXCLUDED.severity,
                      valid_from = EXCLUDED.valid_from,
                      valid_to   = EXCLUDED.valid_to,
                      fetched_at = now();
                """, (
                    fid, advisory_key, "severe" if risk=="high" else "moderate",
                    date_iso, date_iso,
                    f"Water-quality {risk} risk (recent rain) — keep your dog out of the surf.",
                    risk,
                    json.dumps({"bacteria_risk": risk}),
                ))
                n_advisories += 1

            if i % 10 == 0 and not args.dry_run:
                conn.commit()
        if not args.dry_run:
            conn.commit()

        print(f"\nDone. Deterministic advisories upserted: {n_advisories}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
