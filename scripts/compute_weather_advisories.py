"""Compute deterministic-weather advisories from beach_day_hourly_scores.

v2 rewrite per Franz 2026-05-30 (task #3 of v1-retirement). Drops the v1
`*_status` column reads; per-hour severity is now derived from raw values
via the SQL helper `public.v2_signal_status(entity_type, signal_key, raw)`.

Why: v1 status columns were driven by hardcoded thresholds in
_shared/scoring.ts that drifted from scoring_config_v2's bands — e.g.
v1 fired "Hot asphalt advisory" at 105°F where v2 says no penalty below
115°F. Confirmed false-positive pills on fid 8344 (Torrey Pines):
asphalt 111°F minor, heat 78°F minor, wind 10mph minor — all noise.
The v2 helper eliminates these by reading scoring_config_v2.bands +
the hardcoded gate overrides. See pin [[v2-signal-status-mapping]].

Reads:
  beaches_gold (scope)
  beach_day_hourly_scores (today + tomorrow, raw values only)
  beach_day_recommendations (today; bacteria_risk daily field)

Writes:
  beach_advisory rows whose v2_signal_status >= 'advisory' on ≥1 future
  hour, plus bacteria_risk in (moderate, high).

Source name in beach_advisory:
  source='deterministic_weather' for hourly metric advisories
  source='deterministic_bacteria' for daily bacteria

advisory_key includes the local date so re-runs same-day refresh the
same row; next day = new row.

Run:
  python scripts/compute_weather_advisories.py            # pilot 30 CA
  python scripts/compute_weather_advisories.py --all-mvp
  python scripts/compute_weather_advisories.py --fid 8344
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


# v2 status → beach_advisory.severity column value.
# 'clear' results don't write rows.
V2_TO_SEVERITY = {"no_go": "severe", "caution": "moderate", "advisory": "minor"}
V2_RANK = {"clear": 0, "advisory": 1, "caution": 2, "no_go": 3}

# Metric specs — v2 signal_key drives status; raw_col reads the value
# directly from beach_day_hourly_scores. Cold metric uses `min` over
# triggered hours; everything else uses `max`.
# Fields: (advisory_event_type, signal_key, raw_col, label, icon, dog_impact_class,
#          text_tmpl, unit_fmt, aggregate)
HOURLY_METRICS = [
    ("sand_status",     "sand_temp_neg",   "sand_temp",     "Hot sand",    "🏖️", "paws_warning",
     "Sand will hit {observed}°F — paws will burn. Go dawn or dusk.", "{:.0f}°F", "max"),
    ("asphalt_status",  "asphalt_neg",     "asphalt_temp",  "Hot asphalt", "🚶", "paws_warning",
     "Parking-lot asphalt {observed}°F — booties for the walk in.", "{:.0f}°F", "max"),
    ("uv_status",       "uv_neg",          "uv_index",      "High UV",     "☀️", "review_required",
     "UV peaks at {observed} — sunscreen for you, shade breaks for the pup.", "{:.0f}", "max"),
    ("wind_status",     "wind_harsh_neg",  "wind_speed",    "Strong wind", "💨", "blowing_sand",
     "Wind gusts {observed}mph — blowing sand will sting.", "{:.0f}mph", "max"),
    ("tide_status",     "tide_neg",        "tide_height",   "High tide",   "🌊", "skip_swim",
     "High tide ≥{observed}ft — limited beach to walk on.", "{:.1f}ft", "max"),
    ("rain_status",     "precip_chance",   "precip_chance", "Rain",        "🌧️", "review_required",
     "Rain likely ({observed}% chance) — bring a towel.", "{:.0f}%", "max"),
    ("temp_hot_status", "feels_like_hot",  "feels_like",    "Heat",        "🥵", "paws_warning",
     "Hot day (feels like {observed}°F) — dawn or dusk, plenty of water.", "{:.0f}°F", "max"),
    ("temp_cold_status","feels_like_cold", "feels_like",    "Cold",        "🥶", "cold_paws",
     "Cold (feels like {observed}°F) — short coats may need a jacket.", "{:.0f}°F", "min"),
    ("crowd_status",    "crowd_neg",       "busyness_score","Crowded",     "👥", "review_required",
     "Beach is busy (score {observed}) — reactive dogs may struggle.", "{:.0f}", "max"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--pilot", action="store_true", help="(default) pilot 30 CA")
    grp.add_argument("--all-mvp", action="store_true")
    grp.add_argument("--state")
    grp.add_argument("--fid", type=int, help="single beach fid")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = connect()
    try:
        with conn.cursor() as cur:
            if args.fid:
                cur.execute("""
                    SELECT fid, location_id FROM public.beaches_gold
                     WHERE fid=%s AND location_id IS NOT NULL
                """, (args.fid,))
            elif args.state:
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
        n_retired = 0
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        upd = conn.cursor()
        for i, (fid, location_id) in enumerate(scope, 1):
            # Read today + tomorrow's hourly rows — raw values only, no
            # v1 *_status columns. v2_signal_status() derives severity
            # per metric at write time.
            cur.execute("""
                SELECT local_date, local_hour, forecast_ts,
                       sand_temp, asphalt_temp, uv_index, wind_speed,
                       tide_height, precip_chance, feels_like, busyness_score,
                       public.v2_signal_status('beach','sand_temp_neg',   sand_temp::numeric)   AS sand_temp_v2,
                       public.v2_signal_status('beach','asphalt_neg',     asphalt_temp::numeric) AS asphalt_v2,
                       public.v2_signal_status('beach','uv_neg',          uv_index::numeric)    AS uv_v2,
                       public.v2_signal_status('beach','wind_harsh_neg',  wind_speed::numeric)  AS wind_v2,
                       public.v2_signal_status('beach','tide_neg',        tide_height::numeric) AS tide_v2,
                       public.v2_signal_status('beach','precip_chance',   precip_chance::numeric) AS precip_v2,
                       public.v2_signal_status('beach','feels_like_hot',  feels_like::numeric)  AS feels_hot_v2,
                       public.v2_signal_status('beach','feels_like_cold', feels_like::numeric)  AS feels_cold_v2,
                       public.v2_signal_status('beach','crowd_neg',       busyness_score::numeric) AS crowd_v2
                  FROM public.beach_day_hourly_scores
                 WHERE location_id = %s
                   AND local_date >= (now() at time zone 'UTC')::date
                   AND local_date <= ((now() at time zone 'UTC')::date + interval '1 day')
                 ORDER BY local_date, local_hour
            """, (location_id,))
            hours = [dict(r) for r in cur.fetchall()]
            if not hours:
                continue

            # Group by local_date — emit per-day advisories
            by_date: dict[str, list[dict]] = {}
            for h in hours:
                by_date.setdefault(str(h["local_date"]), []).append(h)

            # Map signal_key → v2 column alias used in the SELECT above
            v2_alias = {
                "sand_temp_neg":   "sand_temp_v2",
                "asphalt_neg":     "asphalt_v2",
                "uv_neg":          "uv_v2",
                "wind_harsh_neg":  "wind_v2",
                "tide_neg":        "tide_v2",
                "precip_chance":   "precip_v2",
                "feels_like_hot":  "feels_hot_v2",
                "feels_like_cold": "feels_cold_v2",
                "crowd_neg":       "crowd_v2",
            }

            # Time-aware filter: advisories matter only for the rest of
            # the day. If a high tide peaked at 7am and it's 2pm, nobody
            # needs a warning. Past-only triggered events are actively
            # retired so any row from an earlier run drops out.
            now_utc = datetime.now(timezone.utc)
            for date_iso, day_hours in by_date.items():
                for (event_type, signal_key, raw_col, label, icon, klass, text_tmpl, unit_fmt, agg) in HOURLY_METRICS:
                    alias = v2_alias[signal_key]
                    triggered_all = [h for h in day_hours
                                     if V2_RANK.get(h.get(alias) or "clear", 0) >= 1]
                    if not triggered_all:
                        continue
                    # Worst severity across FULL-day data so advisory_key
                    # stays stable across runs.
                    worst = max((h[alias] for h in triggered_all),
                                key=lambda s: V2_RANK.get(s, 0))
                    advisory_key = f"det:{event_type}_{worst}:{date_iso}"

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
                            n_retired += 1
                        continue

                    severity = V2_TO_SEVERITY[worst]
                    # Observed extreme — max for "too much" metrics, min
                    # for "too cold". Computed over future-only hours so
                    # the displayed value is what the user can still act on.
                    vals = [h[raw_col] for h in triggered if h[raw_col] is not None]
                    if not vals:
                        continue
                    extreme = (min if agg == "min" else max)(vals)
                    text = text_tmpl.format(observed=extreme)
                    value_str = unit_fmt.format(extreme)
                    first = triggered[0]; last = triggered[-1]
                    if args.dry_run:
                        print(f"  fid={fid:<10} {date_iso}  {event_type:<18} {worst:<8} val={extreme}  → {text}", flush=True)
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

            # Also retire any v1-era rows for this beach today whose
            # advisory_key was generated under the old status-column
            # naming. Without this sweep, "asphalt 111°F minor" pills
            # written in pre-v2 runs would linger until they aged out.
            if not args.dry_run:
                upd.execute("""
                    DELETE FROM public.beach_advisory
                     WHERE beach_fid = %s
                       AND source = 'deterministic_weather'
                       AND raw_data->>'scoring_version' IS DISTINCT FROM 'v2'
                       AND fetched_at < now() - interval '1 minute'
                """, (fid,))
                if upd.rowcount:
                    n_retired += upd.rowcount

            # Bacteria from daily recommendations — unchanged. Bacteria
            # risk is a daily-level field with its own categorical scale
            # (none/low/moderate/high), not band-derived.
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

        print(f"\nDone. Deterministic advisories upserted: {n_advisories} · retired: {n_retired}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
