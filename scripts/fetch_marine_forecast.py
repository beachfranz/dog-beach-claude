"""Fetch hourly Open-Meteo marine forecast per beach.

Per docs/water_conditions_advisory_spec.md Phase 3.

Output: upsert into public.beach_marine_forecast (wave/swell/SST/wind).

Marine API is OCEAN-ONLY. Inland lake beaches won't have data; they're
filtered out (those beaches stay in the regular weather pipeline).

Run:
  python scripts/fetch_marine_forecast.py                # pilot (30 CA)
  python scripts/fetch_marine_forecast.py --state CA
  python scripts/fetch_marine_forecast.py --all-mvp
  python scripts/fetch_marine_forecast.py --dry-run
  python scripts/fetch_marine_forecast.py --hours 168    # 7-day forecast
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect

MARINE_BASE = "https://marine-api.open-meteo.com/v1/marine"
USER_AGENT = "DogBeachScout/1.0 (https://dogbeachscout.com; franz@franzfunk.com) marine-fetch"

MARINE_VARIABLES = [
    "wave_height", "wave_period", "wave_direction",
    "swell_wave_height", "swell_wave_period",
    "wind_wave_height", "wind_wave_period",
    "sea_surface_temperature", "ocean_current_velocity",
]

PACING_S = 1.2   # generous rate limits but polite


def fetch_one(lat: float, lng: float, hours: int = 48) -> dict | None:
    params = {
        "latitude": f"{lat}",
        "longitude": f"{lng}",
        "hourly": ",".join(MARINE_VARIABLES),
        "timezone": "UTC",
        "forecast_days": str(max(1, (hours + 23) // 24)),
    }
    try:
        r = requests.get(MARINE_BASE, params=params,
                         headers={"User-Agent": USER_AGENT}, timeout=30)
        if r.status_code == 400:
            # Common cause: beach is inland / no marine grid cell. Quiet skip.
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  marine fetch fail ({lat:.3f},{lng:.3f}): {e}", flush=True)
        return None


def upsert_marine(conn, beach_fid: int, data: dict, max_hours: int) -> int:
    hourly = (data or {}).get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return 0
    cols = {v: hourly.get(v) or [] for v in MARINE_VARIABLES}
    upd = conn.cursor()
    n = 0
    for i, t in enumerate(times[:max_hours]):
        ts = t + "Z" if "T" in t and not t.endswith("Z") else t
        row = {
            "ts": ts,
            "wave_height_m":           cols["wave_height"][i]              if i < len(cols["wave_height"]) else None,
            "wave_period_s":           cols["wave_period"][i]              if i < len(cols["wave_period"]) else None,
            "wave_direction_deg":      cols["wave_direction"][i]           if i < len(cols["wave_direction"]) else None,
            "swell_wave_height_m":     cols["swell_wave_height"][i]        if i < len(cols["swell_wave_height"]) else None,
            "swell_wave_period_s":     cols["swell_wave_period"][i]        if i < len(cols["swell_wave_period"]) else None,
            "wind_wave_height_m":      cols["wind_wave_height"][i]         if i < len(cols["wind_wave_height"]) else None,
            "wind_wave_period_s":      cols["wind_wave_period"][i]         if i < len(cols["wind_wave_period"]) else None,
            "sst_c":                   cols["sea_surface_temperature"][i]  if i < len(cols["sea_surface_temperature"]) else None,
            "ocean_current_velocity_ms": cols["ocean_current_velocity"][i] if i < len(cols["ocean_current_velocity"]) else None,
        }
        upd.execute("""
            INSERT INTO public.beach_marine_forecast (
              beach_fid, ts, wave_height_m, wave_period_s, wave_direction_deg,
              swell_wave_height_m, swell_wave_period_s,
              wind_wave_height_m, wind_wave_period_s,
              sst_c, ocean_current_velocity_ms,
              source, fetched_at
            ) VALUES (%(fid)s, %(ts)s, %(wave_height_m)s, %(wave_period_s)s, %(wave_direction_deg)s,
                      %(swell_wave_height_m)s, %(swell_wave_period_s)s,
                      %(wind_wave_height_m)s, %(wind_wave_period_s)s,
                      %(sst_c)s, %(ocean_current_velocity_ms)s,
                      'open_meteo', now())
            ON CONFLICT (beach_fid, ts) DO UPDATE SET
              wave_height_m            = EXCLUDED.wave_height_m,
              wave_period_s            = EXCLUDED.wave_period_s,
              wave_direction_deg       = EXCLUDED.wave_direction_deg,
              swell_wave_height_m      = EXCLUDED.swell_wave_height_m,
              swell_wave_period_s      = EXCLUDED.swell_wave_period_s,
              wind_wave_height_m       = EXCLUDED.wind_wave_height_m,
              wind_wave_period_s       = EXCLUDED.wind_wave_period_s,
              sst_c                    = EXCLUDED.sst_c,
              ocean_current_velocity_ms = EXCLUDED.ocean_current_velocity_ms,
              fetched_at               = now()
        """, {**row, "fid": beach_fid})
        n += 1
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--pilot", action="store_true",
                     help="Pilot scope: top 30 tier-1 CA beaches (default)")
    grp.add_argument("--state", help="All scoreable beaches in one state")
    grp.add_argument("--all-mvp", action="store_true", help="All MVP+ (CA + OR + WA)")
    ap.add_argument("--hours", type=int, default=48,
                    help="Forecast horizon in hours (default 48; max 168)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = connect()
    try:
        with conn.cursor() as cur:
            if args.state:
                cur.execute("""
                    SELECT fid, name, lat, lon FROM public.beaches_gold
                     WHERE state = %s AND is_active = true
                       AND scoring_tier IN ('daily','hourly')
                       AND lat IS NOT NULL AND lon IS NOT NULL
                """, (args.state.upper(),))
            elif args.all_mvp:
                cur.execute("""
                    SELECT fid, name, lat, lon FROM public.beaches_gold
                     WHERE state IN ('CA','OR','WA') AND is_active = true
                       AND scoring_tier IN ('daily','hourly')
                       AND lat IS NOT NULL AND lon IS NOT NULL
                """)
            else:
                cur.execute("""
                    SELECT fid, name, lat, lon FROM public.beaches_gold
                     WHERE state='CA' AND is_active = true
                       AND scoring_tier IN ('daily','hourly')
                       AND catchment_score IS NOT NULL
                       AND lat IS NOT NULL AND lon IS NOT NULL
                     ORDER BY catchment_score DESC LIMIT 30
                """)
            beaches = cur.fetchall()
        print(f"Scope: {len(beaches)} beaches", flush=True)

        n_fetched = n_inland = n_rows = 0
        for i, (fid, name, lat, lng) in enumerate(beaches, 1):
            data = fetch_one(lat, lng, args.hours)
            if data is None:
                n_inland += 1
                print(f"  [{i}/{len(beaches)}] fid={fid:<10} {name[:35]:<35}  (no marine grid — inland?)", flush=True)
                time.sleep(PACING_S); continue
            if args.dry_run:
                hourly = (data or {}).get("hourly") or {}
                ts_count = len(hourly.get("time") or [])
                first_wave = (hourly.get("wave_height") or [None])[0]
                first_sst  = (hourly.get("sea_surface_temperature") or [None])[0]
                print(f"  [{i}/{len(beaches)}] fid={fid:<10} {name[:35]:<35}  {ts_count}hr  wave={first_wave}m sst={first_sst}°C  [dry]", flush=True)
            else:
                rows = upsert_marine(conn, fid, data, args.hours)
                n_rows += rows
                if i % 5 == 0:
                    conn.commit()
                print(f"  [{i}/{len(beaches)}] fid={fid:<10} {name[:35]:<35}  {rows} hours", flush=True)
            n_fetched += 1
            time.sleep(PACING_S)
        if not args.dry_run:
            conn.commit()

        print(f"\n=== SUMMARY ===")
        print(f"  Beaches with marine data: {n_fetched}")
        print(f"  Beaches inland (skipped): {n_inland}")
        print(f"  Forecast rows upserted:   {n_rows}")
        if args.dry_run:
            print(f"  [dry-run] no writes")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
