---
name: noaa
description: Use this skill when working with NOAA tide data — refreshing the tide cache, debugging missing tide rows, adding stations for new beaches, or fixing tide rendering on beach.html / detail.html. Triggers include "refresh tides for <state>", "tide is missing on <beach>", "tide curve isn't rendering", "the tide cache is stale", "add a tide station for <beach>", "weekly_tide_refresh failed", "expand to subordinate stations". Covers the tide_grid_hourly reference-layer architecture (per-NOAA-station cache mirroring the W2 weather_grid pattern), the CO-OPS API contract, the loader + consumer wiring, and the failure modes from the 2026-06-07 cutover. DO NOT use for Open-Meteo wave/swell/current signals (those live in marine_grid_hourly via Open-Meteo Marine — different network entirely), BestTime crowd data, or new-state beach inventory (use new-state-onboard).
---

# noaa — tide_grid reference layer

NOAA CO-OPS tide predictions are cached as a reference layer: one row per (station_id, forecast_ts) in `tide_grid_hourly`. Consumers (daily-beach-refresh, beach.html, detail.html) read from the cache. **The loader fetches per-STATION, not per-beach.** ~296 primary stations cover all 3,899 active scoring beaches — ~13× redundancy that the grid eliminates.

This mirrors the W2 weather_grid + marine_grid patterns. Same architectural rule applies: **never fall back to direct NOAA in the consumer path.** That's the failure mode we removed on 2026-06-07.

## The two NOAA networks

| Network | What it gives | Where in our system |
|---|---|---|
| **CO-OPS** — Center for Operational Oceanographic Products and Services | Tide predictions, water levels, station-keyed currents | **HERE.** `tide_grid_hourly`, populated by `refresh-tide-stations` edge fn |
| **NDBC** — National Data Buoy Center | Wave heights, periods, SST, ocean currents from offshore buoys | We DON'T use directly. Open-Meteo Marine wraps these signals into `marine_grid_hourly` (per migration 20260606l). |

If a request mentions waves, swell, or surf height — that's `marine_grid` territory, not this skill.

## The architecture

```
beaches_gold.noaa_station_id
        │
        ├── trigger tg_register_tide_station (on INSERT/UPDATE)
        ▼
tide_station_inventory      ← loader picks from here
   (station_id, first_seen, last_fetched_at, last_error)
        │
        ▼ refresh-tide-stations (edge fn, weekly cron)
        │  fetches NOAA CO-OPS per station, upserts
        ▼
tide_grid_hourly
   (station_id, forecast_ts, tide_height_ft, tide_direction, fetched_at, source)
        │
        ├── tide_for_station(station_id, start, end)  ← per-station read
        ├── tide_for_beach(fid, start, end)            ← per-beach read (joins via station)
        └── tide_station_is_warm(station_id, hours)    ← gate for partial coverage
        │
        ▼
daily-beach-refresh
   `lookupTidesFromGrid()` queries tide_for_station,
   converts UTC forecast_ts → beach-local "YYYY-MM-DD HH" hour keys,
   joins with Open-Meteo hourly, writes denorm tide_height into
   beach_day_hourly_scores
        │
        ▼
get_beach_info (beach.html), get-beaches-find, etc.
   read tide_height from beach_day_hourly_scores
```

The per-beach denorm in `beach_day_hourly_scores.tide_height` is kept (backwards compat) but the source-of-truth is `tide_grid_hourly`. Future Phase 2: drop the denorm and have consumers JOIN `tide_for_beach` at read time.

## Run shapes

```bash
# Manual full refresh (forces all stations regardless of staleness)
.venv-pipeline/Scripts/python.exe scripts/call_edge.py refresh-tide-stations \
  --body '{"force_full": true, "limit": 500}'

# Refresh a specific station list (smoke test, new station)
.venv-pipeline/Scripts/python.exe scripts/call_edge.py refresh-tide-stations \
  --body '{"station_ids": ["9410230", "1612340"]}'

# Trigger the weekly cron fire fn manually (uses default limit 500, force_full=true)
supabase db query --linked "SELECT public._fire_weekly_tide_refresh()"

# Drop tide for one beach forward (downstream verification after a refresh)
.venv-pipeline/Scripts/python.exe scripts/call_edge.py daily-beach-refresh \
  --body '{"location_ids": ["la-jolla-shores-san-diego"], "limit": 1}'
```

If `scripts/call_edge.py` doesn't exist in the repo, build the request inline — same shape as the smoke test in commit `d1333b3`:

```python
import json, urllib.request
with open('scripts/pipeline/.env') as f:
    for line in f:
        if line.startswith('ADMIN_SECRET='):
            secret = line.split('=', 1)[1].strip().strip("'\""); break
req = urllib.request.Request(
    'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/refresh-tide-stations',
    method='POST',
    headers={
        'Content-Type': 'application/json',
        'Authorization': 'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
        'x-admin-secret': secret,
    },
    data=json.dumps({'force_full': True, 'limit': 500}).encode())
print(urllib.request.urlopen(req, timeout=300).read().decode())
```

Expected timing: ~28s for the full 296-station refresh, ~100,000 rows upserted.

## NOAA CO-OPS API contract

Endpoint: `https://api.tidesandcurrents.noaa.gov/api/prod/datagetter`. No API key required.

Loader uses these params:

| Param | Value | Why |
|---|---|---|
| `station` | station_id | the CO-OPS station identifier (e.g. `9410230` La Jolla) |
| `product` | `predictions` | harmonic-computed; deterministic; valid years into the future |
| `datum` | `MLLW` | Mean Lower Low Water — standard marine chart datum |
| `units` | `english` | feet |
| `time_zone` | **`gmt`** | returns UTC. **Critical** — the legacy per-beach fetcher used `lst_ldt` which forced beach-local-time math at write. Grid stores UTC; consumers convert on read. |
| `interval` | `h` | hourly (6 also works for 6-min, but overkill) |
| `begin_date` | `YYYYMMDD` | start (we use `now - 1d` for a small back-buffer) |
| `end_date` | `YYYYMMDD` | end (we use `now + 14d`) |
| `format` | `json` | |

Response shape: `{ predictions: [{ t: "YYYY-MM-DD HH:MM", v: "3.42" }, ...] }`. `tide_direction` is computed at write time by comparing adjacent rows.

## Debugging

### "Tide coverage is at 11%" / "the tide cache is stale"

Most likely: loader hasn't run, OR the orch fire fn is still pointed at the legacy daily-beach-refresh path. Check:

```sql
-- When did the last refresh complete?
SELECT job_name, last_succeeded_at, last_error
  FROM public.orch_jobs WHERE job_name='weekly_tide_refresh';

-- Per-station last-fetched distribution
SELECT
  date_trunc('day', last_fetched_at) AS bucket,
  count(*) AS stations
FROM public.tide_station_inventory
GROUP BY 1 ORDER BY 1 DESC NULLS LAST;

-- Per-station error sample
SELECT station_id, last_fetched_at, last_error
  FROM public.tide_station_inventory
 WHERE last_error IS NOT NULL ORDER BY last_fetched_at LIMIT 10;
```

If `_fire_weekly_tide_refresh` is still POSTing daily-beach-refresh, it's pre-cutover. Re-apply migration `20260607k` or rewrite the fire fn to call `refresh-tide-stations` instead.

### "Tide is missing on <beach>"

The beach's `noaa_station_id` either isn't set, isn't in `tide_station_inventory`, or the station's cache is cold.

```sql
-- Resolve fid → station → coverage
SELECT g.fid, g.name, g.noaa_station_id,
       i.last_fetched_at, i.last_error,
       (SELECT count(*) FROM tide_grid_hourly t
         WHERE t.station_id = g.noaa_station_id
           AND t.forecast_ts >= now()
           AND t.forecast_ts <  now() + interval '24 hours') AS hours_warm_24h
FROM beaches_gold g
LEFT JOIN tide_station_inventory i ON i.station_id = g.noaa_station_id
WHERE g.fid = <FID>;
```

If `noaa_station_id IS NULL`: the beach is inland or hasn't been station-matched (`v2-noaa-station-match` pipeline step). Score's tide component defaults to neutral (0.5); that's fine for landlocked beaches.

If `noaa_station_id` is set but not in inventory: the trigger should have fired on the last UPDATE — manually backfill `INSERT INTO tide_station_inventory (station_id) VALUES ('<id>') ON CONFLICT DO NOTHING`.

If the station is in inventory but cold: fire `refresh-tide-stations` with `{"station_ids": ["<id>"]}` to warm it immediately.

### "Tide curve renders with weird times" (DST / timezone wonky)

The grid stores UTC. The consumer (`lookupTidesFromGrid` in daily-beach-refresh, line ~993ish) converts UTC `forecast_ts` to beach-local "YYYY-MM-DD HH" keys via `Intl.DateTimeFormat({timeZone: beach.timezone})`. If the conversion looks off:

- Confirm `beach.timezone` is correct on `beaches_gold` (e.g. `Pacific/Honolulu`, `America/Los_Angeles`)
- Confirm NOAA fetch used `time_zone=gmt`, not `lst_ldt` — peek at one row's `forecast_ts` and verify the math against tidesandcurrents.noaa.gov for the same station + day
- The hourly `is_day` flip near sunrise/sunset is a separate signal — don't conflate with tide timing

### "HI tide coverage is missing"

Should NOT happen post-cutover. The new loader picks from `tide_station_inventory` which auto-tracks every catalog state via the `tg_register_tide_station` trigger. Pre-cutover the legacy `weekly_tide_refresh.scope_states` list lacked HI; that scope is now decorative.

If HI beaches are missing tides, confirm:
1. Their `beaches_gold.noaa_station_id` is set (HI primary stations: `1611400` Nawiliwili, `1612340` Honolulu, `1612480` Mokuoloe, `1615680` Kahului, `1617433` Kawaihae, `1617760` Hilo)
2. Those stations are in `tide_station_inventory`
3. The loader has run since the station joined the inventory

## When to widen with subordinate stations

NOAA CO-OPS has ~296 primary stations + **~3,200 subordinate stations** derived from those primaries' harmonics. Some beaches are 30+ miles from their assigned primary — prediction error grows with distance because tide timing/amplitude varies with coastal geometry.

Same API endpoint, just different `station_id` list. To widen:

1. Find subordinate station candidates near beaches with poor coverage (NOAA's station finder: https://tidesandcurrents.noaa.gov/stations.html)
2. Update `beaches_gold.noaa_station_id` for affected beaches → the trigger registers the new station in `tide_station_inventory`
3. Fire `refresh-tide-stations` to warm the new station: `{"station_ids": ["<id>"]}`

Widening is parked Phase 2 — primaries cover the bulk acceptably. Revisit if specific beaches show real-world tide drift complaints.

## Reference files

| Path | What |
|---|---|
| `supabase/migrations/20260607j_tide_grid_reference_layer.sql` | Schema + helpers + trigger |
| `supabase/migrations/20260607k_weekly_tide_refresh_uses_grid.sql` | Orch fire fn wired to new loader |
| `supabase/functions/refresh-tide-stations/index.ts` | Loader (concurrency=4, 14-day horizon, retries) |
| `supabase/functions/daily-beach-refresh/index.ts` | Consumer — `lookupTidesFromGrid` helper around line 993 |
| `supabase/functions/daily-beach-refresh/noaa.ts` | Legacy `fetchTides` — kept as reference, no longer imported |

## Related skills

- `weather_grid` reference layer (W2): same architectural pattern for Open-Meteo weather. Pin: `[[weather-grid-reference-layer]]`.
- `marine_grid` reference layer: same pattern for wave/swell/current from Open-Meteo Marine. Migration `20260606l`.
- `[[grid-consumers-require-cell-warmth-gate]]` HARD rule — applies to tide too (use `tide_station_is_warm` for read gating when partial coverage would mislead).
