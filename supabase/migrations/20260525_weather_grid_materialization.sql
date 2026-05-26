-- 20260525_weather_grid_materialization.sql
--
-- W1.3 of weather-grid-reference-layer.
--
-- Materialization layer — caches the 0.025° grid cell coordinates on
-- entity tables (beaches_gold, dog_parks_gold) and auto-registers each
-- cell in weather_grid as entities are added/moved.
--
-- After this migration:
--   - Consumers JOIN by weather_grid_lat/lon (no per-row function call)
--   - Loader reads weather_grid (no UNION query)
--   - New entities auto-extend coverage via the trigger
--
-- Per [[weather-grid-reference-layer]] two-level materialization spec.
-- Same pattern as the existing beach_polygon_membership cache.

begin;

-- ───────────────────────────────────────────────────────────────────────────
-- 1. Cached cols on entity tables
-- ───────────────────────────────────────────────────────────────────────────

alter table public.beaches_gold
  add column if not exists weather_grid_lat numeric(6,3),
  add column if not exists weather_grid_lon numeric(7,3);

alter table public.dog_parks_gold
  add column if not exists weather_grid_lat numeric(6,3),
  add column if not exists weather_grid_lon numeric(7,3);

create index if not exists beaches_gold_weather_grid_idx
  on public.beaches_gold (weather_grid_lat, weather_grid_lon)
  where is_active;
create index if not exists dog_parks_gold_weather_grid_idx
  on public.dog_parks_gold (weather_grid_lat, weather_grid_lon)
  where is_active;

comment on column public.beaches_gold.weather_grid_lat is
  'Cached 0.025° bin-floor latitude. Auto-maintained by tg_compute_weather_grid_cell trigger. Used by consumers to JOIN weather_grid_hourly without a function call.';
comment on column public.beaches_gold.weather_grid_lon is
  'Cached 0.025° bin-floor longitude. Auto-maintained.';
comment on column public.dog_parks_gold.weather_grid_lat is
  'Cached 0.025° bin-floor latitude. Auto-maintained by tg_compute_weather_grid_cell trigger.';
comment on column public.dog_parks_gold.weather_grid_lon is
  'Cached 0.025° bin-floor longitude. Auto-maintained.';

-- ───────────────────────────────────────────────────────────────────────────
-- 2. Shared trigger function — computes cached cols + registers cell
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public.tg_compute_weather_grid_cell()
returns trigger
language plpgsql
as $$
declare
  v_changed boolean;
begin
  v_changed := tg_op = 'INSERT'
            or new.lat is distinct from old.lat
            or new.lon is distinct from old.lon;

  if v_changed and new.lat is not null and new.lon is not null then
    new.weather_grid_lat := public.weather_grid_bin_lat(new.lat);
    new.weather_grid_lon := public.weather_grid_bin_lon(new.lon);

    -- Auto-register the cell in weather_grid (idempotent)
    insert into public.weather_grid (grid_lat, grid_lon, geom, state)
    values (
      new.weather_grid_lat,
      new.weather_grid_lon,
      st_setsrid(st_makepoint(new.weather_grid_lon::float, new.weather_grid_lat::float), 4326)::geography,
      new.state
    )
    on conflict (grid_lat, grid_lon) do nothing;
  end if;

  return new;
end $$;

comment on function public.tg_compute_weather_grid_cell is
  'Trigger fn used by beaches_gold + dog_parks_gold. Computes cached weather_grid_lat/lon from lat/lon AND auto-registers the cell in weather_grid via INSERT ON CONFLICT DO NOTHING. Per [[weather-grid-reference-layer]].';

-- ───────────────────────────────────────────────────────────────────────────
-- 3. Triggers on both entity tables
-- ───────────────────────────────────────────────────────────────────────────

drop trigger if exists tg_beaches_gold_weather_grid on public.beaches_gold;
create trigger tg_beaches_gold_weather_grid
  before insert or update on public.beaches_gold
  for each row execute function public.tg_compute_weather_grid_cell();

drop trigger if exists tg_dog_parks_gold_weather_grid on public.dog_parks_gold;
create trigger tg_dog_parks_gold_weather_grid
  before insert or update on public.dog_parks_gold
  for each row execute function public.tg_compute_weather_grid_cell();

commit;
