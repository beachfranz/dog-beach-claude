-- 20260606k_marine_grid_materialization.sql
--
-- M1.3 of marine-grid-reference-layer.
--
-- Materialization layer — caches the 0.1° marine cell coords on beaches_gold
-- and auto-registers each cell in marine_grid as coastal beaches are added.
--
-- Unlike weather_grid (which has triggers on both beaches_gold AND
-- dog_parks_gold), marine only matters for coastal beaches. The trigger
-- still fires on every beach insert/update; non-coastal beaches just won't
-- end up with marine_grid_hourly data (the loader skips inland cells that
-- Open-Meteo returns 400 for).
--
-- After this migration:
--   - Consumers JOIN by marine_grid_lat/lon (no per-row function call)
--   - Loader reads marine_grid (no UNION query)
--   - New beaches auto-extend coverage via trigger
--   - Backfill populates cached cols + registers cells for existing 2,218
--     coastal scoreable beaches across the 10 coastal MVP+ states

begin;

-- ───────────────────────────────────────────────────────────────────────────
-- 1. Cached cols on beaches_gold
-- ───────────────────────────────────────────────────────────────────────────

alter table public.beaches_gold
  add column if not exists marine_grid_lat numeric(5,2),
  add column if not exists marine_grid_lon numeric(6,2);

create index if not exists beaches_gold_marine_grid_idx
  on public.beaches_gold (marine_grid_lat, marine_grid_lon)
  where is_active;

comment on column public.beaches_gold.marine_grid_lat is
  'Cached 0.1° bin-floor latitude for marine_grid lookup. Auto-maintained by tg_compute_marine_grid_cell. Consumers JOIN marine_grid_hourly without a function call.';
comment on column public.beaches_gold.marine_grid_lon is
  'Cached 0.1° bin-floor longitude. Auto-maintained.';

-- ───────────────────────────────────────────────────────────────────────────
-- 2. Trigger function — computes cached cols + registers cell
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public.tg_compute_marine_grid_cell()
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
    new.marine_grid_lat := public.marine_grid_bin_lat(new.lat);
    new.marine_grid_lon := public.marine_grid_bin_lon(new.lon);

    -- Auto-register the cell (idempotent). Loader will discover whether
    -- the cell has marine data when it tries the API; inland cells just
    -- stay empty in marine_grid_hourly.
    insert into public.marine_grid (grid_lat, grid_lon, geom, state)
    values (
      new.marine_grid_lat,
      new.marine_grid_lon,
      st_setsrid(st_makepoint(new.marine_grid_lon::float, new.marine_grid_lat::float), 4326)::geography,
      new.state
    )
    on conflict (grid_lat, grid_lon) do nothing;
  end if;

  return new;
end $$;

comment on function public.tg_compute_marine_grid_cell is
  'Trigger fn on beaches_gold. Computes cached marine_grid_lat/lon from lat/lon AND auto-registers the cell in marine_grid via INSERT ON CONFLICT DO NOTHING. Per [[marine-grid-reference-layer]].';

-- ───────────────────────────────────────────────────────────────────────────
-- 3. Trigger on beaches_gold (no dog_parks — they don't need marine)
-- ───────────────────────────────────────────────────────────────────────────

drop trigger if exists tg_beaches_gold_marine_grid on public.beaches_gold;
create trigger tg_beaches_gold_marine_grid
  before insert or update on public.beaches_gold
  for each row execute function public.tg_compute_marine_grid_cell();

-- ───────────────────────────────────────────────────────────────────────────
-- 4. Backfill — populate cached cols + register cells for existing beaches
-- ───────────────────────────────────────────────────────────────────────────
--
-- Hit only coastal MVP+ states to avoid registering thousands of inland
-- cells the loader would discover are empty. Other states get auto-registered
-- naturally if their beaches are scoreable and get hit by future updates.

update public.beaches_gold
   set marine_grid_lat = public.marine_grid_bin_lat(lat),
       marine_grid_lon = public.marine_grid_bin_lon(lon)
 where state in ('CA','OR','WA','MA','RI','NH','MD','VA','DE','AL')
   and is_active = true
   and scoring_tier in ('daily','hourly')
   and lat is not null and lon is not null;

-- Inventory rebuild from the updated cached cols.
insert into public.marine_grid (grid_lat, grid_lon, geom, state)
select distinct
  bg.marine_grid_lat,
  bg.marine_grid_lon,
  st_setsrid(st_makepoint(bg.marine_grid_lon::float, bg.marine_grid_lat::float), 4326)::geography,
  bg.state
from public.beaches_gold bg
where bg.state in ('CA','OR','WA','MA','RI','NH','MD','VA','DE','AL')
  and bg.is_active = true
  and bg.scoring_tier in ('daily','hourly')
  and bg.marine_grid_lat is not null
on conflict (grid_lat, grid_lon) do nothing;

commit;
