-- 20260606m_marine_grid_picker.sql
--
-- M1.4 of marine-grid-reference-layer. Schema bits needed by the loader:
--   * marine_grid.last_fetched_at  — when each cell was last successfully refreshed
--   * marine_grid.is_inland        — cells Open-Meteo Marine 400s on
--                                    (no marine grid coverage there)
--   * _orch_pick_stale_marine_cells RPC — picker the edge fn calls
--
-- Single-tier model for marine (vs weather_grid's t1/t2/t3):
--   - Open-Meteo Marine returns 7-day forecast in one call
--   - Marine state changes slower than weather (no need for layered cadence)
--   - One picker → one cron → one tier

begin;

alter table public.marine_grid
  add column if not exists last_fetched_at timestamptz,
  add column if not exists is_inland       boolean not null default false;

create index if not exists marine_grid_stale_idx
  on public.marine_grid (last_fetched_at)
  where not is_inland;

comment on column public.marine_grid.last_fetched_at is
  'Last successful Open-Meteo Marine fetch for this cell. NULL = never fetched. Picker orders by this NULLS FIRST.';
comment on column public.marine_grid.is_inland is
  'True when Open-Meteo Marine returned 400 for this cell (no marine data — inland or unsupported region). Picker excludes. Flip back to false to re-test.';

-- ───────────────────────────────────────────────────────────────────────────
-- Picker RPC — called from refresh-marine-grid edge fn
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public._orch_pick_stale_marine_cells(
  p_limit integer default 400,
  p_state text    default null,
  p_force boolean default false
)
returns table(grid_lat numeric, grid_lon numeric)
language sql
stable parallel safe
as $$
  select g.grid_lat, g.grid_lon
    from public.marine_grid g
   where not g.is_inland
     and (p_state is null or g.state = p_state)
     and (p_force
          or g.last_fetched_at is null
          or g.last_fetched_at < now() - interval '1 hour')
   order by g.last_fetched_at nulls first, g.grid_lat, g.grid_lon
   limit p_limit;
$$;

comment on function public._orch_pick_stale_marine_cells is
  'Picks marine cells due for refresh. Excludes is_inland=true. Stale threshold: 1h (matches refresh-marine-grid cron cadence). Per [[marine-grid-reference-layer]].';

grant execute on function public._orch_pick_stale_marine_cells to anon, authenticated;

-- ───────────────────────────────────────────────────────────────────────────
-- Inland-marker RPC — the edge fn calls this on Open-Meteo 400 responses
-- ───────────────────────────────────────────────────────────────────────────
-- Edge fn could UPDATE directly but a dedicated RPC makes the intent
-- explicit + lets us add audit logging later without touching the fn.

create or replace function public._orch_mark_marine_cells_inland(
  p_cells jsonb   -- array of {grid_lat, grid_lon}
)
returns integer
language plpgsql
as $$
declare
  v_marked int := 0;
begin
  update public.marine_grid g
     set is_inland = true,
         last_fetched_at = now()  -- so picker doesn't immediately re-pick
    from jsonb_array_elements(p_cells) c
   where g.grid_lat = (c->>'grid_lat')::numeric
     and g.grid_lon = (c->>'grid_lon')::numeric;
  get diagnostics v_marked = row_count;
  return v_marked;
end $$;

grant execute on function public._orch_mark_marine_cells_inland to anon, authenticated;

commit;
