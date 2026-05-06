-- 20260506_arena_to_gold_auto_promote.sql
--
-- Auto-promote arena rows to beaches_gold when:
--   - state would resolve to CA (county_name is a CA county)
--   - active + has nav_lat/nav_lon
--   - source_code in trusted set (poi = us_beach_points, manual)
--   - not already in beaches_gold
--
-- Conservative on purpose. OSM-sourced arena rows include parks, dog
-- parks, and other non-beach polygons; CCC rows are access points,
-- not beaches. POI (us_beach_points) is the cleanest source for
-- auto-promotion.
--
-- The companion is_scoreable trigger and the gold-insert chain
-- trigger already do the rest of the cascade automatically.

begin;

-- The 5 SoCal counties are the only ones currently scored.
-- Other CA counties auto-promote to gold but stay is_scoreable=false
-- (per the auto-scoreable trigger's filter).
create or replace function public.tg_arena_auto_promote_to_gold()
returns trigger
language plpgsql
as $$
declare
  v_state text;
begin
  -- Skip if not a trusted source
  if NEW.source_code not in ('poi', 'manual') then return NEW; end if;
  -- Skip if no coords
  if NEW.nav_lat is null or NEW.nav_lon is null then return NEW; end if;
  -- Skip if not active
  if NEW.is_active is not true then return NEW; end if;
  -- Skip if already in gold
  if exists (select 1 from public.beaches_gold where fid = NEW.fid) then
    return NEW;
  end if;

  -- Derive state from county. Only CA today; expand later.
  v_state := case
    when NEW.county_name in (
      'Los Angeles','Orange','San Diego','Santa Barbara','Ventura',
      'San Mateo','Marin','San Francisco','Sonoma','Mendocino','Humboldt',
      'Del Norte','Monterey','Santa Cruz','San Luis Obispo','Alameda',
      'Contra Costa','Solano','Napa','Lake'
    ) then 'CA'
    else null
  end;
  if v_state is null then return NEW; end if;

  insert into public.beaches_gold
    (fid, name, lat, lon, county_name, state, geom, source_code,
     location_id, promoted_from, promoted_at, is_active)
  values
    (NEW.fid, NEW.name, NEW.nav_lat, NEW.nav_lon, NEW.county_name,
     v_state, NEW.geom, NEW.source_code,
     lower(regexp_replace(NEW.name, '[^a-zA-Z0-9]+', '-', 'g')),
     'arena_auto_promote', now(), true)
  on conflict (fid) do nothing;

  return NEW;
end $$;

drop trigger if exists tg_after_change_arena_auto_promote on public.arena;
create trigger tg_after_change_arena_auto_promote
  after insert or update on public.arena
  for each row
  when (NEW.is_active is true and NEW.source_code in ('poi','manual'))
  execute function public.tg_arena_auto_promote_to_gold();

commit;
