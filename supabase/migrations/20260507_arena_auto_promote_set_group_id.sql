-- 20260507_arena_auto_promote_set_group_id.sql
--
-- Defensive fix: tg_arena_auto_promote_to_gold inserts a beaches_gold
-- stub on every arena INSERT (when source_code in poi/manual) but did
-- not set group_id. That left every auto-promoted row with NULL
-- group_id, which silently broke every downstream consumer-table join
-- keyed on `arena_group_id = beaches_gold.group_id` (most importantly
-- the `beach_dog_policy` and `beach_amenities` joins).
--
-- This is paired with a seed_arena_beach.py edit that switches its
-- INSERT to ON CONFLICT (fid) DO UPDATE so the seeder's full payload
-- (location_id with county slug, noaa_station_id, timezone, open/close,
-- park_name) wins over the trigger's minimal stub. Either fix alone
-- closes the bug; both together makes it robust.

begin;

create or replace function public.tg_arena_auto_promote_to_gold()
returns trigger
language plpgsql
as $function$
declare
  v_state text;
begin
  if NEW.source_code not in ('poi', 'manual') then return NEW; end if;
  if NEW.nav_lat is null or NEW.nav_lon is null then return NEW; end if;
  if NEW.is_active is not true then return NEW; end if;
  if exists (select 1 from public.beaches_gold where fid = NEW.fid) then
    return NEW;
  end if;

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
     group_id, location_id, promoted_from, promoted_at, is_active)
  values
    (NEW.fid, NEW.name, NEW.nav_lat, NEW.nav_lon, NEW.county_name,
     v_state, NEW.geom, NEW.source_code,
     coalesce(NEW.group_id, NEW.fid),
     lower(regexp_replace(NEW.name, '[^a-zA-Z0-9]+', '-', 'g')),
     'arena_auto_promote', now(), true)
  on conflict (fid) do nothing;

  return NEW;
end
$function$;

-- Backfill any pre-existing rows still missing group_id (defensive —
-- catches the 4 SoCal seeds patched manually tonight + any other
-- historical rows where the trigger fired before this fix).
update public.beaches_gold
   set group_id = fid
 where group_id is null;

commit;
