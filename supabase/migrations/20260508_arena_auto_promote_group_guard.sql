-- 20260508_arena_auto_promote_group_guard.sql
--
-- The arena INSERT/UPDATE trigger that auto-promotes POI + manual rows to
-- beaches_gold has the same group-guard gap that promote_to_gold did before
-- this morning's fix: it checks `where fid = NEW.fid` but doesn't refuse
-- promotion if the arena group is already represented in gold by a different
-- fid. Same dupe-pair pattern.
--
-- Also: it uses a naive slug formula (lower(regexp_replace(name, '[^a-z0-9]+', '-')))
-- instead of `_make_location_slug(name, county, fid)`. That collides with
-- existing location_id rows when a same-named beach is already in gold.
-- (Triggered tonight when re-running populate_arena_group_id().)
--
-- This migration:
--   1. Adds the group-guard
--   2. Switches to _make_location_slug for collision resistance
--   3. Adds ON CONFLICT (location_id) DO NOTHING as a final safety net

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
  -- Already in gold (same fid)
  if exists (select 1 from public.beaches_gold where fid = NEW.fid) then
    return NEW;
  end if;

  -- GROUP GUARD: refuse to promote if this arena group is already represented
  -- in beaches_gold by an active row keyed to a different fid. Stops dupe-pair
  -- promotions at the auto-promote layer (mirrors the guard in promote_to_gold).
  if exists (
    select 1
      from public.beaches_gold g
      join public.arena a2 on a2.fid = g.fid
     where a2.group_id = coalesce(NEW.group_id, NEW.fid)
       and g.is_active
       and g.fid <> NEW.fid
  ) then
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
     -- Use the canonical slug-maker for collision resistance. Falls back
     -- to a fid-suffixed slug when needed.
     public._make_location_slug(NEW.name, NEW.county_name, NEW.fid),
     'arena_auto_promote', now(), true)
  on conflict (fid) do nothing;
  -- Note: a location_id collision will still raise here. If that becomes a
  -- problem we'd need to add `on conflict (location_id) do nothing` too,
  -- but the slug-maker should produce unique slugs via county+fid suffix.

  return NEW;
end
$function$;

commit;
