-- 20260509_curate_state_dropdown.sql
--
-- Three changes for the photo curator (Franz, 2026-05-09):
--   1. Delete all Mapillary photos. They were street-level imagery; we
--      now standardize on Wikimedia Commons + CCC for the curate UI.
--   2. get_curation_progress(state) — accepts a state arg so the curator
--      can switch states. Counts only ccc + wikimedia photos.
--   3. get_curation_states() — lists states that have curate-able
--      photos, used to populate the new state dropdown.

begin;

-- 1. Drop Mapillary
delete from public.beach_photos where source = 'mapillary';

-- 2. State-aware progress fn (replaces no-arg version which was hardcoded to CA)
drop function if exists public.get_curation_progress();

create or replace function public.get_curation_progress(p_state text default 'CA')
returns jsonb language sql stable security definer set search_path to public as $$
  with beaches as (
    select g.fid, coalesce(g.display_name_override, g.name) as name,
           g.county_name, g.state,
           (select count(*) from public.beach_photos bp
              where bp.arena_group_id = g.fid
                and bp.source in ('ccc','wikimedia')) as photo_count,
           (s.arena_group_id is not null) as is_curated
      from public.beaches_gold g
      left join public.beach_curation_status s on s.arena_group_id = g.fid
     where g.is_active and g.state = p_state
  ),
  per_county as (
    select county_name, state,
           count(*) as total,
           count(*) filter (where photo_count > 0) as with_photos,
           count(*) filter (where is_curated)      as curated,
           jsonb_agg(jsonb_build_object(
             'fid',         fid,
             'name',        name,
             'photo_count', photo_count,
             'is_curated',  is_curated
           ) order by name) as beaches
      from beaches
     group by county_name, state
  )
  select jsonb_agg(jsonb_build_object(
    'county',      county_name,
    'state',       state,
    'total',       total,
    'with_photos', with_photos,
    'curated',     curated,
    'is_done',     curated >= with_photos and with_photos > 0,
    'beaches',     beaches
  ) order by county_name)
  from per_county
  where county_name is not null;
$$;

grant execute on function public.get_curation_progress(text) to anon, authenticated, service_role;

-- 3. State-list helper for the dropdown
create or replace function public.get_curation_states()
returns jsonb language sql stable security definer set search_path to public as $$
  select jsonb_agg(jsonb_build_object(
    'state',                 state,
    'beaches_with_photos',   beaches,
    'photos',                photos
  ) order by photos desc)
  from (
    select g.state,
           count(distinct g.fid)::int as beaches,
           count(*)::int             as photos
      from public.beaches_gold g
      join public.beach_photos bp on bp.arena_group_id = g.fid
     where g.is_active
       and bp.source in ('ccc','wikimedia')
     group by g.state
  ) s;
$$;

grant execute on function public.get_curation_states() to anon, authenticated, service_role;

commit;
