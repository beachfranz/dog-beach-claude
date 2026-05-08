-- 20260507_curation_needs_review.sql
--
-- Adds a `needs_review` flag to beach_location_curation_status for
-- the curator's "I'm not sure, please double-check" path. Mirrors the
-- skip/inactive button group on curate-locations.html.

begin;

alter table public.beach_location_curation_status
  add column if not exists needs_review boolean default false;

create index if not exists beach_location_curation_needs_review_idx
  on public.beach_location_curation_status(needs_review)
  where needs_review = true;

-- Update progress RPC to surface needs_review per beach + per-county count.
create or replace function public.get_location_curation_progress(
  p_state    text default 'CA',
  p_counties text[] default null
)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  with beaches as (
    select g.fid, coalesce(g.display_name_override, g.name) as name,
           g.county_name,
           st_y(g.geom) as lat,
           st_x(g.geom) as lng,
           (s.arena_group_id is not null) as is_curated,
           coalesce(s.was_moved, false) as was_moved,
           coalesce(s.needs_review, false) as needs_review
      from public.beaches_gold g
      left join public.beach_location_curation_status s on s.arena_group_id = g.fid
     where g.is_active and g.state = p_state
       and (p_counties is null or g.county_name = any(p_counties))
       and g.geom is not null
  ),
  per_county as (
    select county_name,
           count(*) as total,
           count(*) filter (where is_curated)    as curated,
           count(*) filter (where was_moved)     as moved,
           count(*) filter (where needs_review)  as needs_review,
           jsonb_agg(jsonb_build_object(
             'fid',          fid,
             'name',         name,
             'lat',          lat,
             'lng',          lng,
             'is_curated',   is_curated,
             'was_moved',    was_moved,
             'needs_review', needs_review
           ) order by name) as beaches
      from beaches
     group by county_name
  )
  select jsonb_agg(jsonb_build_object(
    'county',       county_name,
    'total',        total,
    'curated',      curated,
    'moved',        moved,
    'needs_review', needs_review,
    'is_done',      curated >= total and total > 0,
    'beaches',      beaches
  ) order by county_name)
  from per_county where county_name is not null;
$$;

grant execute on function public.get_location_curation_progress(text, text[])
  to anon, authenticated, service_role;

commit;
