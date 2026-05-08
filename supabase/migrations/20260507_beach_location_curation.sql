-- 20260507_beach_location_curation.sql
--
-- Per-beach completion tracker for the location-curation tool
-- (curate-locations.html). Mirror of beach_curation_status (photos)
-- but for marker drag-and-drop on the OSM map.
--
-- A row's presence = "the curator looked at this beach and either
-- moved the marker or confirmed it's correct".

begin;

create table if not exists public.beach_location_curation_status (
  arena_group_id  bigint primary key references public.beaches_gold(fid) on delete cascade,
  curated_at      timestamptz default now(),
  curated_by      text,
  was_moved       boolean default false,    -- did they actually move the marker?
  before_lat      double precision,
  before_lng      double precision,
  after_lat       double precision,
  after_lng       double precision,
  distance_moved_m int,
  notes           text
);

create index if not exists beach_location_curation_was_moved_idx
  on public.beach_location_curation_status(was_moved);

alter table public.beach_location_curation_status enable row level security;
create policy beach_location_curation_anon_read on public.beach_location_curation_status
  for select to anon, authenticated using (true);

-- Progress RPC. Scoped by state + counties array for the curator UI.
create or replace function public.get_location_curation_progress(
  p_state    text default 'CA',
  p_counties text[] default null    -- null = all counties in state
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
           s.was_moved
      from public.beaches_gold g
      left join public.beach_location_curation_status s on s.arena_group_id = g.fid
     where g.is_active and g.state = p_state
       and (p_counties is null or g.county_name = any(p_counties))
       and g.geom is not null
  ),
  per_county as (
    select county_name,
           count(*) as total,
           count(*) filter (where is_curated)  as curated,
           count(*) filter (where was_moved)   as moved,
           jsonb_agg(jsonb_build_object(
             'fid',        fid,
             'name',       name,
             'lat',        lat,
             'lng',        lng,
             'is_curated', is_curated,
             'was_moved',  was_moved
           ) order by name) as beaches
      from beaches
     group by county_name
  )
  select jsonb_agg(jsonb_build_object(
    'county',  county_name,
    'total',   total,
    'curated', curated,
    'moved',   moved,
    'is_done', curated >= total and total > 0,
    'beaches', beaches
  ) order by county_name)
  from per_county where county_name is not null;
$$;

grant execute on function public.get_location_curation_progress(text, text[])
  to anon, authenticated, service_role;

commit;
