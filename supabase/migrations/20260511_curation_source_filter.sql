-- Add a source filter that cascades through states / counties / beaches in
-- the curate UI. Pass p_source='all' for the legacy aggregated view (default);
-- pass a specific source ('wikimedia', 'flickr', 'ccc', 'unsplash') to restrict.

create or replace function public.get_curation_states(p_source text default 'all')
returns jsonb
language sql
stable security definer
set search_path to 'public'
as $$
  select jsonb_agg(jsonb_build_object(
    'state',               state,
    'beaches_with_photos', beaches,
    'photos',              photos
  ) order by photos desc)
  from (
    select g.state,
           count(distinct g.fid)::int as beaches,
           count(*)::int              as photos
      from public.beaches_gold g
      join public.beach_photos bp on bp.arena_group_id = g.fid
     where g.is_active
       and (
         p_source = 'all' and bp.source in ('ccc','wikimedia','flickr','unsplash')
         or bp.source = p_source
       )
     group by g.state
  ) s;
$$;

create or replace function public.get_curation_progress(
  p_state  text default 'CA',
  p_source text default 'all'
) returns jsonb
language sql
stable security definer
set search_path to 'public'
as $$
  with beaches as (
    select g.fid,
           coalesce(g.display_name_override, g.name) as name,
           g.county_name, g.state,
           (select count(*) from public.beach_photos bp
              where bp.arena_group_id = g.fid
                and (
                  p_source = 'all' and bp.source in ('ccc','wikimedia','flickr','unsplash')
                  or bp.source = p_source
                )) as photo_count,
           -- is_curated = a status row exists AND no photos arrived after the status timestamp
           coalesce(
             (s.arena_group_id is not null)
             and not exists (
               select 1 from public.beach_photos bp2
                where bp2.arena_group_id = g.fid
                  and (
                    p_source = 'all' and bp2.source in ('ccc','wikimedia','flickr','unsplash')
                    or bp2.source = p_source
                  )
                  and bp2.loaded_at > s.curated_at
             ),
             false
           ) as is_curated
      from public.beaches_gold g
      left join public.beach_curation_status s on s.arena_group_id = g.fid
     where g.is_active and g.state = p_state
  ),
  per_county as (
    select county_name, state,
           count(*) as total,
           count(*) filter (where photo_count > 0)                  as with_photos,
           count(*) filter (where is_curated and photo_count > 0)   as curated,
           -- When source='all': include every beach in the county (preserves
           -- Franz's 2026-05-09 preference — zero-photo beaches stay visible
           -- so the curator can still navigate to them).
           -- When source != 'all': filter to only photo'd beaches in that
           -- source — that's the source-cascade UX.
           jsonb_agg(jsonb_build_object(
             'fid',         fid,
             'name',        name,
             'photo_count', photo_count,
             'is_curated',  is_curated
           ) order by name) filter (
             where p_source = 'all' or photo_count > 0
           ) as beaches
      from beaches
     group by county_name, state
    having count(*) filter (where photo_count > 0) > 0
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
