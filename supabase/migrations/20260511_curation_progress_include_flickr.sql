-- Include flickr (and unsplash) in the curation queue's photo_count.
-- The 2026-05-09 version of get_curation_progress hardcoded source IN ('ccc','wikimedia').
-- After the 2026-05-11 Flickr ingest, flickr-only beaches were invisible in the queue.
-- Also: redefine is_curated as "no uncurated photos remain" rather than the existence
-- of any beach_curation_status row, so newly-arrived photos automatically re-flag
-- the beach for curation.

create or replace function public.get_curation_progress(p_state text default 'CA')
returns jsonb
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
                and bp.source in ('ccc','wikimedia','flickr','unsplash')) as photo_count,
           -- is_curated = a status row exists AND no photos arrived after the status timestamp
           coalesce(
             (s.arena_group_id is not null)
             and not exists (
               select 1 from public.beach_photos bp2
                where bp2.arena_group_id = g.fid
                  and bp2.source in ('ccc','wikimedia','flickr','unsplash')
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
