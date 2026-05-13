-- Add cdpr + nps to the hardcoded source whitelist in the curation RPCs.
-- Both new sources landed in beach_photos today but the curation
-- progress + states RPCs filtered them out, surfacing only 0/few photos
-- in curate.html when source dropdown was set to cdpr or nps.


CREATE OR REPLACE FUNCTION public.get_curation_states()
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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
$function$;

CREATE OR REPLACE FUNCTION public.get_curation_states(p_source text DEFAULT 'all'::text)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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
         p_source = 'all' and bp.source in ('ccc','wikimedia','flickr','unsplash','cdpr','nps')
         or bp.source = p_source
       )
     group by g.state
  ) s;
$function$;

CREATE OR REPLACE FUNCTION public.get_curation_progress(p_state text DEFAULT 'CA'::text)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with beaches as (
    select g.fid,
           coalesce(g.display_name_override, g.name) as name,
           g.county_name, g.state,
           (select count(*) from public.beach_photos bp
              where bp.arena_group_id = g.fid
                and bp.source in ('ccc','wikimedia','flickr','unsplash','cdpr','nps')) as photo_count,
           -- is_curated = a status row exists AND no photos arrived after the status timestamp
           coalesce(
             (s.arena_group_id is not null)
             and not exists (
               select 1 from public.beach_photos bp2
                where bp2.arena_group_id = g.fid
                  and bp2.source in ('ccc','wikimedia','flickr','unsplash','cdpr','nps')
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
$function$;

CREATE OR REPLACE FUNCTION public.get_curation_progress(p_state text DEFAULT 'CA'::text, p_source text DEFAULT 'all'::text)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with beaches as (
    select g.fid,
           coalesce(g.display_name_override, g.name) as name,
           g.county_name, g.state,
           (select count(*) from public.beach_photos bp
              where bp.arena_group_id = g.fid
                and (
                  p_source = 'all' and bp.source in ('ccc','wikimedia','flickr','unsplash','cdpr','nps')
                  or bp.source = p_source
                )) as photo_count,
           -- is_curated = a status row exists AND no photos arrived after the status timestamp
           coalesce(
             (s.arena_group_id is not null)
             and not exists (
               select 1 from public.beach_photos bp2
                where bp2.arena_group_id = g.fid
                  and (
                    p_source = 'all' and bp2.source in ('ccc','wikimedia','flickr','unsplash','cdpr','nps')
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
           -- Franz's 2026-05-09 preference â€” zero-photo beaches stay visible
           -- so the curator can still navigate to them).
           -- When source != 'all': filter to only photo'd beaches in that
           -- source â€” that's the source-cascade UX.
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
$function$;
