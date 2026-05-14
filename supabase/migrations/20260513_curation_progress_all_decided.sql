-- 20260513_curation_progress_all_decided.sql
--
-- Extends is_curated semantics in get_curation_progress so a beach is
-- considered "done" when every loader-sourced photo is either curated
-- (curated_at != null) or hidden (hidden_at != null) — i.e. nothing
-- left for the curator to decide. Previously, is_curated required an
-- explicit beach_curation_status upsert; for beaches whose only photos
-- are curator-promoted from sibling-beach propagation, the curator
-- never visited the page, so is_curated stayed false even though
-- there's no work left.
--
-- New is_curated logic:
--   path A: beach_curation_status row exists AND no photos arrived
--           after that status timestamp (legacy semantics)
--   OR
--   path B: photo_count > 0 AND no photo is pending (curated_at IS NULL
--           AND hidden_at IS NULL) for the relevant source set
--
-- This drops auto-done beaches out of the curator's queue.

begin;

create or replace function public.get_curation_progress(
  p_state text default 'CA',
  p_source text default 'all'
) returns jsonb
language sql stable security definer
set search_path to 'public'
as $function$
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
           -- is_curated = path A OR path B (see migration header)
           coalesce(
             -- Path A: explicit beach_curation_status upsert + no new photos
             (
               (s.arena_group_id is not null)
               and not exists (
                 select 1 from public.beach_photos bp2
                  where bp2.arena_group_id = g.fid
                    and (
                      p_source = 'all' and bp2.source in ('ccc','wikimedia','flickr','unsplash','cdpr','nps')
                      or bp2.source = p_source
                    )
                    and bp2.loaded_at > s.curated_at
               )
             )
             OR
             -- Path B: at least one photo, AND every photo is curated_at
             --         or hidden_at (nothing pending decision).
             (
               exists (
                 select 1 from public.beach_photos bp_any
                  where bp_any.arena_group_id = g.fid
                    and (
                      p_source = 'all' and bp_any.source in ('ccc','wikimedia','flickr','unsplash','cdpr','nps')
                      or bp_any.source = p_source
                    )
               )
               AND not exists (
                 select 1 from public.beach_photos bp3
                  where bp3.arena_group_id = g.fid
                    and (
                      p_source = 'all' and bp3.source in ('ccc','wikimedia','flickr','unsplash','cdpr','nps')
                      or bp3.source = p_source
                    )
                    and bp3.curated_at is null
                    and bp3.hidden_at is null
               )
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

commit;
