-- 20260509_wikimedia_blocked_photographers.sql
--
-- Photographer-blocklist for Wikimedia Commons loader. After photos
-- are loaded with relevance scores in source_meta, this function
-- aggregates by artist and returns photographers whose track record
-- suggests their photos aren't useful for our beach UI:
--   ≥3 photos in our DB AND mean relevance_score ≤ 0.0
--
-- The Commons loader queries this at start of each run and skips
-- photos by these artists in rank_and_pick. Self-correcting feedback
-- loop — first run fills the dataset, second run filters out the
-- specimen-photo / wildlife-app contributors.

begin;

create or replace function public.wikimedia_blocked_photographers()
returns table(artist text, photo_count bigint, mean_relevance numeric)
language sql
stable
as $function$
  select source_meta->>'artist' as artist,
         count(*)::bigint as photo_count,
         round(avg((source_meta->>'relevance_score')::numeric), 2) as mean_relevance
    from public.beach_photos
   where source = 'wikimedia'
     and source_meta ? 'artist'
     and source_meta ? 'relevance_score'
     and source_meta->>'artist' is not null
     and trim(source_meta->>'artist') <> ''
   group by 1
  having count(*) >= 3
     and avg((source_meta->>'relevance_score')::numeric) < 0.0
   order by mean_relevance asc, photo_count desc;
$function$;

grant execute on function public.wikimedia_blocked_photographers() to anon, authenticated, service_role;

commit;
