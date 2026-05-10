-- 20260509_wikimedia_blocked_photographers_v2.sql
--
-- Refines the blocklist: a photographer is blocked only if they have
-- ≥3 photos, mean relevance < 0, AND no individual photo scored
-- positive. Even one good photo (rel > 0) is enough to keep them in
-- the contributor pool. Franz: "is one positive enough?"
--
-- Rationale: a photographer who occasionally captures a great dog-beach
-- shot adds value even if their other photos are specimen/wildlife
-- contributions. We don't want to lose the gem to filter the noise.

begin;

create or replace function public.wikimedia_blocked_photographers()
returns table(artist text, photo_count bigint, mean_relevance numeric, max_relevance numeric)
language sql
stable
as $function$
  select source_meta->>'artist' as artist,
         count(*)::bigint as photo_count,
         round(avg((source_meta->>'relevance_score')::numeric), 2) as mean_relevance,
         round(max((source_meta->>'relevance_score')::numeric), 2) as max_relevance
    from public.beach_photos
   where source = 'wikimedia'
     and source_meta ? 'artist'
     and source_meta ? 'relevance_score'
     and source_meta->>'artist' is not null
     and trim(source_meta->>'artist') <> ''
   group by 1
  having count(*) >= 3
     and avg((source_meta->>'relevance_score')::numeric) < 0.0
     and max((source_meta->>'relevance_score')::numeric) <= 0.0
   order by mean_relevance asc, photo_count desc;
$function$;

grant execute on function public.wikimedia_blocked_photographers() to anon, authenticated, service_role;

commit;
