-- Generalize wikimedia_blocked_photographers to accept a source parameter,
-- so flickr (and future sources) can use the same auto-blocklist mechanism.
-- Wikimedia keeps its specific name as a passthrough for backward compat.

create or replace function public.beach_photo_blocked_photographers(p_source text default 'wikimedia')
returns table(artist text, photo_count bigint, mean_relevance numeric)
language sql
stable
as $$
  select source_meta->>'artist'                                       as artist,
         count(*)::bigint                                              as photo_count,
         round(avg((source_meta->>'relevance_score')::numeric), 2)    as mean_relevance
    from public.beach_photos
   where source = p_source
     and source_meta ? 'artist'
     and source_meta ? 'relevance_score'
     and source_meta->>'artist' is not null
     and trim(source_meta->>'artist') <> ''
   group by 1
  having count(*) >= 3
     and avg((source_meta->>'relevance_score')::numeric) < 0.0
   order by mean_relevance asc, photo_count desc;
$$;

-- Keep the original wikimedia_blocked_photographers wrapper for the legacy caller.
create or replace function public.wikimedia_blocked_photographers()
returns table(artist text, photo_count bigint, mean_relevance numeric)
language sql
stable
as $$
  select * from public.beach_photo_blocked_photographers('wikimedia');
$$;
