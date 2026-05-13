-- "No photos" filter for curate.html. Mirrors get_dog_photo_fids
-- semantics: returns the set of beach fids that have NO photos in
-- the currently-selected source (or any source when p_source is null).
--
-- Respects the existing source dropdown — when source=nps is selected,
-- the no-photos checkbox surfaces beaches with no NPS photos
-- (regardless of whether they have flickr/wikimedia photos). When
-- source=all (default), it surfaces beaches with zero photos at all.

create or replace function public.get_no_photo_fids(
  p_source text default null
)
returns table(fid bigint)
language sql
stable
security definer
set search_path to 'public'
as $$
  select g.fid
    from public.beaches_gold g
   where g.is_active
     and g.is_scoreable
     and not exists (
       select 1 from public.beach_photos bp
        where bp.arena_group_id = g.fid
          and (p_source is null or p_source = 'all' or bp.source = p_source)
     );
$$;
