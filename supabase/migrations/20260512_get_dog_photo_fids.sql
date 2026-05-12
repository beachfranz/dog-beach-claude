-- Surfaces fids that currently have at least one uncurated photo whose title
-- (or description) contains a dog-related term. Used by the curate UI's
-- "🐶 Dog only" filter — surfaces the high-signal cohort for quick run-thru.

create or replace function public.get_dog_photo_fids()
returns table(fid bigint)
language sql
stable
security definer
set search_path to 'public'
as $$
  select distinct bp.arena_group_id
    from public.beach_photos bp
    join public.beaches_gold g on g.fid = bp.arena_group_id
   where g.is_active and bp.curated_at is null
     and (coalesce(bp.source_meta->>'title','')
          ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M'
       or coalesce(bp.source_meta->>'description','')
          ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M');
$$;
