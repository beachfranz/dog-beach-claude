-- Extend get_dog_photo_fids to include vision-detected dogs, not just
-- title-regex matches. Vision tag (source_meta.vision.has_dog) catches
-- dog photos with non-dog titles (e.g. iPhone export filenames — the
-- failure mode at fid 9838 id 8943, the collie at the Ballard Locks).
--
-- Used by curate.html to filter the beach dropdown when 🐶 Dog only is
-- toggled, surfacing only beaches with un-curated dog photos.

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
     and (
       -- Vision tag (the strong signal — pixels don't lie)
       coalesce((bp.source_meta -> 'vision' ->> 'has_dog')::boolean, false)
       or
       -- Title/description regex (catches photos not yet vision-tagged
       -- and acts as backup for vision misses)
       coalesce(bp.source_meta->>'title','')
         ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M'
       or coalesce(bp.source_meta->>'description','')
          ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M'
     );
$$;
