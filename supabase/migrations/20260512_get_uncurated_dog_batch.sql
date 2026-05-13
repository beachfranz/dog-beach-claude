-- Batch fetch of uncurated dog photos across all beaches, for the
-- focused dog-curation pass UI (curate-dogs.html). Returns up to N
-- photos with beach context so the curator can decide without
-- per-beach navigation. Sorted by predicted_keep_prob desc so the
-- highest-confidence candidates come first.

create or replace function public.get_uncurated_dog_batch(
  p_limit int default 6, p_offset int default 0
)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  select coalesce(jsonb_agg(jsonb_build_object(
           'id',          bp.id,
           'arena_group_id', bp.arena_group_id,
           'beach_name',  coalesce(g.display_name_override, g.name),
           'beach_state', g.state,
           'beach_county', g.county_name,
           'source',      bp.source,
           'image_url',   bp.image_url,
           'thumb_url',   bp.thumb_url,
           'attribution', bp.attribution,
           'license',     bp.license,
           'page_url',    bp.page_url,
           'distance_m',  bp.distance_m,
           'sort_order',  bp.sort_order,
           'source_meta', bp.source_meta,
           'keep_prob',   coalesce((bp.source_meta->>'predicted_keep_prob')::float, 0)
         ) order by coalesce((bp.source_meta->>'predicted_keep_prob')::float, 0) desc,
                    bp.id asc), '[]'::jsonb)
    from (
      select bp.*
        from public.beach_photos bp
       where bp.curated_at is null
         and (coalesce((bp.source_meta -> 'vision' ->> 'has_dog')::boolean, false)
              or coalesce(bp.source_meta->>'title','')
                 ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M'
              or coalesce(bp.source_meta->>'description','')
                 ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M')
       order by coalesce((bp.source_meta->>'predicted_keep_prob')::float, 0) desc, bp.id asc
       offset p_offset
       limit p_limit
    ) bp
    join public.beaches_gold g on g.fid = bp.arena_group_id
   where g.is_active;
$$;

-- Companion: total count for progress indicator
create or replace function public.get_uncurated_dog_count()
returns int
language sql
stable
security definer
set search_path to 'public'
as $$
  select count(*)::int
    from public.beach_photos bp
    join public.beaches_gold g on g.fid = bp.arena_group_id
   where bp.curated_at is null
     and g.is_active
     and (coalesce((bp.source_meta -> 'vision' ->> 'has_dog')::boolean, false)
          or coalesce(bp.source_meta->>'title','')
             ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M'
          or coalesce(bp.source_meta->>'description','')
             ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M');
$$;
