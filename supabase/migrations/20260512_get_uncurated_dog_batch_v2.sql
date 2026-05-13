-- Fix: push the g.is_active filter INSIDE the inner subquery so we pick
-- 6 photos at ACTIVE beaches, not 6 photos that may belong to inactive
-- beaches (then get dropped by the post-join is_active filter, returning
-- an empty batch). Bug surfaced 2026-05-12 evening: count() said 27
-- uncurated dog photos but batch() returned 0.

create or replace function public.get_uncurated_dog_batch(
  p_limit int default 6, p_offset int default 0
)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  with all_dogs as (
    select bp.id, bp.arena_group_id, bp.source, bp.external_id,
           bp.image_url, bp.thumb_url, bp.attribution, bp.license,
           bp.page_url, bp.distance_m, bp.sort_order, bp.source_meta,
           coalesce((bp.source_meta->>'predicted_keep_prob')::float, 0) keep_prob,
           coalesce(g.display_name_override, g.name) beach_name,
           g.state, g.county_name
      from public.beach_photos bp
      join public.beaches_gold g on g.fid = bp.arena_group_id
     where bp.curated_at is null
       and g.is_active
       and (coalesce((bp.source_meta -> 'vision' ->> 'has_dog')::boolean, false)
            or coalesce(bp.source_meta->>'title','')
               ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M'
            or coalesce(bp.source_meta->>'description','')
               ~* '\m(dog|dogs|puppy|pup|pups|puppies|pooch|canine|hound|doggie|doggy)\M')
  ),
  -- Dedup by image identity (source, external_id). One representative
  -- row per unique image; sibling_ids carries the rest so curator
  -- decisions propagate. Eliminates CDPR-fan-out repeat-clicks (one
  -- gallery image attributed to many sub-beaches = same image shown
  -- N times in the batch before this fix).
  unique_imgs as (
    select distinct on (source, external_id) *,
           (select jsonb_agg(jsonb_build_object('id', d2.id, 'fid', d2.arena_group_id))
              from all_dogs d2
             where d2.source=all_dogs.source
               and d2.external_id=all_dogs.external_id) sibling_rows
      from all_dogs
     order by source, external_id, keep_prob desc
  ),
  picked as (
    select * from unique_imgs
     order by keep_prob desc, id
     offset p_offset
     limit p_limit
  )
  select coalesce(jsonb_agg(jsonb_build_object(
           'id',          id,
           'arena_group_id', arena_group_id,
           'sibling_rows', sibling_rows,
           'beach_name',  beach_name,
           'beach_state', state,
           'beach_county', county_name,
           'source',      source,
           'image_url',   image_url,
           'thumb_url',   thumb_url,
           'attribution', attribution,
           'license',     license,
           'page_url',    page_url,
           'distance_m',  distance_m,
           'sort_order',  sort_order,
           'source_meta', source_meta,
           'keep_prob',   keep_prob
         ) order by keep_prob desc, id), '[]'::jsonb)
    from picked;
$$;
