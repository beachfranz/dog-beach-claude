-- Standalone photo-fetch RPC, shared between get_beach_info (beach.html)
-- and get-beach-detail edge function (detail.html). Uses option B filter:
-- curated-first, model-fallback at predicted_keep_prob >= 0.65. Cap 6.
--
-- FOR LAUNCH (option C): un-comment the source='cdpr' lines to add
-- CDPR park gallery photos as a middle tier between curated and
-- model-scored. (See 20260512_get_beach_info_photo_filter.sql.)

create or replace function public.get_beach_photos_curated(p_fid bigint)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  with selected as (
    select p.source, p.image_url, p.thumb_url, p.attribution, p.license,
           p.page_url, p.captured_at, p.curated_at, p.sort_order, p.id,
           coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) keep_prob
      from public.beach_photos p
     where p.arena_group_id = p_fid
       and (
         p.curated_at is not null
         -- or p.source = 'cdpr'   -- UNCOMMENT FOR LAUNCH (option C)
         or coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) >= 0.65
       )
     order by
       (p.curated_at is null)::int asc,
       -- case when p.source='cdpr' then 0 else 1 end,  -- UNCOMMENT FOR LAUNCH
       coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) desc,
       p.sort_order asc,
       p.id asc
     limit 6
  )
  select coalesce(jsonb_agg(jsonb_build_object(
           'source',      source,
           'image_url',   image_url,
           'thumb_url',   thumb_url,
           'attribution', attribution,
           'license',     license,
           'page_url',    page_url,
           'captured_at', captured_at,
           'curated',     curated_at is not null,
           'keep_prob',   keep_prob
         ) order by (curated_at is null)::int, keep_prob desc, sort_order, id), '[]'::jsonb)
    from selected;
$$;
