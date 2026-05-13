-- Per-beach NPS curation batch. Returns ONE beach's top N uncurated
-- NPS photos at a time. Curator works beach-by-beach (decides 8 photos,
-- saves, advances to next NPS beach with un-curated photos).
--
-- Picks the beach with the most uncurated NPS photos first → covers
-- the heavy-fanout parks (where one image is attributed to many sub-
-- beaches) most efficiently.

create or replace function public.get_uncurated_nps_beach(
  p_limit int default 8
)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  with target_beach as (
    select bp.arena_group_id fid, count(*) n_uncurated
      from public.beach_photos bp
      join public.beaches_gold g on g.fid = bp.arena_group_id
     where bp.source = 'nps' and bp.curated_at is null and g.is_active
     group by bp.arena_group_id
     order by count(*) desc, bp.arena_group_id
     limit 1
  ),
  beach_meta as (
    select g.fid, coalesce(g.display_name_override, g.name) name,
           g.state, g.county_name
      from beaches_gold g
      join target_beach t on t.fid = g.fid
  ),
  photos as (
    select bp.id, bp.arena_group_id, bp.source,
           bp.image_url, bp.thumb_url, bp.attribution, bp.license,
           bp.page_url, bp.distance_m, bp.sort_order, bp.source_meta,
           bp.source_meta->>'park_code' park_code,
           coalesce((bp.source_meta->>'predicted_keep_prob')::float, 0) keep_prob
      from public.beach_photos bp
      join target_beach t on t.fid = bp.arena_group_id
     where bp.source = 'nps' and bp.curated_at is null
     order by keep_prob desc, bp.sort_order, bp.id
     limit p_limit
  )
  select jsonb_build_object(
    'beach', (select to_jsonb(b.*) from beach_meta b),
    'total_remaining', (select count(*)::int from beach_photos
                         join beaches_gold g on g.fid=beach_photos.arena_group_id
                        where source='nps' and curated_at is null and g.is_active),
    'beaches_remaining', (select count(*)::int from (
                           select 1 from beach_photos
                            join beaches_gold g on g.fid=beach_photos.arena_group_id
                            where source='nps' and curated_at is null and g.is_active
                            group by beach_photos.arena_group_id
                          ) s),
    'photos', coalesce((
      select jsonb_agg(jsonb_build_object(
        'id', id, 'arena_group_id', arena_group_id, 'source', source,
        'image_url', image_url, 'thumb_url', thumb_url,
        'attribution', attribution, 'license', license, 'page_url', page_url,
        'distance_m', distance_m, 'sort_order', sort_order,
        'source_meta', source_meta, 'park_code', park_code,
        'keep_prob', keep_prob
      ) order by keep_prob desc, sort_order, id)
      from photos), '[]'::jsonb)
  );
$$;

create or replace function public.get_uncurated_nps_count()
returns int
language sql stable security definer set search_path to 'public'
as $$
  select count(*)::int
    from public.beach_photos bp
    join public.beaches_gold g on g.fid = bp.arena_group_id
   where bp.source='nps' and bp.curated_at is null and g.is_active;
$$;
