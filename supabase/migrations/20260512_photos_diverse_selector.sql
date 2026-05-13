-- Diverse photo selector for public surfaces (beach.html + detail.html).
--
-- Replaces get_beach_photos_curated's straight top-N-by-score with a
-- variety-aware selection. Returns at most p_target photos per beach,
-- with these rules:
--
--   1. ALL has_dog=true photos pass through (capped at p_target).
--      Franz 2026-05-12: "dogs always stay."
--   2. Remaining slots filled by content bucket in priority order
--      (data-driven from per-bucket keep rate on 2,153 curator labels):
--        surf (84.5%) > landscape (79.7%) > people (67.8%) >
--        water (67.7%) > wide (66.7%) > atmosphere (53.3%) >
--        structure (48.3%, near coin-flip — last)
--      One photo per bucket (the highest-scored), avoid stacking duplicates.
--   3. Any slots still left after the bucket pass get filled with the
--      best leftovers by score.
--
-- Score = (curated_at IS NOT NULL, then predicted_keep_prob desc).
-- Eligibility same as get_beach_photos_curated (curated OR prob>=0.65).
--
-- Two consumers updated to call this:
--   - get_beach_info(fid) (beach.html via PostgREST RPC)
--   - get-beach-detail edge function (detail.html via RPC call)

create or replace function public.get_beach_photos_diverse(
  p_fid bigint, p_target int default 8
)
returns jsonb
language plpgsql
volatile          -- temp-table side effect; safe and session-local
security definer
set search_path to 'public'
as $$
declare
  v_result jsonb := '[]'::jsonb;
  v_picked_ids bigint[] := array[]::bigint[];
  v_bucket text;
  v_pick record;
  v_slots_left int := p_target;
begin
  -- Build the eligible-photo candidate table once with derived buckets.
  drop table if exists pg_temp._cand;
  create temp table _cand on commit drop as
  with eligible as (
    select p.id, p.source, p.image_url, p.thumb_url, p.attribution,
           p.license, p.page_url, p.captured_at, p.curated_at,
           p.sort_order, p.source_meta,
           coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) as keep_prob,
           p.source_meta->'vision' as v
      from public.beach_photos p
     where p.arena_group_id = p_fid
       and (p.curated_at is not null
            or coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) >= 0.65)
  )
  select e.*,
         coalesce((v->>'has_dog')::boolean, false)             as has_dog,
         coalesce((v->>'has_surfing')::boolean, false)         as has_surfing,
         coalesce((v->>'has_active_people')::boolean, false)   as has_active_people,
         coalesce((v->>'has_birds')::boolean, false)           as has_birds,
         coalesce(v->>'scene', '')                              as scene,
         coalesce(v->>'atmosphere', '')                         as atmosphere,
         coalesce(jsonb_array_length(v->'landscape_features'), 0)
                                                                as landscape_count,
         coalesce(v->'subjects', '[]'::jsonb)                   as subjects,
         -- Composite sort score: curated first, then keep_prob, then sort_order
         row_number() over (order by (curated_at is null)::int asc,
                                     coalesce((source_meta->>'predicted_keep_prob')::float, 0) desc,
                                     sort_order asc, id asc) as rank
    from eligible e;

  -- ─── Phase 1: pull all dogs (Franz: "dogs always stay") ─────────────
  for v_pick in
    select * from _cand where has_dog order by rank asc limit p_target
  loop
    v_result := v_result || jsonb_build_array(jsonb_build_object(
      'source',      v_pick.source,
      'image_url',   v_pick.image_url,
      'thumb_url',   v_pick.thumb_url,
      'attribution', v_pick.attribution,
      'license',     v_pick.license,
      'page_url',    v_pick.page_url,
      'captured_at', v_pick.captured_at,
      'curated',     v_pick.curated_at is not null,
      'keep_prob',   v_pick.keep_prob
    ));
    v_picked_ids := array_append(v_picked_ids, v_pick.id);
    v_slots_left := v_slots_left - 1;
    exit when v_slots_left <= 0;
  end loop;

  -- ─── Phase 2: one photo per content bucket, in priority order ──────
  for v_bucket in
    select unnest(array['surf','landscape','people','water','wide','atmosphere','structure'])
  loop
    exit when v_slots_left <= 0;

    select c.* into v_pick
      from _cand c
     where c.id <> all(v_picked_ids)
       and case v_bucket
             when 'surf'       then c.has_surfing
             when 'landscape'  then c.landscape_count > 0
             when 'people'     then c.has_active_people
             when 'structure'  then c.subjects @> '"pier"'::jsonb
                                  or c.subjects @> '"boats"'::jsonb
                                  or c.scene = 'urban'
             when 'atmosphere' then c.atmosphere in ('sunset','fog','stormy')
             when 'wide'       then c.scene = 'beach_with_sand'
             when 'water'      then c.scene in ('water_only','coast_no_sand')
             else false
           end
     order by c.rank asc
     limit 1;

    if v_pick.id is not null then
      v_result := v_result || jsonb_build_array(jsonb_build_object(
      'source',      v_pick.source,
      'image_url',   v_pick.image_url,
      'thumb_url',   v_pick.thumb_url,
      'attribution', v_pick.attribution,
      'license',     v_pick.license,
      'page_url',    v_pick.page_url,
      'captured_at', v_pick.captured_at,
      'curated',     v_pick.curated_at is not null,
      'keep_prob',   v_pick.keep_prob
    ));
      v_picked_ids := array_append(v_picked_ids, v_pick.id);
      v_slots_left := v_slots_left - 1;
    end if;
  end loop;

  -- ─── Phase 3: backfill any remaining slots with best leftovers ─────
  for v_pick in
    select * from _cand
     where id <> all(v_picked_ids)
     order by rank asc
     limit v_slots_left
  loop
    v_result := v_result || jsonb_build_array(jsonb_build_object(
      'source',      v_pick.source,
      'image_url',   v_pick.image_url,
      'thumb_url',   v_pick.thumb_url,
      'attribution', v_pick.attribution,
      'license',     v_pick.license,
      'page_url',    v_pick.page_url,
      'captured_at', v_pick.captured_at,
      'curated',     v_pick.curated_at is not null,
      'keep_prob',   v_pick.keep_prob
    ));
    v_picked_ids := array_append(v_picked_ids, v_pick.id);
    v_slots_left := v_slots_left - 1;
    exit when v_slots_left <= 0;
  end loop;

  return v_result;
end;
$$;


-- (Photo JSONB shape is inlined in get_beach_photos_diverse for record-
-- type compatibility — SQL functions can't take `record` args.)
