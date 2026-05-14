-- 20260513_get_beach_photos_diverse_v3.sql
--
-- Updates the consumer-facing photo selector per Franz 2026-05-13 spec:
--   * Default target = 6 (1 hero + 5 supporting)
--   * Excludes hidden_at IS NOT NULL photos
--   * Effective score = override_keep_prob (if set) > predicted_keep_prob
--     PLUS +0.07 bonus if curator-promoted (curated_at IS NOT NULL)
--   * Hard floor 0.65: non-curator photos must clear effective_score >= 0.65
--     (curator photos always qualify — no minimum threshold)
--   * First returned photo gets is_hero = true (consumer renders as hero)
--   * Bucket-fill diversity preserved (Phase 2 — surf/landscape/people/...)
--   * Do NOT lower threshold to backfill (per Franz: "don't stretch to
--     include anything low quality")
--
-- Backward compat: signature unchanged (p_fid, p_target). Callers that
-- still pass p_target=8 get 8 (subject to availability + floor).

begin;

create or replace function public.get_beach_photos_diverse(
  p_fid bigint,
  p_target integer default 6
) returns jsonb
language plpgsql security definer
set search_path to 'public'
as $function$
declare
  v_result jsonb := '[]'::jsonb;
  v_picked_ids bigint[] := array[]::bigint[];
  v_bucket text;
  v_pick record;
  v_slots_left int := p_target;
  v_is_first boolean := true;
begin
  drop table if exists pg_temp._cand;
  create temp table _cand on commit drop as
  with eligible as (
    select p.id, p.source, p.image_url, p.thumb_url, p.attribution,
           p.license, p.page_url, p.captured_at, p.curated_at,
           p.sort_order, p.source_meta,
           -- Effective keep_prob: override > predicted, +0.07 for curator.
           coalesce(
             (p.source_meta->>'override_keep_prob')::float,
             (p.source_meta->>'predicted_keep_prob')::float,
             0
           )
           + case when p.curated_at is not null then 0.07 else 0 end as eff_score,
           coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) as keep_prob,
           p.source_meta->'vision' as v
      from public.beach_photos p
     where p.arena_group_id = p_fid
       and p.hidden_at is null                                            -- exclude hidden
       and (
         p.curated_at is not null                                         -- curator always qualifies
         or coalesce(
              (p.source_meta->>'override_keep_prob')::float,
              (p.source_meta->>'predicted_keep_prob')::float,
              0
            ) >= 0.65                                                     -- non-curator: 0.65 floor
       )
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
         -- Rank: curator first, then by effective score DESC, then sort_order, id
         row_number() over (order by (curated_at is null)::int asc,
                                     eff_score desc,
                                     sort_order asc, id asc) as rank
    from eligible e;

  -- ─── Phase 1: hard-keep dogs (Franz: "dogs always stay") ─────────────
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
      'keep_prob',   v_pick.keep_prob,
      'eff_score',   v_pick.eff_score,
      'is_hero',     v_is_first
    ));
    v_is_first := false;
    v_picked_ids := array_append(v_picked_ids, v_pick.id);
    v_slots_left := v_slots_left - 1;
    exit when v_slots_left <= 0;
  end loop;

  -- ─── Phase 2: one photo per content bucket, priority order ──────────
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
        'keep_prob',   v_pick.keep_prob,
        'eff_score',   v_pick.eff_score,
        'is_hero',     v_is_first
      ));
      v_is_first := false;
      v_picked_ids := array_append(v_picked_ids, v_pick.id);
      v_slots_left := v_slots_left - 1;
    end if;
  end loop;

  -- ─── Phase 3: backfill remaining slots with best leftovers ─────────
  -- IMPORTANT: only pulls from _cand (already-filtered eligible set). Does
  -- NOT lower the threshold. If we run out of eligibles, slots stay empty
  -- (consumer renders 1-5 instead of 6). Franz: "don't stretch to include
  -- anything low quality."
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
      'keep_prob',   v_pick.keep_prob,
      'eff_score',   v_pick.eff_score,
      'is_hero',     v_is_first
    ));
    v_is_first := false;
    v_picked_ids := array_append(v_picked_ids, v_pick.id);
    v_slots_left := v_slots_left - 1;
    exit when v_slots_left <= 0;
  end loop;

  return v_result;
end;
$function$;

commit;
