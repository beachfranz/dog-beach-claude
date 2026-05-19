-- get_beach_photos_diverse — v3 update per docs/photo_curation_v3_spec.md
-- + Franz 2026-05-19 design discussion.
--
-- Two changes:
--   1. Eligible CTE pulls has_path + has_vehicle out of the vision JSONB
--      so they're available to the bucket-matching CASE.
--   2. Phase 2 bucket order adds 'path' between 'landscape' and 'people'
--      (Franz: "include more pics of paths").
--
-- Vehicle penalty is NOT applied at the selector layer — it lives in
-- apply_vision_rules() during model scoring, so it's already baked into
-- predicted_keep_prob (and thus eff_score) by the time the selector
-- reads it.
--
-- Everything else (dog hard-keep Phase 1, eligibility floor at 0.65,
-- backfill phase 3 with no threshold-lowering) is unchanged.

BEGIN;

CREATE OR REPLACE FUNCTION public.get_beach_photos_diverse(p_fid bigint, p_target integer DEFAULT 6)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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
       and p.hidden_at is null
       and (
         p.curated_at is not null
         or coalesce(
              (p.source_meta->>'override_keep_prob')::float,
              (p.source_meta->>'predicted_keep_prob')::float,
              0
            ) >= 0.65
       )
  )
  select e.*,
         coalesce((v->>'has_dog')::boolean, false)             as has_dog,
         coalesce((v->>'has_surfing')::boolean, false)         as has_surfing,
         coalesce((v->>'has_active_people')::boolean, false)   as has_active_people,
         coalesce((v->>'has_birds')::boolean, false)           as has_birds,
         coalesce((v->>'has_path')::boolean, false)            as has_path,        -- v3
         coalesce((v->>'has_vehicle')::boolean, false)         as has_vehicle,     -- v3 (informational; penalty already in eff_score)
         coalesce(v->>'scene', '')                              as scene,
         coalesce(v->>'atmosphere', '')                         as atmosphere,
         coalesce(jsonb_array_length(v->'landscape_features'), 0)
                                                                as landscape_count,
         coalesce(v->'subjects', '[]'::jsonb)                   as subjects,
         row_number() over (order by (curated_at is null)::int asc,
                                     eff_score desc,
                                     sort_order asc, id asc) as rank
    from eligible e;

  -- ─── Phase 1: hard-keep dogs ─────────────────────────────────────────
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

  -- ─── Phase 2: one photo per content bucket, priority order (v3) ─────
  --   surf → landscape → PATH → people → structure → atmosphere → wide → water
  --   'path' inserted at position 3 per Franz 2026-05-19.
  for v_bucket in
    select unnest(array['surf','landscape','path','people','water','wide','atmosphere','structure'])
  loop
    exit when v_slots_left <= 0;

    select c.* into v_pick
      from _cand c
     where c.id <> all(v_picked_ids)
       and case v_bucket
             when 'surf'       then c.has_surfing
             when 'landscape'  then c.landscape_count > 0
             when 'path'       then c.has_path                                  -- v3
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

COMMIT;
