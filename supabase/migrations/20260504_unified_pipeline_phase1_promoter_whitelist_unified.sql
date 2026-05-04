-- 20260504_unified_pipeline_phase1_promoter_whitelist_unified.sql
--
-- Bug fix: the auto-promoter functions had hardcoded source whitelists that
-- didn't include 'unified_v1'. So canonical unified_v1 evidence (e.g., the
-- newly-discovered Fiesta Island data) was being skipped by the promoter,
-- and beach_dog_policy stayed at the legacy auto_promoted_from_production
-- values.
--
-- Fix: extend both promoter source whitelists to include 'unified_v1'.

begin;

create or replace function public.promote_canonical_dogs_to_beach_dog_policy(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint) as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with canon as (
    select e.gold_fid as fid, e.source, e.confidence, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
     where e.field_group = 'dogs' and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
       and e.confidence >= p_min_confidence
       and e.source = any(array['manual','llm','park_url','park_operators',
                                'research','old_school_llm','json_explode',
                                'unified_v1'])
  ),
  derived as (
    select c.fid, c.source, c.confidence,
           public._norm_dogs_allowed(c.claimed_values->>'allowed') as new_dogs_allowed,
           public._norm_leash_policy(c.claimed_values->>'leash_required') as new_leash_policy,
           public._norm_bool(c.claimed_values->'off_leash_exists') as new_off_leash_flag,
           public._norm_time_text(split_part(c.claimed_values->>'time_prohibited_hours','-',1)) as new_prohib_start,
           public._norm_time_text(split_part(c.claimed_values->>'time_prohibited_hours','-',2)) as new_prohib_end,
           c.claimed_values->>'areas_evidence' as new_dogs_allowed_areas,
           c.claimed_values->>'areas_boundaries' as new_areas_boundaries
      from canon c
  ),
  protected as (
    select arena_group_id from public.beach_dog_policy where source = 'manual_curator'
  ),
  upsert as (
    insert into public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       dogs_prohibited_start, dogs_prohibited_end, dogs_allowed_areas,
       source, curated_at, notes)
    select d.fid, d.new_dogs_allowed, d.new_leash_policy, d.new_off_leash_flag,
           d.new_prohib_start, d.new_prohib_end,
           coalesce(d.new_dogs_allowed_areas, d.new_areas_boundaries),
           'auto_promoted_from_resolver', now(),
           format('Promoted from canonical evidence (source=%s, confidence=%s)', d.source, d.confidence)
      from derived d
     where d.fid not in (select arena_group_id from protected)
       and (d.new_dogs_allowed is not null or d.new_leash_policy is not null
            or d.new_off_leash_flag is not null or d.new_prohib_start is not null
            or d.new_dogs_allowed_areas is not null)
    on conflict (arena_group_id) do update
      set dogs_allowed=excluded.dogs_allowed, leash_policy=excluded.leash_policy,
          off_leash_flag=excluded.off_leash_flag,
          dogs_prohibited_start=excluded.dogs_prohibited_start,
          dogs_prohibited_end=excluded.dogs_prohibited_end,
          dogs_allowed_areas=excluded.dogs_allowed_areas,
          source=excluded.source, curated_at=now(), notes=excluded.notes
      where beach_dog_policy.source <> 'manual_curator'
        and (beach_dog_policy.dogs_allowed is distinct from excluded.dogs_allowed
          or beach_dog_policy.leash_policy is distinct from excluded.leash_policy
          or beach_dog_policy.off_leash_flag is distinct from excluded.off_leash_flag
          or beach_dog_policy.dogs_prohibited_start is distinct from excluded.dogs_prohibited_start
          or beach_dog_policy.dogs_prohibited_end is distinct from excluded.dogs_prohibited_end
          or beach_dog_policy.dogs_allowed_areas is distinct from excluded.dogs_allowed_areas)
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upsert;
  return query select ins::bigint, upd::bigint, skp::bigint;
end;
$$ language plpgsql;


create or replace function public.promote_canonical_practical_to_beach_amenities(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, gold_open_close_set bigint) as $$
declare ins int := 0; upd int := 0; gold_set int := 0;
begin
  with canon as (
    select e.gold_fid as fid, e.source, e.confidence, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
     where e.field_group = 'practical' and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
       and e.confidence >= p_min_confidence
       and e.source = any(array['manual','llm','park_url','park_operators',
                                'old_school_llm','json_explode','ccc',
                                'unified_v1'])
  ),
  protected as (
    select arena_group_id from public.beach_amenities where source = 'manual_curator'
  ),
  upsert_amen as (
    insert into public.beach_amenities
      (arena_group_id, has_restrooms, has_lifeguards, has_showers,
       has_drinking_water, has_disabled_access, has_food, has_fire_pits,
       has_picnic_area, parking_type, hours_text, source, curated_at, notes)
    select c.fid,
      public._norm_bool(c.claimed_values->'has_restrooms'),
      public._norm_bool(c.claimed_values->'has_lifeguards'),
      public._norm_bool(c.claimed_values->'has_showers'),
      public._norm_bool(c.claimed_values->'has_drinking_water'),
      public._norm_bool(c.claimed_values->'has_disabled_access'),
      public._norm_bool(c.claimed_values->'has_food'),
      public._norm_bool(c.claimed_values->'has_fire_pits'),
      public._norm_bool(c.claimed_values->'has_picnic_area'),
      c.claimed_values->>'parking_type',
      coalesce(c.claimed_values->>'hours_text',
        case when (c.claimed_values->>'hours_open') is not null
              or (c.claimed_values->>'hours_close') is not null
        then trim(both '-' from format('%s-%s', coalesce(c.claimed_values->>'hours_open',''),
                                                  coalesce(c.claimed_values->>'hours_close','')))
        end),
      'auto_promoted_from_resolver', now(),
      format('Promoted from canonical evidence (source=%s, confidence=%s)', c.source, c.confidence)
      from canon c
     where c.fid not in (select arena_group_id from protected)
    on conflict (arena_group_id) do update
      set has_restrooms=excluded.has_restrooms, has_lifeguards=excluded.has_lifeguards,
          has_showers=excluded.has_showers, has_drinking_water=excluded.has_drinking_water,
          has_disabled_access=excluded.has_disabled_access, has_food=excluded.has_food,
          has_fire_pits=excluded.has_fire_pits, has_picnic_area=excluded.has_picnic_area,
          parking_type=coalesce(excluded.parking_type, beach_amenities.parking_type),
          hours_text=coalesce(excluded.hours_text, beach_amenities.hours_text),
          source=excluded.source, curated_at=now(), notes=excluded.notes
      where beach_amenities.source <> 'manual_curator'
    returning (xmax = 0) as inserted, arena_group_id
  ),
  parsed_hours as (
    select c.fid,
           public._norm_time_text(c.claimed_values->>'hours_open')  as t_open,
           public._norm_time_text(c.claimed_values->>'hours_close') as t_close
      from canon c
  ),
  upd_gold as (
    update public.beaches_gold g
       set open_time  = coalesce(g.open_time,  ph.t_open),
           close_time = coalesce(g.close_time, ph.t_close)
      from parsed_hours ph
     where g.fid = ph.fid
       and ((g.open_time is null and ph.t_open is not null)
         or (g.close_time is null and ph.t_close is not null))
    returning g.fid
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted),
         (select count(*) from upd_gold)
    into ins, upd, gold_set from upsert_amen;
  return query select ins::bigint, upd::bigint, gold_set::bigint;
end;
$$ language plpgsql;

commit;
