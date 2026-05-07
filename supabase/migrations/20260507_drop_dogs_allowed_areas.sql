-- 20260507_drop_dogs_allowed_areas.sql
--
-- Pin #19: retire dogs_allowed_areas text column from beach_dog_policy.
--
-- The flat-text per-zone summary ("sand: forbidden; water: forbidden; ...")
-- is fully superseded by the zone_rules JSONB structure (sections within
-- regions within seasons), which carries the same per-zone rules as
-- structured data with evidence quotes and source attribution. The
-- redundant text was either an exact flat-string version of zone_rules
-- (156 rows) or noise (1 row pointing at Rosie's from a generic city
-- beach record).
--
-- Coverage breakdown pre-drop (verified 2026-05-07):
--   - 156 rows: redundant with zone_rules
--   - 4 rows: text only, no zone_rules. 3 SoCal seeds (Doheny/San
--     Onofre/Point Dume) had repass_zone_rules run earlier this session
--     so their zone_rules JSONB now carries the per-zone detail. The
--     4th (Long Beach Latin Soul, fid 522) was promoter noise.
--   - 614 rows: NULL (no value to lose)
--
-- This migration drops the column and updates the promoter to stop
-- writing to it. Code edits in get-beach-detail / beach-chat / admin
-- tooling drop the field from SELECTs and form fields in parallel.

begin;

-- 1. Update the promoter to stop writing dogs_allowed_areas.
create or replace function public.promote_canonical_dogs_to_beach_dog_policy(
  p_fid bigint default null, p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with consensus_dogs as (
    select gold_fid,
      max(winning_value) filter (where field_name = 'allowed')           as v_allowed,
      max(confidence)    filter (where field_name = 'allowed')           as conf_allowed,
      bool_or(disagreement) filter (where field_name = 'allowed')        as dis_allowed,
      max(winning_value) filter (where field_name = 'leash_required')    as v_leash,
      max(confidence)    filter (where field_name = 'leash_required')    as conf_leash,
      bool_or(disagreement) filter (where field_name = 'leash_required') as dis_leash,
      max(winning_value) filter (where field_name = 'off_leash_exists')  as v_off_leash,
      max(winning_value) filter (where field_name = 'has_on_leash')      as v_has_on_leash,
      max(confidence)    filter (where field_name = 'has_on_leash')      as conf_has_on_leash,
      max(winning_value) filter (where field_name = 'has_off_leash')     as v_has_off_leash,
      max(confidence)    filter (where field_name = 'has_off_leash')     as conf_has_off_leash
      from public.beach_field_consensus
     where field_group = 'dogs' and (p_fid is null or gold_fid = p_fid)
     group by gold_fid
  ),
  canon_payload as (
    select e.gold_fid as fid, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
     where e.field_group = 'dogs' and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  time_payload as (
    select cp.fid,
      coalesce(
        case when jsonb_typeof(cp.claimed_values->'time_windows') = 'array'
              and jsonb_array_length(cp.claimed_values->'time_windows') > 0
              and (cp.claimed_values->'time_windows'->0->>'before') ~ '^[0-9]{1,2}:[0-9]{2}$'
             then (cp.claimed_values->'time_windows'->0->>'before') end,
        public._norm_time_text(split_part(cp.claimed_values->>'time_prohibited_hours','-',1))
      ) as prohib_start,
      coalesce(
        case when jsonb_typeof(cp.claimed_values->'time_windows') = 'array'
              and jsonb_array_length(cp.claimed_values->'time_windows') > 0
              and (cp.claimed_values->'time_windows'->0->>'after') ~ '^[0-9]{1,2}:[0-9]{2}$'
             then (cp.claimed_values->'time_windows'->0->>'after') end,
        public._norm_time_text(split_part(cp.claimed_values->>'time_prohibited_hours','-',2))
      ) as prohib_end
    from canon_payload cp
  ),
  derived as (
    select c.gold_fid as fid,
           public._norm_dogs_allowed(c.v_allowed)        as new_dogs_allowed,
           public._norm_leash_policy(c.v_leash)          as new_leash_policy,
           public._norm_bool(to_jsonb(c.v_off_leash))    as new_off_leash_flag,
           public._norm_bool(to_jsonb(c.v_has_on_leash)) as new_has_on_leash,
           public._norm_bool(to_jsonb(c.v_has_off_leash)) as new_has_off_leash,
           tp.prohib_start                               as new_prohib_start,
           tp.prohib_end                                 as new_prohib_end,
           greatest(c.conf_allowed, c.conf_leash, c.conf_has_on_leash, c.conf_has_off_leash) as new_confidence,
           coalesce(c.dis_allowed, false) or coalesce(c.dis_leash, false) as new_disagreement
      from consensus_dogs c
      left join time_payload  tp on tp.fid = c.gold_fid
  ),
  protected as (
    select arena_group_id from public.beach_dog_policy where source = 'manual_curator'
  ),
  upsert as (
    insert into public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       has_on_leash, has_off_leash,
       dogs_prohibited_start, dogs_prohibited_end,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select d.fid, d.new_dogs_allowed, d.new_leash_policy, d.new_off_leash_flag,
           d.new_has_on_leash, d.new_has_off_leash,
           d.new_prohib_start, d.new_prohib_end,
           'auto_promoted_from_consensus', now(),
           format('Phase B consensus: dogs_allowed=%s (conf=%s%s), leash=%s (conf=%s%s), has_on=%s, has_off=%s',
                  d.new_dogs_allowed, coalesce(d.new_confidence, 0),
                  case when d.new_disagreement then ', DISAGREEMENT' else '' end,
                  d.new_leash_policy, coalesce(d.new_confidence, 0),
                  case when d.new_disagreement then ', DISAGREEMENT' else '' end,
                  coalesce(d.new_has_on_leash::text, '?'),
                  coalesce(d.new_has_off_leash::text, '?')),
           d.new_confidence, d.new_disagreement
      from derived d
     where d.fid not in (select arena_group_id from protected)
       and (d.new_dogs_allowed is not null or d.new_leash_policy is not null
            or d.new_off_leash_flag is not null or d.new_prohib_start is not null
            or d.new_has_on_leash is not null or d.new_has_off_leash is not null)
       and coalesce(d.new_confidence, 1) >= p_min_confidence
    on conflict (arena_group_id) do update
      set dogs_allowed=excluded.dogs_allowed,
          leash_policy=excluded.leash_policy,
          off_leash_flag=excluded.off_leash_flag,
          has_on_leash=excluded.has_on_leash,
          has_off_leash=excluded.has_off_leash,
          dogs_prohibited_start=excluded.dogs_prohibited_start,
          dogs_prohibited_end=excluded.dogs_prohibited_end,
          source=excluded.source, curated_at=now(), notes=excluded.notes,
          consensus_confidence=excluded.consensus_confidence,
          disagreement_flag=excluded.disagreement_flag
      where beach_dog_policy.source <> 'manual_curator'
        and (beach_dog_policy.dogs_allowed is distinct from excluded.dogs_allowed
          or beach_dog_policy.leash_policy is distinct from excluded.leash_policy
          or beach_dog_policy.off_leash_flag is distinct from excluded.off_leash_flag
          or beach_dog_policy.has_on_leash is distinct from excluded.has_on_leash
          or beach_dog_policy.has_off_leash is distinct from excluded.has_off_leash
          or beach_dog_policy.dogs_prohibited_start is distinct from excluded.dogs_prohibited_start
          or beach_dog_policy.dogs_prohibited_end is distinct from excluded.dogs_prohibited_end
          or beach_dog_policy.consensus_confidence is distinct from excluded.consensus_confidence
          or beach_dog_policy.disagreement_flag is distinct from excluded.disagreement_flag)
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upsert;
  return query select ins::bigint, upd::bigint, skp::bigint;
end;
$function$;

-- 2. Drop the column.
alter table public.beach_dog_policy drop column if exists dogs_allowed_areas;

commit;
