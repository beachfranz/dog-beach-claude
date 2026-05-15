-- 20260515_dogs_prohibited_zr_overrides_claimed.sql
--
-- Follow-up to the no-escape-hatch migration: when zone_rules exists,
-- trust the helper exclusively. Don't fall back to claimed_values
-- (time_windows[0].before/after, time_prohibited_hours) for those rows.
--
-- WHY: Corona del Mar (8333) survived the no-escape-hatch fix because
-- claimed_values had `{before: 10:00, after: 16:30, season: year_round}`
-- which the time_payload fallback lifted into dogs_prohibited even
-- after the helper correctly returned NULL. Zone_rules is the
-- authoritative source for beaches that have it (2,612 / 2,612 of
-- scoreable beaches do); the claimed_values fallback is for beaches
-- without zone_rules (currently 0 scoreable beaches).
--
-- After this: zone_rules-bearing beaches with no beach-wide prohibition
-- (per helper) → dogs_prohibited NULL/NULL. Zone_rules-absent beaches
-- still fall through to claimed_values as before.

begin;

create or replace function public.promote_canonical_dogs_to_beach_dog_policy(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
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
  supplemental as (
    select gold_fid as fid, jsonb_object_agg(k, v) as extra
      from (
        select distinct on (e.gold_fid, ek.key)
               e.gold_fid, ek.key as k, e.claimed_values -> ek.key as v
          from public.beach_enrichment_provenance e
          cross join lateral jsonb_object_keys(e.claimed_values) ek(key)
         where e.field_group = 'dogs' and not e.is_canonical
           and e.claimed_values -> ek.key is not null
           and (p_fid is null or e.gold_fid = p_fid)
         order by e.gold_fid, ek.key, e.confidence desc nulls last
      ) t
     group by gold_fid
  ),
  merged_cv as (
    select c.fid,
           coalesce(s.extra, '{}'::jsonb) || c.claimed_values as cv
      from canon_payload c
      left join supplemental s on s.fid = c.fid
  ),
  zr_payload as (
    select bdp.arena_group_id as fid,
           bdp.zone_rules is not null as has_zr,
           pw.prohib_start, pw.prohib_end
      from public.beach_dog_policy bdp
      left join lateral public._zone_rules_beach_wide_prohibition(bdp.zone_rules) pw on true
     where (p_fid is null or bdp.arena_group_id = p_fid)
  ),
  time_payload as (
    select cp.fid,
      -- When zone_rules exists, trust the helper exclusively (NULL means
      -- no beach-wide prohibition). Only fall back to claimed_values
      -- when zone_rules is absent — currently 0 scoreable beaches.
      case when coalesce(zr.has_zr, false) then zr.prohib_start
           else coalesce(
             case when jsonb_typeof(cp.claimed_values->'time_windows') = 'array'
                   and jsonb_array_length(cp.claimed_values->'time_windows') > 0
                   and (cp.claimed_values->'time_windows'->0->>'before') ~ '^[0-9]{1,2}:[0-9]{2}$'
                  then (cp.claimed_values->'time_windows'->0->>'before') end,
             public._norm_time_text(split_part(cp.claimed_values->>'time_prohibited_hours','-',1))
           ) end as prohib_start,
      case when coalesce(zr.has_zr, false) then zr.prohib_end
           else coalesce(
             case when jsonb_typeof(cp.claimed_values->'time_windows') = 'array'
                   and jsonb_array_length(cp.claimed_values->'time_windows') > 0
                   and (cp.claimed_values->'time_windows'->0->>'after') ~ '^[0-9]{1,2}:[0-9]{2}$'
                  then (cp.claimed_values->'time_windows'->0->>'after') end,
             public._norm_time_text(split_part(cp.claimed_values->>'time_prohibited_hours','-',2))
           ) end as prohib_end
    from canon_payload cp
    left join zr_payload zr on zr.fid = cp.fid
  ),
  derived as (
    select c.gold_fid as fid,
           public._norm_dogs_allowed(c.v_allowed)        as new_dogs_allowed,
           case when public._norm_dogs_allowed(c.v_allowed) = 'no' then null
                else public._norm_leash_policy(c.v_leash) end as new_leash_policy,
           case when public._norm_dogs_allowed(c.v_allowed) = 'no' then false
                else public._norm_bool(to_jsonb(c.v_off_leash)) end    as new_off_leash_flag,
           case when public._norm_dogs_allowed(c.v_allowed) = 'no' then false
                else public._norm_bool(to_jsonb(c.v_has_on_leash)) end as new_has_on_leash,
           case when public._norm_dogs_allowed(c.v_allowed) = 'no' then false
                else public._norm_bool(to_jsonb(c.v_has_off_leash)) end as new_has_off_leash,
           tp.prohib_start                               as new_prohib_start,
           tp.prohib_end                                 as new_prohib_end,
           case lower(m.cv->>'public_access')
             when 'yes'        then 'yes'
             when 'restricted' then 'restricted'
             when 'no'         then 'no'
             else null end                              as new_public_access,
           greatest(c.conf_allowed, c.conf_leash, c.conf_has_on_leash, c.conf_has_off_leash) as new_confidence,
           coalesce(c.dis_allowed, false) or coalesce(c.dis_leash, false) as new_disagreement
      from consensus_dogs c
      left join time_payload tp on tp.fid = c.gold_fid
      left join merged_cv m on m.fid = c.gold_fid
  ),
  protected as (
    select arena_group_id from public.beach_dog_policy where source = 'manual_curator'
  ),
  upsert as (
    insert into public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       has_on_leash, has_off_leash,
       dogs_prohibited_start, dogs_prohibited_end, public_access,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select d.fid, d.new_dogs_allowed, d.new_leash_policy, d.new_off_leash_flag,
           d.new_has_on_leash, d.new_has_off_leash,
           d.new_prohib_start, d.new_prohib_end, d.new_public_access,
           'auto_promoted_from_consensus', now(),
           format('Phase B consensus: dogs=%s, has_on=%s, has_off=%s%s',
                  d.new_dogs_allowed,
                  coalesce(d.new_has_on_leash::text, '?'),
                  coalesce(d.new_has_off_leash::text, '?'),
                  case when d.new_disagreement then ' [DISAGREEMENT]' else '' end),
           d.new_confidence, d.new_disagreement
      from derived d
     where d.fid not in (select arena_group_id from protected)
       and (d.new_dogs_allowed is not null or d.new_leash_policy is not null
            or d.new_off_leash_flag is not null or d.new_prohib_start is not null
            or d.new_has_on_leash is not null or d.new_has_off_leash is not null
            or d.new_public_access is not null)
       and coalesce(d.new_confidence, 1) >= p_min_confidence
    on conflict (arena_group_id) do update
      set dogs_allowed=excluded.dogs_allowed,
          leash_policy=excluded.leash_policy,
          off_leash_flag=excluded.off_leash_flag,
          has_on_leash=excluded.has_on_leash,
          has_off_leash=excluded.has_off_leash,
          dogs_prohibited_start=excluded.dogs_prohibited_start,
          dogs_prohibited_end=excluded.dogs_prohibited_end,
          public_access=coalesce(excluded.public_access, beach_dog_policy.public_access),
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
          or beach_dog_policy.public_access is distinct from excluded.public_access
          or beach_dog_policy.consensus_confidence is distinct from excluded.consensus_confidence
          or beach_dog_policy.disagreement_flag is distinct from excluded.disagreement_flag)
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upsert;
  return query select ins::bigint, upd::bigint, skp::bigint;
end;
$function$;

commit;
