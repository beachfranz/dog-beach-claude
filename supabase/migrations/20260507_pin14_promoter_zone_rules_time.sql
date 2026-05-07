-- 20260507_pin14_promoter_zone_rules_time.sql
--
-- Pin #14 (final): land time-window data from zone_rules JSONB into
-- beach_dog_policy.dogs_prohibited_start/end.
--
-- Earlier today we extended the promoter to parse structured time_windows
-- from canonical BEP claims. That captured 24 beaches (mostly the SD
-- County 22:00-04:00 rule from county_policy votes).
--
-- The remaining 3 SD city time-window beaches (Mission Beach, Pacific
-- Beach, La Jolla Shores) had their time data extracted by text_repass_v1
-- but stored INSIDE zone_rules JSONB at varying nesting levels:
--   PB / LJS:    regions[0].time_windows[]
--   Mission:     regions[0].sections.<section>.time_windows[]
--
-- This migration extends the promoter to pull from beach_dog_policy.zone_rules
-- using jsonpath recursive descent ($.**.time_windows[*] ? (@.rule ==
-- "not_allowed")). Falls through to the existing structured time_windows
-- and time_prohibited_hours paths.
--
-- Note: zone_rules is on the SAME table the promoter writes to. That's
-- not a circular dependency — _promote_zone_rules_for_fid populates
-- zone_rules separately (after text_repass runs); the promoter just
-- reads the existing value when computing the flat fields. For
-- first-time promotes (zone_rules NULL), the existing fallbacks apply.

begin;

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
  -- Pull time-window data from beach_dog_policy.zone_rules first (richest),
  -- fall back to canonical claim's structured time_windows, then to legacy
  -- time_prohibited_hours text format.
  zr_payload as (
    select bdp.arena_group_id as fid,
           jsonb_path_query_first(
             bdp.zone_rules,
             'strict $.**.time_windows[*] ? (@.rule == "not_allowed")'
           ) as nawin
      from public.beach_dog_policy bdp
     where bdp.zone_rules is not null
       and (p_fid is null or bdp.arena_group_id = p_fid)
  ),
  time_payload as (
    select cp.fid,
      coalesce(
        -- 1. zone_rules recursive descent → first not_allowed time_window's start
        case when (zr.nawin->>'start') ~ '^[0-9]{1,2}:[0-9]{2}$'
             then (zr.nawin->>'start') end,
        -- 2. canonical claim's structured time_windows[0].before
        case when jsonb_typeof(cp.claimed_values->'time_windows') = 'array'
              and jsonb_array_length(cp.claimed_values->'time_windows') > 0
              and (cp.claimed_values->'time_windows'->0->>'before') ~ '^[0-9]{1,2}:[0-9]{2}$'
             then (cp.claimed_values->'time_windows'->0->>'before') end,
        -- 3. legacy text format
        public._norm_time_text(split_part(cp.claimed_values->>'time_prohibited_hours','-',1))
      ) as prohib_start,
      coalesce(
        case when (zr.nawin->>'end') ~ '^[0-9]{1,2}:[0-9]{2}$'
             then (zr.nawin->>'end') end,
        case when jsonb_typeof(cp.claimed_values->'time_windows') = 'array'
              and jsonb_array_length(cp.claimed_values->'time_windows') > 0
              and (cp.claimed_values->'time_windows'->0->>'after') ~ '^[0-9]{1,2}:[0-9]{2}$'
             then (cp.claimed_values->'time_windows'->0->>'after') end,
        public._norm_time_text(split_part(cp.claimed_values->>'time_prohibited_hours','-',2))
      ) as prohib_end
    from canon_payload cp
    left join zr_payload zr on zr.fid = cp.fid
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
      left join time_payload tp on tp.fid = c.gold_fid
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

commit;
