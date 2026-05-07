-- 20260507_promoter_time_windows_jsonb.sql
--
-- Pin #14 (partial): the promoter currently lands dogs_prohibited_start/end
-- only when a source provides time_prohibited_hours in "HH:MM-HH:MM" text
-- format. But many sources (county_policy in particular) emit structured
-- `time_windows` jsonb arrays like:
--   [{"after": "04:00", "before": "22:00", "season": "year_round"}]
-- meaning dogs allowed AFTER 04:00 AND BEFORE 22:00 → prohibited 22:00-04:00.
-- The promoter ignored this format, so 50 votes of structured time-window
-- data weren't reaching the consumer surface.
--
-- This migration extends promote_canonical_dogs_to_beach_dog_policy to
-- coalesce structured time_windows ahead of legacy text format. When
-- time_windows[0].before is present, treat it as prohibited_start
-- (when allowed access ends) and time_windows[0].after as prohibited_end
-- (when allowed access resumes).
--
-- Out of scope for tonight: parsing time-window patterns from narrative
-- `notes` text (Mission Beach / Pacific Beach / La Jolla Shores all have
-- the SD city pre-9am/post-6pm rule in notes but not in structured form).
-- That's calibration / extraction prompt work — deferred to remaining
-- pin #14 effort.

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
  -- Pull structured time-window data when present. Falls back to legacy
  -- time_prohibited_hours text format. When a source provides
  -- time_windows[0] with `before`+`after`, prohibited window runs FROM
  -- before TO after (with day-wrap semantics for the consumer).
  time_payload as (
    select cp.fid,
      coalesce(
        -- Structured: time_windows[0].before = when allowed ends = prohibited starts
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
           coalesce(p.claimed_values->>'areas_evidence',
                    p.claimed_values->>'areas_boundaries',
                    p.claimed_values->>'designated_dog_zones',
                    p.claimed_values->>'prohibited_areas') as new_dogs_allowed_areas,
           greatest(c.conf_allowed, c.conf_leash, c.conf_has_on_leash, c.conf_has_off_leash) as new_confidence,
           coalesce(c.dis_allowed, false) or coalesce(c.dis_leash, false) as new_disagreement
      from consensus_dogs c
      left join canon_payload p on p.fid = c.gold_fid
      left join time_payload  tp on tp.fid = c.gold_fid
  ),
  protected as (
    select arena_group_id from public.beach_dog_policy where source = 'manual_curator'
  ),
  upsert as (
    insert into public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       has_on_leash, has_off_leash,
       dogs_prohibited_start, dogs_prohibited_end, dogs_allowed_areas,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select d.fid, d.new_dogs_allowed, d.new_leash_policy, d.new_off_leash_flag,
           d.new_has_on_leash, d.new_has_off_leash,
           d.new_prohib_start, d.new_prohib_end, d.new_dogs_allowed_areas,
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
            or d.new_dogs_allowed_areas is not null
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
          dogs_allowed_areas=excluded.dogs_allowed_areas,
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
          or beach_dog_policy.dogs_allowed_areas is distinct from excluded.dogs_allowed_areas
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
