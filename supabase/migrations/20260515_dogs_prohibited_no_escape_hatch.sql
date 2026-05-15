-- 20260515_dogs_prohibited_no_escape_hatch.sql
--
-- Tighten the dogs_prohibited rollup: a beach-wide prohibition only
-- exists when no section provides an escape hatch.
--
-- BUG (carried from the earlier section-level-vs-region-level fix):
-- Some beaches have a region-level time_window with rule=not_allowed
-- that contradicts the section-level data. Corona del Mar (8333) is
-- the canary: region-level says not_allowed 10:00-16:30, but
-- picnic_area / parking_lot / restrooms / showers all default to
-- on_leash with no prohibition time_window → dogs ARE allowed in
-- those zones during 10-4:30. Per Franz's principle: "if dogs are
-- permitted on any zone of the beach, the beach is not a no-go."
-- The Hours-card bold "Dogs not allowed 10:00-16:30" line was wrong.
--
-- FIX: introduce _zone_rules_beach_wide_prohibition(zone_rules) which
-- returns (prohib_start, prohib_end) ONLY when every section in the
-- region is effectively not_allowed during the candidate window.
--   * Section default rule=not_allowed → blocks during any candidate.
--   * Section default rule=on_leash/off_leash with a not_allowed
--     time_window fully covering the candidate → blocks.
--   * Otherwise → section permits at some moment in the candidate →
--     escape hatch → no beach-wide prohibition.
--
-- The promoter's zr_payload swaps from the JSONPath query to a call
-- to this helper. claimed_values fallbacks (time_windows[0].before/
-- after, time_prohibited_hours) are unchanged.
--
-- Affected: 6 "both region+section" beaches identified earlier
-- (region-level not_allowed AND section-level not_allowed). After
-- re-firing the promoter for them, the ones with escape-hatch sections
-- (picnic_area etc. defaulting to on_leash) will lose their
-- dogs_prohibited values, which is correct.

begin;

create or replace function public._zone_rules_beach_wide_prohibition(
  p_zone_rules jsonb,
  out prohib_start text,
  out prohib_end text
)
language plpgsql
immutable
as $function$
declare
  region jsonb;
  candidate_tw jsonb;
  cand_start text;
  cand_end text;
  section_kv record;
  sec jsonb;
  sec_rule text;
  all_blocked boolean;
begin
  prohib_start := null;
  prohib_end := null;
  if p_zone_rules is null then return; end if;

  -- Iterate over every region (v1: $.regions[]; v2: $.seasons[].regions[]).
  for region in
    select r from jsonb_array_elements(coalesce(p_zone_rules->'regions','[]'::jsonb)) r
    union all
    select r from jsonb_array_elements(coalesce(p_zone_rules->'seasons','[]'::jsonb)) s
      cross join lateral jsonb_array_elements(coalesce(s->'regions','[]'::jsonb)) r
  loop
    -- For each region-level time_window with rule=not_allowed, treat
    -- as a candidate beach-wide window and verify against sections.
    for candidate_tw in
      select tw from jsonb_array_elements(coalesce(region->'time_windows','[]'::jsonb)) tw
       where (tw->>'rule') = 'not_allowed'
         and (tw->>'start') ~ '^[0-9]{1,2}:[0-9]{2}$'
         and (tw->>'end')   ~ '^[0-9]{1,2}:[0-9]{2}$'
    loop
      cand_start := candidate_tw->>'start';
      cand_end   := candidate_tw->>'end';
      all_blocked := true;

      -- A section blocks during the candidate window if:
      --   (a) its default rule is not_allowed (section always blocks), OR
      --   (b) it has a time_window with rule=not_allowed whose range
      --       fully covers the candidate window.
      -- Otherwise the section provides an escape hatch.
      for section_kv in
        select key, value from jsonb_each(coalesce(region->'sections','{}'::jsonb))
      loop
        sec := section_kv.value;
        sec_rule := sec->>'rule';
        if sec_rule = 'not_allowed' then
          -- Section is blocked by default. Continue (still blocks).
          continue;
        end if;
        -- Default permits. Look for a section-level time_window that
        -- fully covers the candidate.
        if exists (
          select 1
            from jsonb_array_elements(coalesce(sec->'time_windows','[]'::jsonb)) stw
           where (stw->>'rule') = 'not_allowed'
             and (stw->>'start') ~ '^[0-9]{1,2}:[0-9]{2}$'
             and (stw->>'end')   ~ '^[0-9]{1,2}:[0-9]{2}$'
             and (stw->>'start') <= cand_start
             and (stw->>'end')   >= cand_end
        ) then
          continue; -- section is fully blocked during the candidate
        end if;
        -- Section provides an escape hatch.
        all_blocked := false;
        exit;
      end loop;

      if all_blocked then
        prohib_start := cand_start;
        prohib_end   := cand_end;
        return; -- first all-blocked window wins
      end if;
    end loop;
  end loop;
  return;
end;
$function$;


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
  -- FIX 2026-05-15 (escape-hatch): use the helper that walks every
  -- section to verify there's no permitting zone during the candidate.
  zr_payload as (
    select bdp.arena_group_id as fid, pw.prohib_start, pw.prohib_end
      from public.beach_dog_policy bdp
      left join lateral public._zone_rules_beach_wide_prohibition(bdp.zone_rules) pw on true
     where bdp.zone_rules is not null
       and (p_fid is null or bdp.arena_group_id = p_fid)
  ),
  time_payload as (
    select cp.fid,
      coalesce(
        case when zr.prohib_start ~ '^[0-9]{1,2}:[0-9]{2}$'
             then zr.prohib_start end,
        case when jsonb_typeof(cp.claimed_values->'time_windows') = 'array'
              and jsonb_array_length(cp.claimed_values->'time_windows') > 0
              and (cp.claimed_values->'time_windows'->0->>'before') ~ '^[0-9]{1,2}:[0-9]{2}$'
             then (cp.claimed_values->'time_windows'->0->>'before') end,
        public._norm_time_text(split_part(cp.claimed_values->>'time_prohibited_hours','-',1))
      ) as prohib_start,
      coalesce(
        case when zr.prohib_end ~ '^[0-9]{1,2}:[0-9]{2}$'
             then zr.prohib_end end,
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
