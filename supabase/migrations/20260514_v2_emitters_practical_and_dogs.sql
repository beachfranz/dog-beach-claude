-- 20260514_v2_emitters_practical_and_dogs.sql
--
-- Promote the remaining 6 extract_research_v2 fields into BEP:
--
-- practical field_group:
--   lifeguard (full_time/seasonal/none/unknown) → has_lifeguards
--   restrooms (yes/seasonal/none/unknown)        → has_restrooms
--   outdoor_showers (yes/seasonal/none/unknown) → has_showers
--   + seasonal flag encoded in claimed_values as has_X_seasonal='true'
--
-- dogs field_group:
--   dogs_allowed (yes/no/seasonal/unclear) → dogs_allowed (yes/no/mixed)
--   leash_policy (off_leash/on_leash/unknown) → leash_policy
--   public_access (yes/restricted/no/unclear)  → public_access
--
-- Strict normalization: parsed_value must exactly match an enum value.
-- Prose-style responses ("Yes, the public can reach...") drop. Roughly
-- 50% of v2 rows have clean enums; that's enough signal.
--
-- Confidence-from-agreement (same schedule as parking emitter):
--   3+ votes no dissent → 0.90
--   2 votes no dissent  → 0.75
--   majority + dissent  → 0.55
--   lone vote           → 0.40
--
-- Also adds public_access column to beach_dog_policy (yes/restricted/
-- no/unclear) so we don't squash the signal into access_rule (which
-- holds different-concept values like 'mixed'/'off_leash').

begin;

alter table public.beach_dog_policy
  add column if not exists public_access text;

comment on column public.beach_dog_policy.public_access is
  'Public reachability: yes / restricted (permit-required, special hours) / no / null. Sourced from extract_research_v2 multi-model votes.';

-- ─── Practical emitter (lifeguard / restrooms / outdoor_showers) ─────
create or replace function public._emit_evidence_from_beach_policy_practical_v2(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with normalized as (
    select arena_group_id as gold_fid, field_name,
      case
        -- Affirmative ('yes', 'full_time') and seasonal both imply presence
        when lower(parsed_value) in ('yes','full_time','seasonal') then 'true'
        when lower(parsed_value) in ('none','no') then 'false'
        else null
      end as norm,
      lower(parsed_value) = 'seasonal' as is_seasonal
    from public.beach_policy_extractions
    where field_name in ('lifeguard','restrooms','outdoor_showers')
      and parse_succeeded = true
      and (p_fid is null or arena_group_id = p_fid)
  ),
  tallied as (
    select gold_fid, field_name,
      count(*) filter (where norm = 'true')  as n_true,
      count(*) filter (where norm = 'false') as n_false,
      count(*) filter (where norm is null)   as n_unk,
      bool_or(is_seasonal and norm = 'true') as any_seasonal,
      count(*) as n_total
    from normalized
    group by 1,2
  ),
  decided as (
    select gold_fid, field_name, any_seasonal,
      case
        when n_true > n_false then 'true'
        when n_false > n_true then 'false'
        else null
      end as decision,
      greatest(n_true, n_false) as n_winning,
      ((n_true + n_false) - greatest(n_true, n_false)) as n_dissent
    from tallied
  ),
  scored as (
    select gold_fid, field_name, decision, any_seasonal, n_winning, n_dissent,
      case
        when decision is null then null
        when n_winning >= 3 and n_dissent = 0 then 0.90
        when n_winning >= 2 and n_dissent = 0 then 0.75
        when n_winning > n_dissent then 0.55
        else 0.40
      end as conf
    from decided
  ),
  -- Map v2 field name → BEP claimed_values key
  per_fid as (
    select gold_fid,
      jsonb_strip_nulls(jsonb_build_object(
        'has_lifeguards', max(case when field_name='lifeguard'       then decision end),
        'has_lifeguards_seasonal',
          case when bool_or(field_name='lifeguard' and any_seasonal) then 'true' end,
        'has_restrooms',  max(case when field_name='restrooms'       then decision end),
        'has_restrooms_seasonal',
          case when bool_or(field_name='restrooms' and any_seasonal) then 'true' end,
        'has_showers',    max(case when field_name='outdoor_showers' then decision end),
        'has_showers_seasonal',
          case when bool_or(field_name='outdoor_showers' and any_seasonal) then 'true' end
      )) as cv,
      max(conf) as row_conf,
      bool_or(n_dissent > 0) as row_dissent
    from scored
    group by gold_fid
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'beach_policy_v2_practical', null, cv,
           row_conf, false, 'derived_url_crawl', 'beach_access', now()
    from per_fid where cv <> '{}'::jsonb and row_conf > 0
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence    is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

-- ─── Dogs emitter (dogs_allowed / leash_policy / public_access) ─────
create or replace function public._emit_evidence_from_beach_policy_dogs_v2(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with normalized as (
    select arena_group_id as gold_fid, field_name,
      case
        -- dogs_allowed: yes/no/seasonal(→mixed)/unclear(→drop)
        when field_name = 'dogs_allowed' then
          case lower(parsed_value)
            when 'yes' then 'yes'
            when 'no'  then 'no'
            when 'seasonal' then 'mixed'
            else null end
        -- leash_policy: off_leash/on_leash/unknown(→drop)
        when field_name = 'leash_policy' then
          case lower(parsed_value)
            when 'off_leash' then 'off_leash'
            when 'on_leash'  then 'on_leash'
            else null end
        -- public_access: yes/restricted/no/unclear(→drop)
        when field_name = 'public_access' then
          case lower(parsed_value)
            when 'yes'        then 'yes'
            when 'restricted' then 'restricted'
            when 'no'         then 'no'
            else null end
      end as norm
    from public.beach_policy_extractions
    where field_name in ('dogs_allowed','leash_policy','public_access')
      and parse_succeeded = true
      and (p_fid is null or arena_group_id = p_fid)
  ),
  -- For each (fid, field) tally each distinct norm value
  tallied as (
    select gold_fid, field_name, norm,
      count(*) as votes,
      sum(count(*)) over (partition by gold_fid, field_name
                          rows between unbounded preceding and unbounded following) as field_total
    from normalized
    where norm is not null
    group by 1,2,3
  ),
  -- Pick the per-field winner — max votes wins
  winner as (
    select distinct on (gold_fid, field_name)
      gold_fid, field_name, norm as winning, votes as n_winning, field_total
    from tallied
    order by gold_fid, field_name, votes desc
  ),
  dissent_by_field as (
    select gold_fid, field_name, (field_total - n_winning) as n_dissent
    from winner
  ),
  scored as (
    select w.gold_fid, w.field_name, w.winning, w.n_winning, d.n_dissent,
      case
        when w.n_winning >= 3 and d.n_dissent = 0 then 0.90
        when w.n_winning >= 2 and d.n_dissent = 0 then 0.75
        when w.n_winning > d.n_dissent             then 0.55
        else                                            0.40
      end as conf
    from winner w
    join dissent_by_field d using (gold_fid, field_name)
  ),
  per_fid as (
    select gold_fid,
      jsonb_strip_nulls(jsonb_build_object(
        'dogs_allowed',  max(case when field_name='dogs_allowed'  then winning end),
        'leash_policy',  max(case when field_name='leash_policy'  then winning end),
        'public_access', max(case when field_name='public_access' then winning end)
      )) as cv,
      max(conf) as row_conf,
      bool_or(n_dissent > 0) as row_dissent
    from scored
    group by gold_fid
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'beach_policy_v2_dogs', null, cv,
           row_conf, false, 'derived_url_crawl', 'beach_access', now()
    from per_fid where cv <> '{}'::jsonb and row_conf > 0
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence    is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $function$;

-- ─── public_access promoter (side-channel update) ────────────────────
-- Named distinctly from the existing promote_canonical_dogs_to_beach_dog_policy
-- (which writes dogs_allowed / leash_policy / off_leash_flag etc.) so the
-- two stay independent. Run after the main dogs promoter.
create or replace function public.promote_dogs_public_access(p_fid bigint default null)
returns table(rows_changed bigint)
language plpgsql
as $function$
declare n int := 0;
begin
  with canon as (
    select e.gold_fid, e.claimed_values
      from public.beach_enrichment_provenance e
     where e.field_group = 'dogs' and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  supp as (
    select gold_fid, jsonb_object_agg(k, v) as extra
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
  merged as (
    select c.gold_fid,
           coalesce(s.extra, '{}'::jsonb) || c.claimed_values as cv
      from canon c
      left join supp s on s.gold_fid = c.gold_fid
  ),
  updated as (
    update public.beach_dog_policy bdp
       set public_access = m.cv->>'public_access'
      from merged m
      join public.arena a on a.fid = m.gold_fid
     where bdp.arena_group_id = a.group_id
       and bdp.source <> 'manual_curator'
       and (m.cv->>'public_access') is not null
       and (bdp.public_access is distinct from m.cv->>'public_access')
    returning bdp.arena_group_id
  )
  select count(*) into n from updated;
  return query select n::bigint;
end $function$;

commit;
