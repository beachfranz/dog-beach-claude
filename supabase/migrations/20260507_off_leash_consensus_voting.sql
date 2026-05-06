-- 20260507_off_leash_consensus_voting.sql
--
-- Promotes off_leash_exists from canonical-row-only to consensus-voted.
-- Today the promoter reads off_leash_flag from a single canonical BEP
-- row's claimed_values; that row is determined by static
-- _gold_field_group_source_priority. None of the four 2026-05-06/07
-- emitters (cpad/pad_us/operator/zone_rules_derived) are in the
-- priority list, so they default to priority 99 — never canonical.
-- Result: zone_rules-derived off_leash signals are written but never
-- flow to beach_dog_policy.off_leash_flag (1 of 19 ground-truth match).
--
-- This migration:
--   1. Fixes _emit_evidence_from_zone_rules_derived's derivation rule —
--      ANY on-beach off_leash section → off_leash_exists='true', else
--      'false' if any conclusive on-beach signal, else null. Was
--      "all on-beach off_leash" (too strict).
--   2. Adds off_leash_exists to consensus_field_config so the consensus
--      function processes it across all sources.
--   3. Extends _consensus_normalize for field_kind='off_leash_flag'.
--   4. Updates promote_canonical_dogs_to_beach_dog_policy to read
--      off_leash_flag from beach_field_consensus, not from the canonical
--      row's claimed_values.
--
-- Net effect: zone_rules-derived off_leash votes flow to the flat field.
-- The 19 leash_policy='off_leash' beaches should align with off_leash_flag=true.

begin;

-- ── 1. Update zone_rules_derived emitter — broaden off_leash derivation
create or replace function public._emit_evidence_from_zone_rules_derived(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare
  ins int := 0; upd int := 0; skp int := 0;
begin
  with parent as (
    select distinct on (e.gold_fid)
      e.gold_fid, e.confidence as parent_conf, e.source as parent_source,
      e.source_url as parent_url, e.claimed_values->'zone_rules' as zr
    from public.beach_enrichment_provenance e
    where e.field_group = 'dogs'
      and e.claimed_values ? 'zone_rules'
      and e.claimed_values->'zone_rules' is not null
      and (p_fid is null or e.gold_fid = p_fid)
    order by e.gold_fid, e.confidence desc nulls last
  ),
  sections_v2 as (
    select p.gold_fid, p.parent_conf, p.parent_source, p.parent_url,
           section.key as section_name, section.value->>'rule' as rule
      from parent p,
           jsonb_array_elements(p.zr->'seasons') s,
           jsonb_array_elements(s.value->'regions') r,
           jsonb_each(r.value->'sections') section
     where p.zr ? 'seasons'
  ),
  sections_v1 as (
    select p.gold_fid, p.parent_conf, p.parent_source, p.parent_url,
           section.key, section.value->>'rule'
      from parent p,
           jsonb_array_elements(p.zr->'regions') r,
           jsonb_each(r.value->'sections') section
     where not (p.zr ? 'seasons') and (p.zr ? 'regions')
  ),
  sections as (select * from sections_v2 union all select * from sections_v1),
  per_beach as (
    select
      gold_fid, max(parent_conf) as parent_conf, max(parent_source) as parent_source,
      max(parent_url) as parent_url,
      bool_or(rule in ('on_leash','off_leash') and section_name in
        ('sand','water_swim','tide_pools','dunes','bluff','boardwalk'))
        as any_on_beach_allowed,
      bool_or(rule in ('on_leash','off_leash') and section_name in
        ('parking_lot','picnic_area','restrooms_showers','campground','trails','playground'))
        as any_perimeter_allowed,
      -- Broader: any on-beach section that's off_leash → flag true
      bool_or(rule = 'off_leash' and section_name in
        ('sand','water_swim','tide_pools','dunes','bluff','boardwalk'))
        as any_on_beach_off_leash,
      -- All-on-leash check still useful for dogs_allowed='yes' derivation
      coalesce(bool_and(rule = 'on_leash')
        filter (where section_name in
          ('sand','water_swim','tide_pools','dunes','bluff','boardwalk')), false)
        as all_on_beach_on_leash,
      coalesce(bool_and(rule = 'off_leash')
        filter (where section_name in
          ('sand','water_swim','tide_pools','dunes','bluff','boardwalk')), false)
        as all_on_beach_off_leash,
      -- Conclusive on-beach signal exists?
      bool_or(rule in ('on_leash','off_leash','not_allowed') and section_name in
        ('sand','water_swim','tide_pools','dunes','bluff','boardwalk'))
        as any_on_beach_conclusive,
      count(*) filter (where rule in ('on_leash','off_leash','not_allowed'))
        as conclusive_section_count
    from sections group by gold_fid
  ),
  derived as (
    select
      gold_fid, parent_conf, parent_source, parent_url, conclusive_section_count,
      -- dogs_allowed derivation (unchanged)
      case
        when any_on_beach_allowed and all_on_beach_off_leash then 'yes'
        when any_on_beach_allowed and all_on_beach_on_leash  then 'yes'
        when any_on_beach_allowed                            then 'mixed'
        when any_perimeter_allowed                           then 'no'
        else null
      end as derived_allowed,
      -- off_leash_exists derivation (broadened)
      case
        when any_on_beach_off_leash      then 'true'
        when any_on_beach_conclusive     then 'false'
        else null
      end as off_leash_exists
    from per_beach where conclusive_section_count > 0
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select
      d.gold_fid, 'dogs', 'zone_rules_derived_v1', d.parent_url,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',             d.derived_allowed,
        'off_leash_exists',    d.off_leash_exists,
        'derivation_parent',   d.parent_source,
        'conclusive_sections', d.conclusive_section_count
      )),
      d.parent_conf, false,
      'derived_url_crawl', 'beach_access', now()
    from derived d
    where d.derived_allowed is not null or d.off_leash_exists is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          source_url     = excluded.source_url,
          updated_at     = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $$;

-- ── 2. Register off_leash_exists in consensus_field_config
insert into public.consensus_field_config (field_group, claim_key, field_kind)
values ('dogs', 'off_leash_exists', 'off_leash_flag')
on conflict (field_group, claim_key) do update
  set field_kind = excluded.field_kind;

-- ── 3. Extend _consensus_normalize for field_kind='off_leash_flag'
create or replace function public._consensus_normalize(p_field text, p_value text)
returns text language sql immutable as $$
  select case
    when p_value is null then null
    when lower(trim(p_value)) in ('unclear','unknown','null','none','') then null
    else case p_field
      when 'dogs_allowed' then case lower(trim(p_value))
        when 'yes' then 'yes'
        when 'no'  then 'no'
        when 'restricted' then 'mixed'   -- retired 2026-05-05
        when 'seasonal'   then 'mixed'   -- retired 2026-05-07
        when 'mixed' then 'mixed'
        else null end
      when 'leash_policy' then case lower(trim(p_value))
        when 'on_leash' then 'on_leash' when 'off_leash' then 'off_leash'
        when 'mixed' then 'mixed' when 'mixed_by_zone' then 'mixed'
        when 'required' then 'on_leash' when 'optional' then 'off_leash'
        when 'varies_by_time' then 'varies_by_time'
        else null end
      when 'off_leash_flag' then case lower(trim(p_value))
        when 'true' then 'true' when 'yes' then 'true' when '1' then 'true'
        when 'false' then 'false' when 'no' then 'false' when '0' then 'false'
        else null end
      when 'has_lifeguards' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_restrooms' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_parking' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_showers' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_disabled_access' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      else lower(trim(p_value))
    end
  end;
$$;

-- ── 4. Update promoter to read off_leash_flag from consensus
create or replace function public.promote_canonical_dogs_to_beach_dog_policy(p_fid bigint default null::bigint, p_min_confidence numeric default 0.5)
 returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
 language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with consensus_dogs as (
    select gold_fid,
      max(winning_value) filter (where field_name = 'allowed')        as v_allowed,
      max(confidence)    filter (where field_name = 'allowed')        as conf_allowed,
      bool_or(disagreement) filter (where field_name = 'allowed')     as dis_allowed,
      max(winning_value) filter (where field_name = 'leash_required') as v_leash,
      max(confidence)    filter (where field_name = 'leash_required') as conf_leash,
      bool_or(disagreement) filter (where field_name = 'leash_required') as dis_leash,
      max(winning_value) filter (where field_name = 'off_leash_exists') as v_off_leash
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
  derived as (
    select c.gold_fid as fid,
           public._norm_dogs_allowed(c.v_allowed)        as new_dogs_allowed,
           public._norm_leash_policy(c.v_leash)          as new_leash_policy,
           public._norm_bool(to_jsonb(c.v_off_leash))    as new_off_leash_flag,
           public._norm_time_text(split_part(p.claimed_values->>'time_prohibited_hours','-',1)) as new_prohib_start,
           public._norm_time_text(split_part(p.claimed_values->>'time_prohibited_hours','-',2)) as new_prohib_end,
           coalesce(p.claimed_values->>'areas_evidence',
                    p.claimed_values->>'areas_boundaries',
                    p.claimed_values->>'designated_dog_zones',
                    p.claimed_values->>'prohibited_areas') as new_dogs_allowed_areas,
           greatest(c.conf_allowed, c.conf_leash) as new_confidence,
           coalesce(c.dis_allowed, false) or coalesce(c.dis_leash, false) as new_disagreement
      from consensus_dogs c
      left join canon_payload p on p.fid = c.gold_fid
  ),
  protected as (
    select arena_group_id from public.beach_dog_policy where source = 'manual_curator'
  ),
  upsert as (
    insert into public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       dogs_prohibited_start, dogs_prohibited_end, dogs_allowed_areas,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select d.fid, d.new_dogs_allowed, d.new_leash_policy, d.new_off_leash_flag,
           d.new_prohib_start, d.new_prohib_end, d.new_dogs_allowed_areas,
           'auto_promoted_from_consensus', now(),
           format('Phase B consensus: dogs_allowed=%s (conf=%s%s), leash=%s (conf=%s%s)',
                  d.new_dogs_allowed, coalesce(d.new_confidence, 0),
                  case when d.new_disagreement then ', DISAGREEMENT' else '' end,
                  d.new_leash_policy, coalesce(d.new_confidence, 0),
                  case when d.new_disagreement then ', DISAGREEMENT' else '' end),
           d.new_confidence, d.new_disagreement
      from derived d
     where d.fid not in (select arena_group_id from protected)
       and (d.new_dogs_allowed is not null or d.new_leash_policy is not null
            or d.new_off_leash_flag is not null or d.new_prohib_start is not null
            or d.new_dogs_allowed_areas is not null)
       and coalesce(d.new_confidence, 1) >= p_min_confidence
    on conflict (arena_group_id) do update
      set dogs_allowed=excluded.dogs_allowed,
          leash_policy=excluded.leash_policy,
          off_leash_flag=excluded.off_leash_flag,
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
