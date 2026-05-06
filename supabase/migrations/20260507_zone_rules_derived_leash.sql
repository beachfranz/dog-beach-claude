-- 20260507_zone_rules_derived_leash.sql
--
-- Extends _emit_evidence_from_zone_rules_derived to also vote on
-- `leash_required` (which consensus_field_config maps to field_kind
-- 'leash_policy'). Same source row, additional claim_key in the
-- claimed_values payload.
--
-- Derivation rule:
--   * any section has time_windows                        → 'varies_by_time'
--   * all on-beach sections off_leash (≥1 present)        → 'off_leash'
--   * all on-beach sections on_leash (≥1 present)         → 'on_leash'
--   * mix of on_leash + off_leash within same beach       → 'mixed'
--   * else null (skip)
--
-- Inherits confidence from parent BEP row (text_repass_v1, etc.).

begin;

create or replace function public._emit_evidence_from_zone_rules_derived(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare ins int := 0; upd int := 0; skp int := 0;
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
           section.key as section_name, section.value->>'rule' as rule,
           (section.value ? 'time_windows') as has_time_window
      from parent p,
           jsonb_array_elements(p.zr->'seasons') s,
           jsonb_array_elements(s.value->'regions') r,
           jsonb_each(r.value->'sections') section
     where p.zr ? 'seasons'
  ),
  sections_v1 as (
    select p.gold_fid, p.parent_conf, p.parent_source, p.parent_url,
           section.key, section.value->>'rule',
           (section.value ? 'time_windows')
      from parent p,
           jsonb_array_elements(p.zr->'regions') r,
           jsonb_each(r.value->'sections') section
     where not (p.zr ? 'seasons') and (p.zr ? 'regions')
  ),
  sections as (select * from sections_v2 union all select * from sections_v1),
  per_beach as (
    select
      gold_fid,
      max(parent_conf)   as parent_conf,
      max(parent_source) as parent_source,
      max(parent_url)    as parent_url,

      -- on-beach allow flags (used for dogs_allowed + leash_policy derivation)
      bool_or(rule in ('on_leash','off_leash') and section_name in
        ('sand','water_swim','tide_pools','dunes','bluff','boardwalk'))
        as any_on_beach_allowed,
      bool_or(rule in ('on_leash','off_leash') and section_name in
        ('parking_lot','picnic_area','restrooms_showers','campground','trails','playground'))
        as any_perimeter_allowed,
      bool_or(rule = 'off_leash' and section_name in
        ('sand','water_swim','tide_pools','dunes','bluff','boardwalk'))
        as any_on_beach_off_leash,
      bool_or(rule = 'on_leash' and section_name in
        ('sand','water_swim','tide_pools','dunes','bluff','boardwalk'))
        as any_on_beach_on_leash,
      coalesce(bool_and(rule = 'on_leash')
        filter (where section_name in
          ('sand','water_swim','tide_pools','dunes','bluff','boardwalk')), false)
        as all_on_beach_on_leash,
      coalesce(bool_and(rule = 'off_leash')
        filter (where section_name in
          ('sand','water_swim','tide_pools','dunes','bluff','boardwalk')), false)
        as all_on_beach_off_leash,
      bool_or(rule in ('on_leash','off_leash','not_allowed') and section_name in
        ('sand','water_swim','tide_pools','dunes','bluff','boardwalk'))
        as any_on_beach_conclusive,
      count(*) filter (where rule in ('on_leash','off_leash','not_allowed'))
        as conclusive_section_count,

      -- time-window detection (for varies_by_time leash_policy)
      bool_or(has_time_window and section_name in
        ('sand','water_swim','tide_pools','dunes','bluff','boardwalk'))
        as any_on_beach_time_varying
    from sections group by gold_fid
  ),
  derived as (
    select
      gold_fid, parent_conf, parent_source, parent_url, conclusive_section_count,

      -- dogs_allowed (unchanged)
      case
        when any_on_beach_allowed and all_on_beach_off_leash then 'yes'
        when any_on_beach_allowed and all_on_beach_on_leash  then 'yes'
        when any_on_beach_allowed                            then 'mixed'
        when any_perimeter_allowed                           then 'no'
        else null
      end as derived_allowed,

      -- off_leash_exists (unchanged from prior fix)
      case
        when any_on_beach_off_leash      then 'true'
        when any_on_beach_conclusive     then 'false'
        else null
      end as off_leash_exists,

      -- leash_required (NEW — derives leash_policy via consensus normalize)
      case
        when any_on_beach_time_varying                       then 'varies_by_time'
        when all_on_beach_off_leash                          then 'off_leash'
        when any_on_beach_off_leash and any_on_beach_on_leash then 'mixed'
        when all_on_beach_on_leash                           then 'on_leash'
        when any_on_beach_off_leash                          then 'off_leash'
        when any_on_beach_on_leash                           then 'on_leash'
        else null
      end as leash_required
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
        'leash_required',      d.leash_required,
        'derivation_parent',   d.parent_source,
        'conclusive_sections', d.conclusive_section_count
      )),
      d.parent_conf, false,
      'derived_url_crawl', 'beach_access', now()
    from derived d
    where d.derived_allowed is not null
       or d.off_leash_exists is not null
       or d.leash_required is not null
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

commit;
