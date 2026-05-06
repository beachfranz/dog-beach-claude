-- 20260507_emit_zone_rules_derived.sql
--
-- Fourth emitter in the unified BEP pipeline. Derives a flat-field
-- `dogs_allowed` vote from existing zone_rules JSONB and writes back
-- as its own BEP row. Closes the architectural gap where structured
-- zone_rules and flat-field dogs_allowed were computed independently
-- and could disagree (47 contradictions found 2026-05-07).
--
-- Derivation rule (mirrors the on-beach / perimeter split):
--   * All on-beach sections (sand/water/tide_pools/dunes/bluff/boardwalk)
--     `off_leash` → `yes` (with off_leash_flag=true)
--   * All on-beach `on_leash` → `yes`
--   * ≥1 on-beach allowed → `mixed`
--   * Only perimeter sections allowed (parking_lot, picnic, etc.) → `no`
--     (preserves "suck" semantic — parking-only is not real beach access)
--   * All sections unknown → no vote (skip)
--
-- Confidence: inherits the parent BEP row's confidence (the row whose
-- claimed_values.zone_rules is the source of truth — typically
-- text_repass_v1, but also research / unified_v1 / etc. when those
-- carry zone_rules). When multiple parent rows exist, pick the
-- highest-confidence one.
--
-- Why inherit instead of fixed: avoids second-derivation noise. The
-- structural derivation ("all on-beach sections allow → beach allows")
-- is tight, so we project the parent's confidence rather than inventing
-- a new claim.

begin;

-- Allow the new source value
alter table public.beach_enrichment_provenance
  drop constraint beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','manual_curator','plz','cpad','tiger_places','ccc','llm',
    'web_scrape','research','csp_parks','park_operators','nps_places',
    'tribal_lands','military_bases','pad_us','sma_code_mappings',
    'jurisdictions','csp_places','name','governing_body','park_url',
    'park_url_buffer_attribution','park_url_governance','old_school_llm',
    'counties','json_explode','unified_v1','city_policy','county_policy',
    'text_repass_v1','cpad_unit_dogs_policy_v1','pad_us_dogs_policy_v1',
    'operator_dogs_policy_v1','zone_rules_derived_v1'
  ]));

-- ----------------------------------------------------------------------
-- The emitter
-- ----------------------------------------------------------------------
create or replace function public._emit_evidence_from_zone_rules_derived(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare
  ins int := 0; upd int := 0; skp int := 0;
begin
  with parent as (
    -- For each beach, pick the highest-confidence BEP row that carries zone_rules
    select distinct on (e.gold_fid)
      e.gold_fid,
      e.confidence as parent_conf,
      e.source     as parent_source,
      e.source_url as parent_url,
      e.claimed_values->'zone_rules' as zr
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
  sections as (
    select * from sections_v2
    union all select * from sections_v1
  ),
  per_beach as (
    select
      gold_fid,
      max(parent_conf)   as parent_conf,
      max(parent_source) as parent_source,
      max(parent_url)    as parent_url,
      bool_or(rule in ('on_leash','off_leash') and section_name in
        ('sand','water_swim','tide_pools','dunes','bluff','boardwalk'))    as any_on_beach_allowed,
      bool_or(rule in ('on_leash','off_leash') and section_name in
        ('parking_lot','picnic_area','restrooms_showers','campground','trails','playground'))
                                                                            as any_perimeter_allowed,
      -- "all on-beach off_leash" — true only when there's at least one on-beach
      -- section, and every on-beach section is off_leash
      coalesce(bool_and(rule = 'off_leash')
        filter (where section_name in
          ('sand','water_swim','tide_pools','dunes','bluff','boardwalk')), false) as all_on_beach_off_leash,
      coalesce(bool_and(rule = 'on_leash')
        filter (where section_name in
          ('sand','water_swim','tide_pools','dunes','bluff','boardwalk')), false) as all_on_beach_on_leash,
      count(*) filter (where rule in ('on_leash','off_leash','not_allowed')) as conclusive_section_count
    from sections
    group by gold_fid
  ),
  derived as (
    select
      gold_fid, parent_conf, parent_source, parent_url, conclusive_section_count,
      case
        when any_on_beach_allowed and all_on_beach_off_leash then 'yes'
        when any_on_beach_allowed and all_on_beach_on_leash  then 'yes'
        when any_on_beach_allowed                            then 'mixed'
        when any_perimeter_allowed                           then 'no'
        else null
      end as derived_allowed,
      case when all_on_beach_off_leash then 'true' else null end as off_leash_flag
    from per_beach
    where conclusive_section_count > 0
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select
      d.gold_fid, 'dogs', 'zone_rules_derived_v1', d.parent_url,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',             d.derived_allowed,
        'off_leash_exists',    d.off_leash_flag,
        'derivation_parent',   d.parent_source,
        'conclusive_sections', d.conclusive_section_count
      )),
      d.parent_conf,
      false,
      'derived_url_crawl', 'beach_access',
      now()
    from derived d where d.derived_allowed is not null
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

grant execute on function public._emit_evidence_from_zone_rules_derived(bigint)
  to authenticated, service_role;

comment on function public._emit_evidence_from_zone_rules_derived(bigint) is
  'Derives flat-field dogs_allowed votes from zone_rules JSONB. Inherits
   confidence from the parent BEP row carrying the zone_rules.';

commit;
