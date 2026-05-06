-- 20260507_operator_amenity_claims_and_fire_pit_prose.sql
--
-- Pin #11 partial: two operator-pattern fixes for the practical field group.
--
-- Part 1 — Operator-level lifeguard signal:
--   Many CA beaches have professional lifeguard services as operators
--   (City of Newport Beach, LA County Beaches & Harbors, San Diego
--   Lifeguard Service, etc.). OSM tagging is sparse here (5 lifeguard
--   towers in all of CA). The real signal: the operator IS the lifeguard
--   service. New table `operator_amenity_claims` lets us hand-curate
--   per-operator amenity claims (start with has_lifeguards). New emitter
--   _emit_evidence_from_operator_amenities votes for every beach inside
--   a flagged operator's polygon.
--
-- Part 2 — Fire pit prose mining:
--   leisure=fire_pit OSM tag is sparse (0 hits in CA). But the prose
--   from text_repass_v1 / park_url / research extractions frequently
--   mentions "fire rings," "fire pits," "BBQ pits." Extend the existing
--   _emit_evidence_from_zone_rules_practical to also keyword-match the
--   parent BEP row's notes / global_notes for fire-pit terms. Same source
--   value (zone_rules_practical_v1), incremental claim_keys.

begin;

-- ────────────────────────────────────────────────────────────────────────
-- Part 1: operator_amenity_claims table + emitter
-- ────────────────────────────────────────────────────────────────────────

create table if not exists public.operator_amenity_claims (
  operator_id    bigint primary key references public.operators(id) on delete cascade,
  has_lifeguards boolean,
  has_lifeguards_seasonal_only boolean,
  has_lifeguards_notes text,
  has_restrooms  boolean,        -- reserved for future use
  has_showers    boolean,        -- reserved for future use
  source_url     text,
  notes          text,
  curated_at     timestamptz default now()
);

comment on table public.operator_amenity_claims is
  'Hand-curated operator-level amenity claims. has_lifeguards=true is the
   primary use case: an operator that IS a lifeguard service (or whose
   jurisdiction is uniformly lifeguarded) emits has_lifeguards=true for
   every beach inside its polygon. Other amenities are reserved.';

-- Seed with known SoCal coastal lifeguard-service operators.
-- These cities/counties operate professional ocean lifeguards covering
-- their entire beach jurisdictions. Source: each city's official site.
insert into public.operator_amenity_claims
  (operator_id, has_lifeguards, has_lifeguards_seasonal_only, has_lifeguards_notes, notes)
select id, true, false,
       canonical_name || ' operates professional ocean lifeguards covering its beach jurisdictions',
       'Hand-curated 2026-05-07 (pin #11 part 1). Source: city/county official sites.'
  from public.operators
 where canonical_name in (
    'City of Long Beach',                                              -- Long Beach Marine Patrol
    'City of Newport Beach',                                           -- Newport Beach Lifeguards
    'City of Huntington Beach',                                        -- HB Marine Safety
    'City of Laguna Beach',                                            -- Laguna Beach Marine Safety
    'City of Manhattan Beach',                                         -- LA County contracts cover beaches; redundant but cheap
    'City of Hermosa Beach',
    'City of Redondo Beach',
    'City of Santa Monica',                                            -- LA County Lifeguards staff
    'City of San Diego',                                               -- San Diego Lifeguard Service
    'Los Angeles County Department of Beaches and Harbors, County of'  -- covers ~80 beaches
   )
on conflict (operator_id) do update
  set has_lifeguards = excluded.has_lifeguards,
      has_lifeguards_seasonal_only = excluded.has_lifeguards_seasonal_only,
      has_lifeguards_notes = excluded.has_lifeguards_notes,
      curated_at = now();

-- Add operator_amenities_v1 to BEP source CHECK constraint
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
    'operator_dogs_policy_v1','zone_rules_derived_v1',
    'zone_rules_practical_v1','osm_amenities_v1',
    'operator_policy_exceptions_v1','operator_amenities_v1'
  ]));

create or replace function public._emit_evidence_from_operator_amenities(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with ranked as (
    select bg.fid as gold_fid, o.id as operator_id, o.canonical_name,
           oac.has_lifeguards, oac.has_restrooms, oac.has_showers,
           oac.has_lifeguards_seasonal_only,
           oac.has_lifeguards_notes, oac.source_url,
           o.footprint_area_km2,
           row_number() over (
             partition by bg.fid
             order by coalesce(o.footprint_area_km2, 1e9) asc  -- smallest operator wins
           ) as rk
      from public.beaches_gold bg
      join public.operators o on ST_Intersects(o.geom, bg.geom)
      join public.operator_amenity_claims oac on oac.operator_id = o.id
     where bg.is_active and (p_fid is null or bg.fid = p_fid)
       and (oac.has_lifeguards is not null
         or oac.has_restrooms is not null
         or oac.has_showers is not null)
  ),
  picks as (select * from ranked where rk = 1),
  payload as (
    select
      gold_fid, canonical_name, source_url,
      jsonb_strip_nulls(jsonb_build_object(
        'has_lifeguards', case when has_lifeguards is true then 'true'
                               when has_lifeguards is false then 'false' else null end,
        'has_restrooms',  case when has_restrooms  is true then 'true'
                               when has_restrooms  is false then 'false' else null end,
        'has_showers',    case when has_showers    is true then 'true'
                               when has_showers    is false then 'false' else null end,
        'lifeguards_seasonal', case when has_lifeguards_seasonal_only is true then 'true' else null end,
        'lifeguards_notes', has_lifeguards_notes,
        'operator_id', operator_id::text
      )) as cv
    from picks
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, cpad_unit_name, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'operator_amenities_v1', source_url, cv,
           0.80,           -- hand-curated; high confidence; outranks osm_amenities_v1 (0.65)
           false, canonical_name, 'derived_url_crawl', 'beach_access', now()
      from payload where cv <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          source_url = excluded.source_url,
          cpad_unit_name = excluded.cpad_unit_name,
          updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $$;

grant execute on function public._emit_evidence_from_operator_amenities(bigint)
  to authenticated, service_role;


-- ────────────────────────────────────────────────────────────────────────
-- Part 2: extend zone_rules_practical_v1 to mine prose for fire-pit terms
-- ────────────────────────────────────────────────────────────────────────
-- The existing emitter sets has_parking / has_restrooms / has_showers /
-- has_picnic_area from section presence. We extend it to ALSO mine the
-- parent BEP row's notes + global_notes for fire-pit / BBQ keywords.
-- Same emitter, same source, additional claim_key.

create or replace function public._emit_evidence_from_zone_rules_practical(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with parent as (
    select distinct on (e.gold_fid)
      e.gold_fid, e.confidence as parent_conf, e.source as parent_source,
      e.source_url as parent_url,
      e.claimed_values->'zone_rules' as zr,
      coalesce(e.claimed_values->>'notes', '') ||
        coalesce(e.claimed_values->'zone_rules'->>'global_notes', '') as parent_prose
    from public.beach_enrichment_provenance e
    where e.field_group = 'dogs'
      and e.claimed_values ? 'zone_rules'
      and e.claimed_values->'zone_rules' is not null
      and (p_fid is null or e.gold_fid = p_fid)
    order by e.gold_fid, e.confidence desc nulls last
  ),
  sections_v2 as (
    select p.gold_fid, p.parent_conf, p.parent_source, p.parent_url, p.parent_prose,
           section.key as section_name
      from parent p,
           jsonb_array_elements(p.zr->'seasons') s,
           jsonb_array_elements(s.value->'regions') r,
           jsonb_each(r.value->'sections') section
     where p.zr ? 'seasons'
  ),
  sections_v1 as (
    select p.gold_fid, p.parent_conf, p.parent_source, p.parent_url, p.parent_prose,
           section.key
      from parent p,
           jsonb_array_elements(p.zr->'regions') r,
           jsonb_each(r.value->'sections') section
     where not (p.zr ? 'seasons') and (p.zr ? 'regions')
  ),
  sections as (select * from sections_v2 union all select * from sections_v1),
  per_beach as (
    select
      gold_fid,
      max(parent_conf) as parent_conf,
      max(parent_source) as parent_source,
      max(parent_url) as parent_url,
      max(parent_prose) as parent_prose,
      bool_or(section_name = 'parking_lot')        as has_parking,
      bool_or(section_name = 'restrooms_showers')  as has_restrooms_showers_section,
      bool_or(section_name = 'picnic_area')        as has_picnic
    from sections group by gold_fid
  ),
  payload as (
    select
      gold_fid, parent_conf, parent_source, parent_url, parent_prose,
      jsonb_strip_nulls(jsonb_build_object(
        'has_parking',     case when has_parking then 'true' else null end,
        'has_restrooms',   case when has_restrooms_showers_section then 'true' else null end,
        'has_showers',     case when has_restrooms_showers_section then 'true' else null end,
        'has_picnic_area', case when has_picnic then 'true' else null end,
        -- NEW: fire-pit prose mining
        'has_fire_pits',   case when parent_prose ~* '\b(fire pits?|fire rings?|barbeque|barbecue|bbq pits?)\b'
                                 then 'true' else null end,
        'derivation_parent', parent_source
      )) as cv
    from per_beach
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select
      p.gold_fid, 'practical', 'zone_rules_practical_v1', p.parent_url,
      p.cv, p.parent_conf, false, 'derived_url_crawl', 'beach_access', now()
    from payload p
    where (p.cv - 'derivation_parent') <> '{}'::jsonb
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
