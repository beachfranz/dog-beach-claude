-- 20260508_bep_source_catalog.sql
--
-- Registry of BEP source tags + their semantics. Surfaced 2026-05-08 by
-- the 50-row simulation: source naming is inconsistent ('cpad' vs
-- 'cpad_unit_dogs_policy_v1' are different signals; 'city_policy' vs
-- 'city_dog_policy' tripped the simulation diagnostic). This table
-- gives every reader of BEP a documented taxonomy.
--
-- Plus a view that joins coverage stats so we can monitor each source.

begin;

create table if not exists public.bep_source_catalog (
  source text primary key,
  kind text not null check (kind in ('raw_spatial', 'raw_extraction', 'derived', 'llm', 'manual', 'unknown')),
  field_groups text[] not null,
  emitter_function text,
  is_active boolean not null default true,
  description text,
  notes text,
  created_at timestamptz default now()
);

-- Seed with known sources (compiled from emitter audit + actual BEP data 2026-05-08)
insert into public.bep_source_catalog (source, kind, field_groups, emitter_function, description) values
  -- Raw spatial overlays
  ('cpad',                          'raw_spatial',    array['governance','access'],         'populate_from_cpad_gold',                      'CPAD polygon containment + name match'),
  ('pad_us',                        'raw_spatial',    array['governance','access'],         'populate_from_pad_us_gold',                    'PAD-US polygon containment (national, parallel to CPAD)'),
  ('park_operators',                'raw_spatial',    array['governance'],                  'populate_from_park_operators_gold',            'Legacy CSP-park-only path. Mostly retired.'),
  ('tiger_places',                  'raw_spatial',    array['governance'],                  'populate_jurisdictions_containment_gold',      'TIGER incorporated places (cities)'),
  ('csp_parks',                     'raw_spatial',    array['governance'],                  'populate_jurisdictions_containment_gold',      'California State Parks polygon containment'),
  ('nps_places',                    'raw_spatial',    array['governance'],                  'populate_jurisdictions_containment_gold',      'National Park Service spatial'),
  ('military_bases',                'raw_spatial',    array['governance'],                  'populate_military_containment_gold',           'DoD military base polygons'),
  ('tribal_lands',                  'raw_spatial',    array['governance'],                  'populate_tribal_containment_gold',             'BIA tribal land polygons'),
  ('osm_amenities_v1',              'raw_spatial',    array['practical'],                   '_emit_evidence_from_osm_amenities',            'OSM amenity nodes/ways spatial join'),
  ('operator_amenities_v1',         'derived',        array['practical'],                   '_emit_evidence_from_operator_amenity_claims',  'Curated operator amenity claims'),
  ('plz',                           'raw_spatial',    array['governance'],                  '?',                                            'Public Land Zones spatial (sparse)'),

  -- Operator-derived signals
  ('operator_dogs_policy_v1',       'derived',        array['dogs'],                        '_emit_evidence_from_operator_dogs_policy',     'Operator-table dog rules via spatial intersect'),
  ('operator_policy_exceptions_v1', 'derived',        array['dogs'],                        '_emit_evidence_from_operator_policy_exceptions','Operator-level exceptions (carve-outs)'),
  ('city_policy',                   'derived',        array['dogs'],                        'populate_from_city_dog_policy_gold',           'City-jurisdiction default rule (via c1_jurisdiction_id)'),
  ('county_policy',                 'derived',        array['dogs'],                        'populate_from_county_dog_policy_gold',         'County-jurisdiction default rule'),
  ('cpad_unit_dogs_policy_v1',      'derived',        array['dogs'],                        '_emit_evidence_from_cpad_unit_dogs_policy',    'CPAD-unit-keyed dog policy'),
  ('pad_us_dogs_policy_v1',         'derived',        array['dogs'],                        '_emit_evidence_from_pad_us_dogs_policy',       'PAD-US-unit-keyed dog policy (parallel to cpad_unit_dogs_policy_v1)'),

  -- LLM extractions
  ('old_school_llm',                'llm',            array['dogs','practical'],            'external (extract_for_orphans.py)',            'Original Anthropic-extraction pipeline output'),
  ('research',                      'llm',            array['dogs','practical'],            'populate_from_research_gold',                  'LLM research extraction (policy_research_extractions)'),
  ('unified_v1',                    'llm',            array['dogs','practical'],            'populate_from_unified_v1_gold',                'Unified v1 LLM extraction path'),
  ('text_repass_v1',                'llm',            array['dogs','practical'],            'external (text_repass)',                       'LLM text re-pass; richer time_windows + zone_rules'),

  -- Park-URL-derived
  ('park_url',                      'derived',        array['dogs','practical'],            'populate_from_park_url_gold',                  'Park-website scrape; structured rule extraction'),
  ('park_url_buffer_attribution',   'derived',        array['governance'],                  'populate_from_park_url_governance_gold',       'URL-attribution within geographic buffer'),
  ('park_url_raw_amenities_v1',     'raw_extraction', array['practical'],                   'external',                                     'Raw amenity bullets from park URL pages'),

  -- Zone-rules derivation
  ('zone_rules_derived_v1',         'derived',        array['dogs'],                        'external',                                     'Zone-rules → dogs_allowed/leash binary derivation'),
  ('zone_rules_practical_v1',       'derived',        array['practical'],                   'external',                                     'Zone-rules → practical (has_*) derivation'),

  -- Manual / heuristic
  ('manual_curator',                'manual',         array['dogs','practical','governance'],'(admin tooling)',                              'Curator-blessed truth — never auto-overridden'),
  ('manual',                        'manual',         array['governance','dogs','practical'],'(seed_arena_beach.py + bulk seeds)',           'Manual seeds'),
  ('name',                          'derived',        array['governance'],                  '(heuristic)',                                  'Beach name implies jurisdiction (City Beach, State Beach)'),
  ('json_explode',                  'derived',        array['dogs','practical'],            '(parser)',                                     'Multi-key JSON payload exploder')
on conflict (source) do nothing;

-- View: BEP coverage per source, joined to catalog metadata.
create or replace view public.bep_source_coverage_v as
  select
    c.source,
    c.kind,
    c.field_groups,
    c.is_active                                   as catalog_active,
    c.emitter_function,
    coalesce(s.bep_rows, 0)                       as bep_rows,
    coalesce(s.beaches_covered, 0)                as beaches_covered,
    coalesce(s.distinct_field_groups, 0)          as distinct_field_groups,
    c.description
  from public.bep_source_catalog c
  left join (
    select source, count(*) as bep_rows,
           count(distinct gold_fid) as beaches_covered,
           count(distinct field_group) as distinct_field_groups
      from public.beach_enrichment_provenance
     group by source
  ) s on s.source = c.source
  order by beaches_covered desc nulls last;

grant select on public.bep_source_coverage_v to anon, authenticated, service_role;

commit;
