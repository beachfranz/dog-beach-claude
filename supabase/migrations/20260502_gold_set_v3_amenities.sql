-- Gold set v3 follow-up:
--   1. Add 3 canonical amenity variants: lifeguard, restrooms, outdoor_showers.
--   2. Re-issue feature_zones with 7 sections (campgrounds replaces
--      playgrounds; restrooms_showers removed — restrooms is now an
--      amenity, showers is its own amenity).
--   3. Re-issue sections variant referencing the same 7 feature types.
--   4. Deactivate the prior 8-feature variant.

begin;

-- 1. Amenities — all enum, run on Haiku for cost
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('lifeguard', 'enum_v1',
   'Does this beach have a lifeguard on duty?

Choose exactly one:
- full_time: lifeguards present year-round during daylight hours
- seasonal: lifeguards present only in summer / peak season
- none: no lifeguards
- unknown: source does not specify

Return only the chosen value.',
   'enum', 'claude-haiku-4-5-20251001', true, true,
   'Beach-wide amenity. Look for "lifeguard tower", "lifeguarded", "no lifeguard on duty" phrases.'),

  ('restrooms', 'enum_v1',
   'Does this beach have public restrooms accessible to visitors?

Choose exactly one:
- yes: restrooms available year-round
- seasonal: restrooms open only in summer / peak season (port-a-potties or seasonal facilities)
- none: no public restrooms
- unknown: source does not specify

Return only the chosen value.',
   'enum', 'claude-haiku-4-5-20251001', true, true,
   'Beach-wide amenity. Includes both permanent restrooms and seasonal port-a-potties.'),

  ('outdoor_showers', 'enum_v1',
   'Does this beach have outdoor cold-water showers (rinse stations) at the access point — for rinsing salt water and sand off after swimming?

Choose exactly one:
- yes: outdoor showers / rinse stations available year-round
- seasonal: outdoor showers active only in summer / peak season
- none: no outdoor showers
- unknown: source does not specify

Return only the chosen value.',
   'enum', 'claude-haiku-4-5-20251001', true, true,
   'Beach-wide amenity. Particularly useful for rinsing dogs after a salt-water swim. Distinct from indoor restroom showers.')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape,
      notes = excluded.notes;

-- 2. Feature zones — 7-section version (campgrounds replaces playgrounds,
--    restrooms_showers removed). Use a fresh variant_key so the schema
--    change is auditable.
insert into public.extraction_prompt_variants
  (field_name, variant_key, prompt_template, expected_shape, target_model,
   active, is_canon, notes)
values
  ('feature_zones', 'closed_7feat_v1',
   'For this beach (treated as a single uniform area), report the dog rule for each of these 7 section types. Use only the listed status values.

Section types (return all 7):
- sand: the open beach / sand area
- parking_lot: the lot, drop-off area, the path from car to beach
- trails_boardwalk: any boardwalks, paths, or cliff trails leading to the beach
- picnic_areas: tables, grassy areas, designated picnic spots
- campgrounds: any adjacent campground / camping area
- water_swim: the water itself / lifeguarded swim zones
- food_concession: snack bars, restaurants, food carts on-site

Status values:
- off_leash: dogs allowed off-leash
- on_leash: dogs allowed only on-leash
- not_allowed: dogs not allowed in this section
- seasonal: rules vary by season
- unknown: source does not specify

Return JSON: {"sand": "off_leash", "parking_lot": "on_leash", ...} (all 7 keys present)',
   'structured_json', 'claude-sonnet-4-6', true, true,
   'v3 feature-zone schema with 7 sections (campgrounds replaces playgrounds; restrooms/showers split out as amenities).')
on conflict (field_name, variant_key) do update
  set prompt_template = excluded.prompt_template,
      is_canon = excluded.is_canon,
      active = excluded.active,
      target_model = excluded.target_model,
      expected_shape = excluded.expected_shape,
      notes = excluded.notes;

-- Deactivate the 8-feature variant + flip its is_canon off
update public.extraction_prompt_variants
   set active = false, is_canon = false,
       notes = coalesce(notes, '') || ' [superseded by closed_7feat_v1 2026-05-02]'
 where field_name = 'feature_zones'
   and variant_key = 'closed_8feat_v1';

-- 3. sections variant — refresh prompt to point at 7-feature list
update public.extraction_prompt_variants
   set prompt_template =
'This beach has multiple geographically distinct sections with different dog rules. Extract one entry per section.

For each section return:
- name: short descriptive name (e.g. "Dog Beach", "Main Beach", "South of Tower 15")
- geographic_descriptor: how a visitor would locate this section (e.g. "between Goldenwest St and Seapoint Ave", "north of the lifeguard tower #15", "the area marked with off-leash signs")
- leash_policy: one of "off_leash", "on_leash", "leash_optional", "varies_by_time"
- temporal_restrictions: free text describing time-of-day or seasonal rules within this section ("" if none)
- feature_zones: object with status per section type — keys: sand, parking_lot, trails_boardwalk, picnic_areas, campgrounds, water_swim, food_concession; values: "off_leash" / "on_leash" / "not_allowed" / "seasonal" / "unknown"
- evidence_quote: direct quote from the source page that supports the rule for this section

Return JSON: {"sections": [{...}, {...}]}',
       notes = 'Per-section extraction with 7-feature_zone keys. Updated 2026-05-02.'
 where field_name = 'sections' and variant_key = 'structured_v1';

commit;

-- Sanity check (informational):
-- SELECT field_name, variant_key, is_canon, active
--   FROM public.extraction_prompt_variants
--  WHERE field_name IN ('lifeguard','restrooms','outdoor_showers',
--                       'feature_zones','sections')
--  ORDER BY field_name, active DESC, is_canon DESC;
