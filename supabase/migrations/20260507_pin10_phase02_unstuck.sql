-- 20260507_pin10_phase02_unstuck.sql
--
-- Pin #10 (closed): unstick the 3 remaining phase-02 beaches via manual
-- curator entries on cpad_unit_dogs_policy. Plus a critical refinement
-- to _derive_has_off_leash that the work surfaced.
--
-- THE 3 STUCK BEACHES:
--   * China Ladder Beach (8307)   — Greyhound Rock Fishing Access (CPAD 25173)
--   * Greyhound Rock Beach (8998) — same CPAD unit
--   * Collins Lake Beach (8687)   — Collins Lake Recreation Area (CPAD 662)
--
-- Why they were stuck:
--   * cpad_unit_dogs_policy had dogs_allowed='unknown' (extraction failed
--     or thin) on both units. The cpad_unit emitter wrote BEP rows but
--     'unknown' got filtered by _consensus_normalize, so no vote landed.
--   * Re-extraction with --refresh: Greyhound Rock returned 'unknown'
--     again (page is content-thin via Tavily/Playwright); Collins Lake
--     wasn't even in the candidate set (extract_cpad_unit_dog_policy.py
--     filters via beach_locations which doesn't include inland lakes).
--
-- Resolution: web-fetched both ground truths and curated cpad_unit_dogs_policy
-- rows directly:
--   * Collins Lake (/faq has 14 dog/pet/leash hits): "Dogs and other pets
--     are permitted but they must be kept on a leash and controlled at
--     all times. Dogs are NOT allowed on the sand beach. They are welcome
--     to swim along the shoreline in other areas." → mixed/leashed,
--     sand=forbidden, water/picnic_area/campground=on_leash, conf 0.95
--   * Greyhound Rock (SC County dog policy page): "Leashed dogs are
--     allowed in all county parks and beaches, except for Scott Creek
--     Beach" → yes/leashed, sand+water=on_leash, conf 0.95
--
-- DERIVATION REFINEMENT (caught by the Collins Lake landing):
--   _derive_has_off_leash was inferring TRUE from claim->>'allowed' = 'mixed'.
--   But 'mixed' on the dogs_allowed enum has TWO meanings:
--     (a) BOTH leash modes apply (Coronado-style carve-out)
--     (b) Allowed in some areas, prohibited in others (Collins Lake-style
--         area-based restriction — leashed everywhere except the sand beach)
--   Conflating them yielded 373 (T,T) beaches dataset-wide; many were
--   actually category (b) and didn't have off-leash zones at all.
--
--   Refined: removed the `allowed='mixed' -> has_off_leash=true` branch.
--   has_off_leash now requires POSITIVE off-leash evidence (leash_required,
--   off_leash_exists, or areas_evidence). has_on_leash kept its 'allowed=mixed'
--   branch since 'mixed' implies at least leashed access exists.
--
--   Dataset-wide effect: 373 (T,T) -> 127 (T,T). 246 beaches correctly
--   moved to (T,F) — leashed-only. Carve-out beaches (Coronado, Fiesta,
--   Rosie's, OB Dog Beach, Hendry's, etc.) preserved at (T,T) because
--   their explicit off_leash signals flow through other derivation paths.

begin;

-- 1. Manual curator entries for the 2 stuck CPAD units.
insert into public.cpad_unit_dogs_policy
  (cpad_unit_id, unit_name, agency_name, url_used, url_kind,
   dogs_allowed, default_rule, leash_required,
   area_sand, area_water, area_picnic_area, area_campground,
   source_quote, extraction_model, extraction_confidence)
values
  (662, 'Collins Lake Recreation Area', 'Browns Valley Irrigation District',
   'https://www.collinslake.com/faq', 'park_url',
   'mixed', 'mixed', true,
   'forbidden', 'on_leash', 'on_leash', 'on_leash',
   'Dogs and other pets are permitted but they must be kept on a leash and controlled at all times. Dogs are NOT allowed on the sand beach. They are welcome to swim along the shoreline in other areas.',
   'manual_curator', 0.95),
  (25173, 'Greyhound Rock Fishing Access', 'California Department of Fish and Wildlife',
   'https://parks.santacruzcountyca.gov/Home/ExploreOurParksBeaches/DogsatCountyParks.aspx',
   'park_url', 'yes', 'yes', true,
   'on_leash', 'on_leash', null, null,
   'Leashed dogs are allowed in all county parks and beaches, except for Scott Creek Beach',
   'manual_curator', 0.95)
on conflict (cpad_unit_id) where cpad_unit_id is not null do update set
  dogs_allowed = excluded.dogs_allowed,
  default_rule = excluded.default_rule,
  leash_required = excluded.leash_required,
  area_sand = excluded.area_sand,
  area_water = excluded.area_water,
  area_picnic_area = excluded.area_picnic_area,
  area_campground = excluded.area_campground,
  source_quote = excluded.source_quote,
  url_used = excluded.url_used,
  extraction_model = excluded.extraction_model,
  extraction_confidence = excluded.extraction_confidence,
  scraped_at = now();

-- 2. Refine _derive_has_off_leash so 'allowed=mixed' alone doesn't trigger.
create or replace function public._derive_has_off_leash(claim jsonb)
returns text language sql immutable as $$
  select case
    when claim is null then null
    when claim->>'areas_evidence' ~ '\moff_leash\M' then 'true'
    when (claim->>'leash_required') in ('off_leash', 'mixed', 'mixed_by_zone') then 'true'
    when (claim->>'off_leash_exists') = 'true' then 'true'
    when (claim->>'off_leash_exists') = 'false' then 'false'
    when (claim->>'allowed') = 'no' then 'false'
    else null
  end
$$;

commit;
