-- Insert a manual locations_stage row for Huntington Beach Dog Beach
-- mirroring the canonical fields from public.beaches.huntington-dog-beach.
--
-- Why manual: the existing staging coverage is mis-located (no row at
-- the actual Goldenwest dog-beach coordinates). Spatial sources will
-- fight over what to assign here; marking source='manual' on the
-- evidence rows guarantees pick_canonical_evidence() short-circuits
-- to manual for governance/dogs/practical, immune to override.
--
-- Synthetic fid in the 999_000_000 range, well above any natural fid
-- (max observed: ~20M), so the source is obvious to anyone scanning.

begin;

insert into public.locations_stage (
  fid, display_name, latitude, longitude, geom,
  state_code, county_name, county_fips,
  place_name, place_fips, place_type,
  raw_address,
  governing_body_name, governing_body_type,
  access_status, website,
  dogs_allowed, dogs_leash_required, dogs_zone_description,
  open_time, close_time, hours_text,
  has_parking, parking_type, parking_notes,
  has_restrooms, has_showers,
  is_active
)
values (
  999000001,
  'Huntington Beach Dog Beach',
  33.667640, -118.018057,
  st_setsrid(st_makepoint(-118.018057, 33.667640), 4326),
  'CA', 'Orange', '06059',
  'Huntington Beach', '36000', 'C1',
  'Huntington Dog Beach, Pacific Coast Highway, Huntington Beach, CA',
  'Huntington Beach, City of', 'city',
  'public',
  'https://www.huntingtonbeachca.gov/residents/parks_facilities/dog_beach/',
  'yes', 'mixed',
  'Beach sand is leash-optional. Dogs must be leashed in the parking lot, on the upper bluff, and until reaching the sand. Off-leash from the sand to the ocean. Adjacent Huntington State Beach (south) prohibits dogs.',
  '05:00:00', '22:00:00', '5am to 10pm',
  true, 'lot', 'Day-use parking; paid',
  true, false,
  true
);

-- Evidence rows. source='manual' means pick_canonical_evidence() picks
-- these as canonical without applying the agreement-boost or precedence
-- ladder. Spatial populators may still write evidence at this fid on
-- the next populate_all run (cpad/tiger via spatial join), but those
-- rows stay non-canonical.

insert into public.beach_enrichment_provenance
  (fid, field_group, source, source_url, claimed_values, confidence, is_canonical, updated_at)
values
  (
    999000001, 'governance', 'manual', null,
    jsonb_build_object(
      'name', 'Huntington Beach, City of',
      'type', 'city'
    ),
    1.00, true, now()
  ),
  (
    999000001, 'dogs', 'manual', null,
    jsonb_build_object(
      'allowed',          'yes',
      'leash_required',   'mixed',
      'zone_description', 'Beach sand is leash-optional. Dogs must be leashed in the parking lot, on the upper bluff, and until reaching the sand. Off-leash from the sand to the ocean. Adjacent Huntington State Beach (south) prohibits dogs.'
    ),
    1.00, true, now()
  ),
  (
    999000001, 'practical', 'manual', null,
    jsonb_build_object(
      'has_parking',   true,
      'parking_type',  'lot',
      'parking_notes', 'Day-use parking; paid',
      'has_restrooms', true,
      'has_showers',   false,
      'open_time',     '05:00:00',
      'close_time',    '22:00:00',
      'hours_text',    '5am to 10pm'
    ),
    1.00, true, now()
  ),
  (
    999000001, 'access', 'manual', null,
    jsonb_build_object('status', 'public'),
    1.00, true, now()
  );

commit;

-- Verify
select fid, display_name, latitude, longitude, governing_body_name,
       governing_body_type, dogs_allowed, dogs_leash_required, is_active
from public.locations_stage
where fid = 999000001;
