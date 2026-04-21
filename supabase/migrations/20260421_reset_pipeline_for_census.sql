-- Reset pipeline to re-run geocoding with Census incorporated-place logic.
-- Address fields (city, county, state, zip, street_number, route) are left
-- intact — they come from Google and haven't changed.
update public.beaches_staging_new set
  geocode_status                   = null,
  governing_city                   = null,
  governing_county                 = null,
  governing_state                  = null,
  governing_jurisdiction           = null,
  census_incorporated_place        = null,
  governing_body                   = null,
  governing_body_source            = null,
  governing_body_notes             = null,
  governing_jurisdiction_ai        = null,
  governing_jurisdiction_ai_confidence = null,
  governing_body_agreement         = null;
