-- California Coastal Commission Public Access Points — localized from
-- the CCC ArcGIS FeatureServer so v2-ccc-crossref can join in-DB instead
-- of calling out per beach. ~1,631 points statewide.
--
-- Source: https://services9.arcgis.com/wwVnNW92ZHUIr0V0/arcgis/rest/
--           services/AccessPoints/FeatureServer/0
-- CRS: EPSG:4326 (native GeoJSON output), stored as-is per
--      project_crs_convention.md.
--
-- Not a jurisdiction classifier — CCC is a curated coastal-access
-- registry. Role: (a) validate records as real beaches, (b) seed
-- ccc_dog_friendly hint, (c) canonical name cross-ref. Complementary
-- to CPAD (which answers "who owns/manages") not a replacement.

create table if not exists public.ccc_access_points (
  objectid            int primary key,                 -- CCC OID
  name                text,
  location            text,                             -- street-level description from CCC
  description         text,
  district            text,                             -- CCC administrative district
  county              text,
  phone               text,

  latitude            double precision,
  longitude           double precision,
  geom                geometry(Point, 4326) not null,

  -- Dog policy
  dog_friendly        text,

  -- Access
  open_to_public      text,
  fee                 text,
  restrictions        text,                             -- may contain dog-relevant rules (RSTRCTNS)

  -- Amenities
  parking             text,
  restrooms           text,
  showers             text,
  drinking_water      text,
  food                text,
  picnic_area         text,
  fire_pits           text,
  lifeguard           text,
  disabled_access     text,
  campground          text,
  beach_wheelchair    text,
  beach_wheelchair_program text,

  -- Beach type flags
  sandy_beach         text,
  dunes               text,
  rocky_shore         text,
  upland_beach        text,
  bluff               text,
  bay_lagoon_lake     text,
  urban_waterfront    text,
  inland_area         text,
  wetland             text,
  stream_corridor     text,
  offshore_reef       text,

  -- Access type (how to get to the beach)
  stairs_to_beach     text,
  path_to_beach       text,
  boardwalk           text,
  blufftop_trails     text,
  blufftop_park       text,
  bike_path           text,
  equestrian_trail    text,
  cct_link            text,                             -- California Coastal Trail
  cct_designation     text,

  -- Activities
  swimming            text,
  diving              text,
  snorkeling          text,
  surfing             text,
  fishing             text,
  boating             text,
  kayaking            text,
  tidepool            text,
  wildlife_viewing    text,
  playground          text,
  sport_fields        text,
  volleyball          text,
  windsurf_kite       text,

  -- Features
  lighthouse          text,
  pier                text,
  historic_structure  text,
  shipwrecks          text,

  -- Media
  photo_1             text,
  photo_2             text,
  photo_3             text,
  photo_4             text,
  google_maps_location text,
  apple_maps_location  text,

  -- Source meta
  data_updated        text,
  archived            text,
  loaded_at           timestamptz not null default now()
);

create index if not exists ccc_geom_gix     on public.ccc_access_points using gist(geom);
create index if not exists ccc_county_idx   on public.ccc_access_points (county);
create index if not exists ccc_name_idx     on public.ccc_access_points (name);
create index if not exists ccc_dog_idx      on public.ccc_access_points (dog_friendly);

alter table public.ccc_access_points enable row level security;

-- Batch upsert RPC. Single-statement set-based insert via
-- jsonb_array_elements, same pattern as load_cpad_batch /
-- load_counties_batch. Geometry is ST_SetSRID of the GeoJSON Point
-- (no ST_MakeValid needed — points can't be invalid).
create or replace function public.load_ccc_batch(p_features jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      (f->'properties'->>'OBJECTID')::int as objectid,
      f->'properties'->>'Name'             as name,
      f->'properties'->>'Location'         as location,
      f->'properties'->>'Description'      as description,
      f->'properties'->>'DISTRICT'         as district,
      f->'properties'->>'COUNTY'           as county,
      f->'properties'->>'PHONE_NMBR'       as phone,
      (f->'properties'->>'LATITUDE')::double precision  as latitude,
      (f->'properties'->>'LONGITUDE')::double precision as longitude,
      ST_SetSRID(ST_GeomFromGeoJSON((f->'geometry')::text), 4326) as geom,
      f->'properties'->>'DOG_FRIEND'       as dog_friendly,
      f->'properties'->>'O_PUBLIC'         as open_to_public,
      f->'properties'->>'FEE'              as fee,
      f->'properties'->>'RSTRCTNS'         as restrictions,
      f->'properties'->>'PARKING'          as parking,
      f->'properties'->>'RESTROOMS'        as restrooms,
      f->'properties'->>'SHOWERS'          as showers,
      f->'properties'->>'DRINKWTR'         as drinking_water,
      f->'properties'->>'FOOD'             as food,
      f->'properties'->>'PCNC_AREA'        as picnic_area,
      f->'properties'->>'FIREPITS'         as fire_pits,
      f->'properties'->>'LIFEGUARD'        as lifeguard,
      f->'properties'->>'DSABLDACSS'       as disabled_access,
      f->'properties'->>'CAMPGROUND'       as campground,
      f->'properties'->>'Bch_whlchr'       as beach_wheelchair,
      f->'properties'->>'BeachWheelchairProgram' as beach_wheelchair_program,
      f->'properties'->>'SNDY_BEACH'       as sandy_beach,
      f->'properties'->>'DUNES'            as dunes,
      f->'properties'->>'RKY_SHORE'        as rocky_shore,
      f->'properties'->>'UPLAND_BCH'       as upland_beach,
      f->'properties'->>'BLUFF'            as bluff,
      f->'properties'->>'BAY_LGN_LK'       as bay_lagoon_lake,
      f->'properties'->>'URBN_WFRNT'       as urban_waterfront,
      f->'properties'->>'INLND_AREA'       as inland_area,
      f->'properties'->>'WETLAND'          as wetland,
      f->'properties'->>'STRM_CRDOR'       as stream_corridor,
      f->'properties'->>'OFFSHR_RFG'       as offshore_reef,
      f->'properties'->>'STRS_BEACH'       as stairs_to_beach,
      f->'properties'->>'PTH_BEACH'        as path_to_beach,
      f->'properties'->>'BOARDWLK'         as boardwalk,
      f->'properties'->>'BLFTP_TRLS'       as blufftop_trails,
      f->'properties'->>'BLFTP_PRK'        as blufftop_park,
      f->'properties'->>'BIKE_PATH'        as bike_path,
      f->'properties'->>'EQUEST_TRL'       as equestrian_trail,
      f->'properties'->>'CCT_LINK'         as cct_link,
      f->'properties'->>'CCTdesigna'       as cct_designation,
      f->'properties'->>'SWIMMING'         as swimming,
      f->'properties'->>'DIVING'           as diving,
      f->'properties'->>'SNORKLNG'         as snorkeling,
      f->'properties'->>'SURFING'          as surfing,
      f->'properties'->>'FISHING'          as fishing,
      f->'properties'->>'BOATING'          as boating,
      f->'properties'->>'KAYAKING'         as kayaking,
      f->'properties'->>'TIDEPOOL'         as tidepool,
      f->'properties'->>'WLDLFE_VWG'       as wildlife_viewing,
      f->'properties'->>'PLAYGROUND'       as playground,
      f->'properties'->>'SPORT_FLDS'       as sport_fields,
      f->'properties'->>'VOLLEYBALL'       as volleyball,
      f->'properties'->>'WNDSRF_KIT'       as windsurf_kite,
      f->'properties'->>'LIGHTHOUSE'       as lighthouse,
      f->'properties'->>'PIER'             as pier,
      f->'properties'->>'HSTRC_STR'        as historic_structure,
      f->'properties'->>'SHPWRECKS'        as shipwrecks,
      f->'properties'->>'Photo_1'          as photo_1,
      f->'properties'->>'Photo_2'          as photo_2,
      f->'properties'->>'Photo_3'          as photo_3,
      f->'properties'->>'Photo_4'          as photo_4,
      f->'properties'->>'GoogleMaps_Location' as google_maps_location,
      f->'properties'->>'AppleMaps_Location'  as apple_maps_location,
      f->'properties'->>'DataUpdate'       as data_updated,
      f->'properties'->>'Archived'         as archived
    from jsonb_array_elements(p_features) as f
    where (f->'properties'->>'OBJECTID') is not null
      and (f->'geometry') is not null
  ),
  upserted as (
    insert into public.ccc_access_points (
      objectid, name, location, description, district, county, phone,
      latitude, longitude, geom,
      dog_friendly, open_to_public, fee, restrictions,
      parking, restrooms, showers, drinking_water, food, picnic_area,
      fire_pits, lifeguard, disabled_access, campground,
      beach_wheelchair, beach_wheelchair_program,
      sandy_beach, dunes, rocky_shore, upland_beach, bluff,
      bay_lagoon_lake, urban_waterfront, inland_area, wetland,
      stream_corridor, offshore_reef,
      stairs_to_beach, path_to_beach, boardwalk,
      blufftop_trails, blufftop_park, bike_path, equestrian_trail,
      cct_link, cct_designation,
      swimming, diving, snorkeling, surfing, fishing, boating, kayaking,
      tidepool, wildlife_viewing, playground, sport_fields, volleyball, windsurf_kite,
      lighthouse, pier, historic_structure, shipwrecks,
      photo_1, photo_2, photo_3, photo_4,
      google_maps_location, apple_maps_location,
      data_updated, archived
    )
    select * from candidates
    on conflict (objectid) do update set
      name = excluded.name, location = excluded.location,
      description = excluded.description, district = excluded.district,
      county = excluded.county, phone = excluded.phone,
      latitude = excluded.latitude, longitude = excluded.longitude, geom = excluded.geom,
      dog_friendly = excluded.dog_friendly, open_to_public = excluded.open_to_public,
      fee = excluded.fee, restrictions = excluded.restrictions,
      parking = excluded.parking, restrooms = excluded.restrooms,
      showers = excluded.showers, drinking_water = excluded.drinking_water,
      food = excluded.food, picnic_area = excluded.picnic_area,
      fire_pits = excluded.fire_pits, lifeguard = excluded.lifeguard,
      disabled_access = excluded.disabled_access, campground = excluded.campground,
      beach_wheelchair = excluded.beach_wheelchair,
      beach_wheelchair_program = excluded.beach_wheelchair_program,
      sandy_beach = excluded.sandy_beach, dunes = excluded.dunes,
      rocky_shore = excluded.rocky_shore, upland_beach = excluded.upland_beach,
      bluff = excluded.bluff, bay_lagoon_lake = excluded.bay_lagoon_lake,
      urban_waterfront = excluded.urban_waterfront, inland_area = excluded.inland_area,
      wetland = excluded.wetland, stream_corridor = excluded.stream_corridor,
      offshore_reef = excluded.offshore_reef,
      stairs_to_beach = excluded.stairs_to_beach, path_to_beach = excluded.path_to_beach,
      boardwalk = excluded.boardwalk, blufftop_trails = excluded.blufftop_trails,
      blufftop_park = excluded.blufftop_park, bike_path = excluded.bike_path,
      equestrian_trail = excluded.equestrian_trail,
      cct_link = excluded.cct_link, cct_designation = excluded.cct_designation,
      swimming = excluded.swimming, diving = excluded.diving,
      snorkeling = excluded.snorkeling, surfing = excluded.surfing,
      fishing = excluded.fishing, boating = excluded.boating,
      kayaking = excluded.kayaking, tidepool = excluded.tidepool,
      wildlife_viewing = excluded.wildlife_viewing, playground = excluded.playground,
      sport_fields = excluded.sport_fields, volleyball = excluded.volleyball,
      windsurf_kite = excluded.windsurf_kite,
      lighthouse = excluded.lighthouse, pier = excluded.pier,
      historic_structure = excluded.historic_structure, shipwrecks = excluded.shipwrecks,
      photo_1 = excluded.photo_1, photo_2 = excluded.photo_2,
      photo_3 = excluded.photo_3, photo_4 = excluded.photo_4,
      google_maps_location = excluded.google_maps_location,
      apple_maps_location = excluded.apple_maps_location,
      data_updated = excluded.data_updated, archived = excluded.archived,
      loaded_at = now()
    returning 1
  )
  select jsonb_build_object(
    'total',    jsonb_array_length(p_features),
    'affected', (select count(*)::int from upserted),
    'skipped',  jsonb_array_length(p_features) - (select count(*)::int from candidates)
  );
$$;

revoke all on function public.load_ccc_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_ccc_batch(jsonb) to service_role;
