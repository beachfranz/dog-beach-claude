-- Pre-reverse-geocode prep:
--   1. Populate geom from raw_wkt for all rows (the disabled trigger
--      meant the load left geom null)
--   2. Add address_source column to distinguish CSV-parsed addresses
--      from reverse-geocoded ones

alter table public.poi_landing
  add column if not exists address_source text;

update public.poi_landing
   set address_source = 'csv'
 where address_source is null and address_validation is not null;

update public.poi_landing
   set geom = ST_SetSRID(ST_GeomFromText(raw_wkt), 4326)
 where geom is null and raw_wkt is not null;

comment on column public.poi_landing.address_source is
  'csv | reverse_geocode | manual — where the parsed address came from. csv = parsed from CSV ADDR1..5; reverse_geocode = derived from lat/lng via external API.';
