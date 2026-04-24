create table if not exists jurisdictions (
  id            serial primary key,
  name          text not null,
  place_type    text not null,   -- 'incorporated' | 'cdp'
  county        text not null,
  state         text not null default 'California',
  fips_state    text not null,   -- '06' for California
  fips_place    text not null,   -- 5-digit Census place FIPS
  fips_county   text not null,   -- 3-digit Census county FIPS
  created_at    timestamptz not null default now()
);

create unique index if not exists jurisdictions_fips_place_idx
  on jurisdictions (fips_state, fips_place);

create index if not exists jurisdictions_name_idx
  on jurisdictions (lower(name));

create index if not exists jurisdictions_county_idx
  on jurisdictions (fips_state, fips_county);

comment on table jurisdictions is
  'Canonical governing jurisdictions seeded from US Census TIGER/Line Gazetteer data.
   Covers incorporated cities and CDPs. place_type=incorporated means active municipal
   government; place_type=cdp means Census Designated Place (unincorporated community
   governed by county).';
