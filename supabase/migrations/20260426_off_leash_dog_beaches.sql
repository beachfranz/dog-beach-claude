-- Curated list of effective off-leash dog beaches in California, loaded
-- from a hand-vetted geojson. Distinct from locations_stage (which is
-- the auto-derived staging corpus) and ccc_access_points (which is the
-- raw CCC dataset) — this is the explicit "where can dogs actually be
-- off-leash" gold-standard reference.

create table if not exists public.off_leash_dog_beaches (
  id                  serial primary key,
  name                text not null,
  region              text,
  city                text,
  off_leash_legal     boolean,
  off_leash_de_facto  boolean,
  enforcement_risk    text,
  social_norm         text,
  confidence          text,
  latitude            double precision,
  longitude           double precision,
  geom                geometry(Point, 4326),
  created_at          timestamptz default now(),
  updated_at          timestamptz default now()
);

create index if not exists off_leash_dog_beaches_geom_idx
  on public.off_leash_dog_beaches using gist (geom);

-- Public reference data — RLS off, anon can read directly.
alter table public.off_leash_dog_beaches disable row level security;
