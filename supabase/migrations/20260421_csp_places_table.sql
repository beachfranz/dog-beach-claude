create table if not exists public.csp_places (
  id              serial primary key,
  park_name       text not null,
  latitude        double precision,
  longitude       double precision,
  county          text,
  street_address  text,
  city            text,
  zip             text,
  unit_nbr        text,
  mgmt_status     text,
  loaded_at       timestamptz not null default now()
);

create index if not exists csp_places_latlon_idx
  on public.csp_places (latitude, longitude)
  where latitude is not null and longitude is not null;

create index if not exists csp_places_name_trgm_idx
  on public.csp_places using gin (park_name gin_trgm_ops);

alter table public.beaches_staging_new
  add column if not exists csp_match_score numeric,
  add column if not exists csp_match_name  text;
