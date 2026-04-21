create table if not exists public.nps_places (
  id              text primary key,   -- NPS place UUID
  title           text not null,
  latitude        double precision,
  longitude       double precision,
  park_code       text,
  park_full_name  text,
  loaded_at       timestamptz not null default now()
);

create index if not exists nps_places_latlon_idx
  on public.nps_places (latitude, longitude);
