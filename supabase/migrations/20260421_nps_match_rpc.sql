create extension if not exists pg_trgm;

create index if not exists nps_places_title_trgm_idx
  on public.nps_places using gin (title gin_trgm_ops);

create index if not exists nps_places_latlon2_idx
  on public.nps_places (latitude, longitude)
  where latitude is not null and longitude is not null;

drop function if exists public.match_beaches_to_nps(float, float);

-- Proximity-only match: bounding-box pre-filter then exact haversine.
-- Fast because it uses the lat/lon index and avoids a full cross join.
create or replace function public.match_beaches_nps_proximity(
  proximity_m float default 300
)
returns table (
  beach_id        int,
  display_name    text,
  nps_title       text,
  nps_park        text,
  distance_m      float,
  name_similarity float
)
language sql stable as $$
  select distinct on (b.id)
    b.id,
    b.display_name,
    n.title,
    n.park_full_name,
    (2 * 6371000 * asin(sqrt(
      power(sin(radians((n.latitude  - b.latitude)  / 2)), 2) +
      cos(radians(b.latitude)) * cos(radians(n.latitude)) *
      power(sin(radians((n.longitude - b.longitude) / 2)), 2)
    )))                             as distance_m,
    similarity(b.display_name, n.title) as name_similarity
  from beaches_staging_new b
  join nps_places n
    on  abs(n.latitude  - b.latitude)  < (proximity_m / 111000.0)
    and abs(n.longitude - b.longitude) < (proximity_m / 85000.0)
    and n.latitude  is not null
    and n.longitude is not null
    and b.latitude  is not null
    and b.longitude is not null
  where (2 * 6371000 * asin(sqrt(
          power(sin(radians((n.latitude  - b.latitude)  / 2)), 2) +
          cos(radians(b.latitude)) * cos(radians(n.latitude)) *
          power(sin(radians((n.longitude - b.longitude) / 2)), 2)
        ))) <= proximity_m
  order by b.id,
    (2 * 6371000 * asin(sqrt(
      power(sin(radians((n.latitude  - b.latitude)  / 2)), 2) +
      cos(radians(b.latitude)) * cos(radians(n.latitude)) *
      power(sin(radians((n.longitude - b.longitude) / 2)), 2)
    ))) asc;
$$;
