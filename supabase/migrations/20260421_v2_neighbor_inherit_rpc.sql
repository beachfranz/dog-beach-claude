-- v2_find_neighbor_inheritance: for each unlocked record, find the nearest
-- locked neighbor within max_distance_m that has a ground-truth source. Used
-- by v2-default-county to propagate jurisdictions to co-located unlocked
-- records before defaulting to county.

create or replace function public.v2_find_neighbor_inheritance(
  max_distance_m  float default 200,
  trusted_sources text[] default array[
    'federal_polygon', 'state_polygon', 'city_polygon', 'city_polygon_buffer'
  ]
)
returns table (
  u_id      integer,
  u_name    text,
  l_id      integer,
  l_name    text,
  l_juris   text,
  l_body    text,
  l_source  text,
  dist_m    float
)
language sql stable as $$
  with unlocked as (
    select id, display_name, latitude, longitude
    from beaches_staging_new
    where review_status is null
      and latitude is not null and longitude is not null
  ),
  locked as (
    select id, display_name, latitude, longitude,
           governing_jurisdiction, governing_body, governing_body_source
    from beaches_staging_new
    where review_status = 'ready'
      and governing_body_source = any (trusted_sources)
      and latitude is not null and longitude is not null
  ),
  pairs as (
    select
      u.id as u_id, u.display_name as u_name,
      l.id as l_id, l.display_name as l_name,
      l.governing_jurisdiction as l_juris,
      l.governing_body as l_body,
      l.governing_body_source as l_source,
      (2 * 6371000 * asin(sqrt(
        power(sin(radians(l.latitude - u.latitude) / 2), 2) +
        cos(radians(u.latitude)) * cos(radians(l.latitude)) *
        power(sin(radians(l.longitude - u.longitude) / 2), 2)
      ))) as dist_m
    from unlocked u
    join locked l
      on abs(u.latitude  - l.latitude)  < 0.003
     and abs(u.longitude - l.longitude) < 0.003
  )
  select distinct on (u_id)
    u_id, u_name, l_id, l_name, l_juris, l_body, l_source, dist_m
  from pairs
  where dist_m <= max_distance_m
  order by u_id, dist_m asc;
$$;
