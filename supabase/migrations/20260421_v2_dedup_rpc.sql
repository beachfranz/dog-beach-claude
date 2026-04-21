-- v2_find_dedup_pairs: spatial + name similarity pair finder for dedup stage.
-- Returns one row per pair. Winner chosen by:
--   (1) already locked/invalid over active null
--   (2) longer display_name
--   (3) lower id as final tiebreaker

create or replace function public.v2_find_dedup_pairs(
  max_distance_m  float default 50,
  min_similarity  float default 0.5
)
returns table (
  winner_id    integer,
  winner_name  text,
  loser_id     integer,
  loser_name   text,
  dist_m       float,
  name_sim     float
)
language sql stable as $$
  with points as (
    select id, display_name, latitude, longitude, review_status
    from beaches_staging_new
    where latitude is not null and longitude is not null
      and review_status is distinct from 'duplicate'
      and review_status is distinct from 'invalid'
  ),
  pairs as (
    select
      a.id as a_id, a.display_name as a_name, a.review_status as a_status,
      b.id as b_id, b.display_name as b_name, b.review_status as b_status,
      similarity(a.display_name, b.display_name) as name_sim,
      (2 * 6371000 * asin(sqrt(
        power(sin(radians(b.latitude - a.latitude) / 2), 2) +
        cos(radians(a.latitude)) * cos(radians(b.latitude)) *
        power(sin(radians(b.longitude - a.longitude) / 2), 2)
      ))) as dist_m
    from points a
    join points b
      on a.id < b.id
     and abs(a.latitude  - b.latitude)  < 0.001
     and abs(a.longitude - b.longitude) < 0.001
  ),
  scored as (
    select
      case
        when (a_status = 'ready') and (b_status is distinct from 'ready') then a_id
        when (b_status = 'ready') and (a_status is distinct from 'ready') then b_id
        when length(a_name) > length(b_name) then a_id
        when length(b_name) > length(a_name) then b_id
        when a_id < b_id then a_id
        else b_id
      end as winner_id,
      case
        when (a_status = 'ready') and (b_status is distinct from 'ready') then a_name
        when (b_status = 'ready') and (a_status is distinct from 'ready') then b_name
        when length(a_name) > length(b_name) then a_name
        when length(b_name) > length(a_name) then b_name
        when a_id < b_id then a_name
        else b_name
      end as winner_name,
      case
        when (a_status = 'ready') and (b_status is distinct from 'ready') then b_id
        when (b_status = 'ready') and (a_status is distinct from 'ready') then a_id
        when length(a_name) > length(b_name) then b_id
        when length(b_name) > length(a_name) then a_id
        when a_id < b_id then b_id
        else a_id
      end as loser_id,
      case
        when (a_status = 'ready') and (b_status is distinct from 'ready') then b_name
        when (b_status = 'ready') and (a_status is distinct from 'ready') then a_name
        when length(a_name) > length(b_name) then b_name
        when length(b_name) > length(a_name) then a_name
        when a_id < b_id then b_name
        else a_name
      end as loser_name,
      dist_m,
      name_sim
    from pairs
    where dist_m <= max_distance_m
      and name_sim >= min_similarity
  )
  -- Pick the best pair per loser_id to avoid double-marking if a record is a
  -- duplicate of multiple winners. Take the closest (smallest distance).
  select distinct on (loser_id)
    winner_id, winner_name, loser_id, loser_name, dist_m, name_sim
  from scored
  order by loser_id, dist_m asc;
$$;
