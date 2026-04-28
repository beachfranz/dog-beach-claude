-- Track where each osm_features.name came from. OSM rows get 'osm';
-- borrowed names from spatial joins get the source table's slug. Lets
-- us reason about confidence ('osm' = the contributor named it; 'ccc'
-- = nearest within 200m of a curated CCC point) and re-do the borrow
-- if we find better source data later.

alter table public.osm_features
  add column if not exists name_source text;

-- Anything that already has a name came from OSM tagging — that's how
-- the loader populated this column.
update public.osm_features
   set name_source = 'osm'
 where name is not null and trim(name) <> '' and name_source is null;

-- ── Borrow #1: nearest CCC point within 200m ──────────────────────
-- CCC takes priority over us_beach_points: more curated, CA-focused,
-- and likely the same access point we already track elsewhere.
with sub as (
  select t.osm_type, t.osm_id, c.name as borrowed
  from public.osm_features t
  cross join lateral (
    select c.name, c.geom
    from public.ccc_access_points c
    where c.name is not null and trim(c.name) <> ''
      and st_dwithin(t.geom::geography, c.geom::geography, 200)
    order by t.geom <-> c.geom
    limit 1
  ) c
  where t.feature_type in ('beach','dog_friendly_beach')
    and (t.name is null or trim(t.name) = '')
)
update public.osm_features t
   set name = sub.borrowed,
       name_source = 'ccc'
  from sub
 where t.osm_type = sub.osm_type and t.osm_id = sub.osm_id;

-- ── Borrow #2: nearest us_beach_points within 200m ────────────────
-- Fills any beaches CCC didn't reach (inland lakes, river mouths).
with sub as (
  select t.osm_type, t.osm_id, c.name as borrowed
  from public.osm_features t
  cross join lateral (
    select c.name, c.geom
    from public.us_beach_points c
    where c.name is not null and trim(c.name) <> ''
      and st_dwithin(t.geom::geography, c.geom::geography, 200)
    order by t.geom <-> c.geom
    limit 1
  ) c
  where t.feature_type in ('beach','dog_friendly_beach')
    and (t.name is null or trim(t.name) = '')
)
update public.osm_features t
   set name = sub.borrowed,
       name_source = 'us_beach_points'
  from sub
 where t.osm_type = sub.osm_type and t.osm_id = sub.osm_id;
