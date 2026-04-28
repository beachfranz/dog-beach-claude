-- Re-do the name borrow with corrected priority. The original pass put
-- CCC ahead of UBP, but CCC catalogs access points (often named after
-- piers, parking lots, or parks) so its name is frequently NOT the
-- beach name. UBP is beach-focused by source intent.
--
-- New rules:
--   Pass 1: nearest UBP within 200m → use that name (UBP source).
--   Pass 2: nearest CCC within 200m, ONLY if CCC's name contains a
--           beach-y word (beach|cove|shore|sand) on a word boundary.
--           Else, leave the OSM feature unnamed.
--
-- Rows where name_source='osm' (named by OSM contributors) are left
-- alone — we never overwrite an OSM-original name.

-- Reset prior borrows so we re-pick from scratch.
update public.osm_features
   set name = null, name_source = null
 where feature_type in ('beach','dog_friendly_beach')
   and name_source in ('ccc','us_beach_points');

-- Pass 1: UBP first.
with sub as (
  select t.osm_type, t.osm_id, u.name as borrowed
  from public.osm_features t
  cross join lateral (
    select u.name, u.geom
    from public.us_beach_points u
    where u.state = 'CA'
      and u.name is not null and trim(u.name) <> ''
      and st_dwithin(t.geom::geography, u.geom::geography, 200)
    order by t.geom <-> u.geom
    limit 1
  ) u
  where t.feature_type in ('beach','dog_friendly_beach')
    and (t.name is null or trim(t.name) = '')
)
update public.osm_features t
   set name = sub.borrowed,
       name_source = 'us_beach_points'
  from sub
 where t.osm_type = sub.osm_type and t.osm_id = sub.osm_id;

-- Pass 2: CCC fallback, but only when its name carries a beach-y word.
-- \m is the PostgreSQL word-boundary anchor (start-of-word) — keeps us
-- from false-positive matches inside random tokens, while still hitting
-- compounds like "Sandbar" or "Beachfront".
with sub as (
  select t.osm_type, t.osm_id, c.name as borrowed
  from public.osm_features t
  cross join lateral (
    select c.name, c.geom
    from public.ccc_access_points c
    where c.name is not null and trim(c.name) <> ''
      and c.name ~* '\m(beach|cove|shore|sand)'
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
