-- 20260508_seed_plover_closures.sql
--
-- Interim closure seeding for OR + WA Snowy Plover protection beaches.
-- Authoritative polygons need manual shapefile download (see
-- project_plover_polygon_load.md memo). Tonight: set the canonical
-- Mar 15 – Sep 15 season directly on beach_dog_policy.dogs_prohibited_*
-- for any beach whose name matches the canonical USFWS recovery list.
-- That column is what daily-beach-refresh + find feed already consume,
-- so the season surfaces immediately.
--
-- public.beach_dog_policy_zones is a VIEW derived from dog_policy_zones
-- polygon overlap; we cannot insert there. The polygon load is pinned
-- for follow-up (see project memo).

begin;

create temp table _plover_seed (
  state_code text,
  county text,
  name text,
  alt_name text,
  approx_lat double precision,
  approx_lon double precision
);

insert into _plover_seed values
  ('OR', 'Lane',         'Sutton Beach',                  'Sutton Recreation Area',     44.0286, -124.1336),
  ('OR', 'Lane',         'Siltcoos River Mouth',          'Siltcoos Estuary',           43.8728, -124.1656),
  ('OR', 'Douglas',      'Tahkenitch Creek',              'Tahkenitch Beach',           43.8011, -124.1597),
  ('OR', 'Coos',         'Tenmile Creek Beach',           'Tenmile Creek',              43.5575, -124.2106),
  ('OR', 'Coos',         'New River',                     'New River Beach',            43.0411, -124.4189),
  ('OR', 'Curry',        'Floras Lake',                   'Floras Lake Beach',          42.8933, -124.5031),
  ('WA', 'Grays Harbor', 'Damon Point',                   'Damon Point State Park',     46.9447, -124.1056),
  ('WA', 'Grays Harbor', 'Copalis Spit',                  'Copalis Spit',               47.1281, -124.1872),
  ('WA', 'Pacific',      'Midway Beach',                  'Midway Beach Conservation',  46.6306, -124.0656),
  ('WA', 'Pacific',      'Leadbetter Point',              'Leadbetter Point State Park',46.6422, -124.0608),
  ('WA', 'Pacific',      'Graveyard Spit',                'Graveyard Spit',             46.6611, -124.0533),
  ('WA', 'Pacific',      'Long Beach Peninsula',          'Long Beach',                 46.6464, -124.0631);

create temp table _plover_matched as
select
  s.*,
  g.fid as matched_fid,
  g.name as matched_name
from _plover_seed s
left join lateral (
  select g.fid, g.name
    from public.beaches_gold g
   where g.state = s.state_code
     and g.is_active
     and (
       g.name ilike s.name || '%'
       or g.name ilike '%' || s.name || '%'
       or g.name ilike s.alt_name || '%'
       or g.name ilike '%' || s.alt_name || '%'
     )
   order by similarity(g.name, s.name) desc nulls last
   limit 1
) g on true;

-- Apply the canonical Mar 15 – Sep 15 season to matched beaches.
update public.beach_dog_policy bdp
   set dogs_prohibited_start = '03-15',
       dogs_prohibited_end   = '09-15',
       notes = coalesce(bdp.notes || E'\n', '') ||
               'Snowy Plover protection (Mar 15 – Sep 15). Source: USFWS Pacific Coast recovery plan; OPRD/WDFW management areas. Authoritative polygon pinned for shapefile load.'
  from _plover_matched m
 where bdp.arena_group_id = m.matched_fid
   and m.matched_fid is not null;

-- Persistent log of unmatched beaches (those not yet in beaches_gold).
-- Future: seed via scripts/pipeline/seed_arena_beach.py at the recorded
-- lat/lon; this table makes the to-do explicit.
create table if not exists public.plover_seed_pending (
  state_code text,
  county text,
  name text,
  alt_name text,
  approx_lat double precision,
  approx_lon double precision,
  reason text,
  recorded_at timestamptz default now(),
  primary key (state_code, name)
);

insert into public.plover_seed_pending
  (state_code, county, name, alt_name, approx_lat, approx_lon, reason)
select state_code, county, name, alt_name, approx_lat, approx_lon,
       'no match in beaches_gold by name; needs seed_arena_beach.py'
  from _plover_matched
 where matched_fid is null
on conflict (state_code, name) do update set
  county     = excluded.county,
  alt_name   = excluded.alt_name,
  approx_lat = excluded.approx_lat,
  approx_lon = excluded.approx_lon,
  reason     = excluded.reason,
  recorded_at = now();

commit;
