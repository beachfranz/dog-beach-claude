-- Path 3a step 1+2: hand-create arena rows + beaches_gold rows for the
-- 5 active OR beaches in public.beaches.
--
-- Per path-3 decision 1=b: OR-state coverage is unblocked by inserting
-- 5 manual seeds rather than waiting on the full PAD-US-based OR
-- pipeline. Each OR beach becomes its own arena head-of-group with
-- source_code='manual'. beaches_gold then mirrors the arena row with
-- state='OR'. public.beaches gets arena_group_id wired up.
--
-- After this migration: every active row in public.beaches has an
-- arena_group_id, which is the prerequisite for the rest of path 3.

begin;

-- 1. INSERT 5 arena rows (one per OR beach)
with seeds(location_id, display_name, lat, lon, county_name) as (values
  ('or-cannon-beach-ecola-creek',  'Cannon Beach Ecola Creek Beach Access',   45.90160, -123.95999, 'Clatsop'),
  ('or-cape-kiwanda-county-park',  'Cape Kiwanda County Park Beach Access',   45.21571, -123.97114, 'Tillamook'),
  ('or-fort-stevens-peter-iredale','Fort Stevens SP Peter Iredale Access',    46.17840, -123.97831, 'Clatsop'),
  ('or-south-jetty-siuslaw-1',     'South Jetty Siuslaw River Beach Access 1', 43.95798, -124.14142, 'Lane'),
  ('or-tolovana-beach-srs',        'Tolovana Beach SRS',                      45.87299, -123.96145, 'Clatsop')
),
inserted as (
  insert into public.arena
    (name, lat, lon, county_name, source_code, source_id,
     nav_lat, nav_lon, nav_source, name_source, is_active)
  select s.display_name, s.lat, s.lon, s.county_name, 'manual',
         'manual/' || s.location_id,
         s.lat, s.lon, 'public.beaches', 'public.beaches', true
    from seeds s
  returning fid, source_id
)
update public.arena a
   set group_id = a.fid
  from inserted i
 where a.fid = i.fid;

-- 2. INSERT matching beaches_gold rows
insert into public.beaches_gold
  (fid, name, lat, lon, county_name, source_code, source_id,
   group_id, nav_lat, nav_lon, nav_source, name_source, state,
   promoted_from, is_active)
select a.fid, a.name, a.lat, a.lon, a.county_name,
       a.source_code, a.source_id,
       a.group_id, a.nav_lat, a.nav_lon, a.nav_source, a.name_source,
       'OR', 'or_manual_seed_v1', true
  from public.arena a
 where a.source_code = 'manual'
   and a.source_id like 'manual/or-%';

-- 3. Wire arena_group_id back to public.beaches (so dual-key FK is happy)
update public.beaches b
   set arena_group_id = a.fid
  from public.arena a
 where a.source_code = 'manual'
   and a.source_id = 'manual/' || b.location_id;

commit;

-- Post-flight (informational; run separately after commit):
-- SELECT count(*) FILTER (WHERE arena_group_id IS NULL AND is_active) AS or_orphans
--   FROM public.beaches;
-- Expect 0.
