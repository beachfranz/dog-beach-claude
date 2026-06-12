-- 20260612e_arena_clustering_tighten.sql
--
-- Tighten Steps 3 + 4 of populate_arena_group_id. Forensic of cluster
-- 13075 (Brewster MA: 9 distinct named POI beaches glued into one
-- group) showed:
--
--   Step 3 (OSM name + county + 5km proximity):
--     Two failures
--      (a) 5km is far too generous — generic-name OSM rows 3-4km
--          apart in Cape Cod regularly share lname e.g. "Town Beach"
--          but are physically distinct beaches.
--      (b) No name-quality floor — "Town Beach" / "Public Beach" /
--          "Main Beach" matches anywhere in a county get clustered
--          even at the edge of the radius.
--     Fix: tighten radius 5000 → 750m; blocklist generic-only names.
--
--   Step 4 (POI-into-polygon, 2km radius, pick best similarity):
--     Two failures
--      (a) 2km radius lets POIs sit far outside a beach polygon and
--          still attach to its cluster.
--      (b) No minimum-similarity floor — a POI with sim=0.1 against
--          the cluster head still wins if no other candidate exists.
--          Result: distinct-named POIs (Crowes Reservation, Brewster
--          Park Sunhouse, Wing Island, etc.) all swept into the
--          "Robbins Hill Beach" cluster.
--     Fix: tighten radius 2000 → 500m; require sim >= 0.6 (POI
--          name vs cluster head name).
--
-- Step 2 (OSM relation membership) is left alone — when it goes
-- wrong, the OSM data itself is wrong.
--
-- This migration ONLY changes the function. It does NOT re-run
-- populate_arena_group_id. The next per-state launch / orchestrated
-- run will exercise the tighter logic; existing miswired groups
-- stay miswired until a controlled re-run + curator audit.
--
-- Self-patch via pg_get_functiondef + replace + EXECUTE, same
-- pattern as 20260612a (advisory off-by-one) and 20260612c
-- (dedup spatial gate).

BEGIN;

DO $patch$
DECLARE
  v_src text;
  v_step3_old_where text := 'a.source_code = ''osm'' and a.name is not null and trim(a.name) <> ''''';
  v_step3_new_where text := 'a.source_code = ''osm'' and a.name is not null and trim(a.name) <> '''''
       || E'\n       and lower(trim(a.name)) not in ('
       || '''town beach'',''public beach'',''main beach'','
       || '''south beach'',''north beach'',''east beach'',''west beach'','
       || '''city beach'',''private beach'',''ocean beach'','
       || '''old beach'',''new beach'',''beach'',''the beach'')';
  v_step3_old_radius text := 'st_dwithin(a.geom::geography, b.geom::geography, 5000)';
  v_step3_new_radius text := 'st_dwithin(a.geom::geography, b.geom::geography, 750)';

  v_step4_old_radius text := 'ST_DWithin(g.poly::geography, poi.geom::geography, 2000)';
  v_step4_new_radius text := 'ST_DWithin(g.poly::geography, poi.geom::geography, 500)';

  v_step4_old_pick text := 'select distinct on (poi_fid) poi_fid, group_id from cands';
  v_step4_new_pick text := 'select distinct on (poi_fid) poi_fid, group_id from cands'
       || E'\n     where sim >= 0.6';
BEGIN
  v_src := pg_get_functiondef('public.populate_arena_group_id'::regproc);

  -- Defensive: error early if any anchor missing (body drifted).
  IF position(v_step3_old_where IN v_src)  = 0 THEN RAISE EXCEPTION 'step3 eligible WHERE anchor missing'; END IF;
  IF position(v_step3_old_radius IN v_src) = 0 THEN RAISE EXCEPTION 'step3 radius anchor missing';        END IF;
  IF position(v_step4_old_radius IN v_src) = 0 THEN RAISE EXCEPTION 'step4 radius anchor missing';        END IF;
  IF position(v_step4_old_pick IN v_src)   = 0 THEN RAISE EXCEPTION 'step4 pick anchor missing';          END IF;

  v_src := replace(v_src, v_step3_old_where,  v_step3_new_where);
  v_src := replace(v_src, v_step3_old_radius, v_step3_new_radius);
  v_src := replace(v_src, v_step4_old_radius, v_step4_new_radius);
  v_src := replace(v_src, v_step4_old_pick,   v_step4_new_pick);

  EXECUTE v_src;
  RAISE NOTICE 'populate_arena_group_id patched: Step 3 radius 5000→750, generic-name blocklist; Step 4 radius 2000→500, sim floor 0.6';
END
$patch$;

COMMIT;

NOTIFY pgrst, 'reload schema';
