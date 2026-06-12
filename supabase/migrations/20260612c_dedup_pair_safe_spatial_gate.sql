-- 20260612c_dedup_pair_safe_spatial_gate.sql
--
-- Add a spatial floor to the dedup safety check + make the geom-swap
-- in run_late_stage_dedup deterministic. Carmel-by-the-Sea forensic
-- showed run_late_stage_dedup killed fid 6417 (Carmel River SB,
-- 36.537) as "dupe_of_8287" (downtown beach, 36.551) — 1.5km away —
-- because _dedup_pair_safe was pure name-trigram with no distance
-- check, and the geom-swap then inherited the wrong nav via LIMIT 1
-- over OSM rows in the cluster.
--
-- Two changes here, both in beach.html / pipeline only:
--
-- (1) New OVERLOADED _dedup_pair_safe(name_a, name_b, min_sim,
--     p_a_fid, p_b_fid, p_max_distance_m DEFAULT 500). STABLE (geom
--     can change). Delegates the name check to the existing IMMUTABLE
--     3-arg overload, then ANDs ST_DWithin between the two fids. If
--     either fid has no geom (NULL), the spatial check returns NULL —
--     `IS NOT FALSE` lets it pass-through (don't accidentally block
--     valid pairs that lack coords).
--
-- (2) Self-patch both overloads of run_late_stage_dedup
--     (run_late_stage_dedup() and run_late_stage_dedup(text)):
--       a. _dedup_pair_safe(r.name, w.winner_name, 0.7)
--          → _dedup_pair_safe(r.name, w.winner_name, 0.7, r.fid, w.winner_fid)
--       b. geom-swap subquery's bare `limit 1` becomes:
--          order by (fid = w.winner_fid) desc, fid asc
--          limit 1
--          (prefers winner's own arena nav; deterministic fallback to
--          lowest-fid OSM row if winner has none.)

BEGIN;

-- ─── 1. New spatial-aware overload ────────────────────────────────────
CREATE OR REPLACE FUNCTION public._dedup_pair_safe(
  name_a            text,
  name_b            text,
  min_sim           real,
  p_a_fid           bigint,
  p_b_fid           bigint,
  p_max_distance_m  integer DEFAULT 500
)
RETURNS boolean
LANGUAGE sql STABLE
AS $function$
  SELECT public._dedup_pair_safe(name_a, name_b, min_sim)
     AND (
       ST_DWithin(
         (SELECT geom::geography FROM public.beaches_gold WHERE fid = p_a_fid),
         (SELECT geom::geography FROM public.beaches_gold WHERE fid = p_b_fid),
         p_max_distance_m
       ) IS NOT FALSE   -- NULL geom (missing fid) → don't block
     );
$function$;

-- ─── 2. Self-patch run_late_stage_dedup overloads ─────────────────────
DO $patch$
DECLARE
  v_sig text;
  v_src text;
  v_old_call text := '_dedup_pair_safe(r.name, w.winner_name, 0.7)';
  v_new_call text := '_dedup_pair_safe(r.name, w.winner_name, 0.7, r.fid, w.winner_fid)';
  v_old_swap text := E'nav_lon is not null\n         limit 1';
  v_new_swap text := E'nav_lon is not null\n         order by (fid = w.winner_fid) desc, fid asc\n         limit 1';
BEGIN
  FOR v_sig IN
    SELECT p.oid::regprocedure::text
      FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'public' AND p.proname = 'run_late_stage_dedup'
  LOOP
    v_src := pg_get_functiondef(v_sig::regprocedure);

    -- Skip legacy overloads that don't match the post-2026-05-12
    -- per-pair-safety pattern. The no-arg overload still uses the old
    -- cluster-level bool_and check (different call shape) and isn't on
    -- the hot path anymore.
    IF position(v_old_call IN v_src) = 0 OR position(v_old_swap IN v_src) = 0 THEN
      RAISE NOTICE '% has no per-pair _dedup_pair_safe pattern — skipping (legacy overload)', v_sig;
      CONTINUE;
    END IF;

    v_src := replace(v_src, v_old_call, v_new_call);
    v_src := replace(v_src, v_old_swap, v_new_swap);
    EXECUTE v_src;
    RAISE NOTICE '% patched', v_sig;
  END LOOP;
END
$patch$;

COMMIT;

NOTIFY pgrst, 'reload schema';
