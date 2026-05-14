-- 20260513_build_beach_evidence_mode_flag.sql
--
-- Adds p_mode parameter to build_beach_evidence(p_fid, p_mode).
--
-- Modes:
--   'full' (default) — runs all 14 populators + 7 resolvers + consensus +
--     promote_canonical. Used by promote_to_gold (first-pass on a new
--     beach), geom-change trigger, manual rebuilds.
--
--   'operator_only' — skips the 9 upstream-data-only populators (their
--     output doesn't change between Pass 1 and Pass 2), runs only the
--     5 operator-dependent populators + downstream resolvers + consensus.
--     Used by Phase 28 (rebuild_beach_evidence) after operator_merge has
--     produced fresh operator_dogs_policy rows.
--
-- Rationale: promote_to_gold's first build pass produces all the polygon-
-- containment / cpad / pad_us / state_default / research / park_url /
-- park_url_governance / unified_v1 / osm_amenities rows. These don't change
-- between Pass 1 and Pass 2 — they read from upstream tables that didn't
-- mutate. Re-running them in Phase 28 is wasted compute.
--
-- Expected impact on OR-scale state launch (151 fids):
--   today's Phase 28 wall-clock:  ~45-60 min
--   after operator_only mode:     ~5-10 min   (cuts ~60-80% of the work)
--
-- ALSO INCLUDED: wires _emit_evidence_from_pad_us_dogs_policy into the 'full'
-- mode path. This emitter existed (with seeded agency-class defaults for NPS,
-- FWS, USFS, USACE, SPR, BLM, etc.) but was never called from build_beach_
-- evidence. Adding it gives every PAD-US-tagged beach a free, deterministic
-- dogs evidence row at confidence 0.55 (agency default) or 0.70 (per-unit
-- override) — no LLM cost. For OR this auto-fills ~50 federal+state-park
-- beaches with correct policy (NPS/FWS = no, USFS/USACE = mixed, SPR = yes).
--
-- See project_build_beach_evidence_perf.md.

begin;

-- Old signature still exists as a wrapper — call build_beach_evidence(fid)
-- still works, defaults to mode='full'. We CREATE OR REPLACE the existing
-- bigint-only function as the wrapper, and add the 2-arg variant.

create or replace function public.build_beach_evidence(
  p_fid  bigint,
  p_mode text
)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
begin
  if p_fid is null then return; end if;
  if p_mode is null then p_mode := 'full'; end if;

  -- Clear regenerable field groups. In operator_only mode, ONLY the field
  -- groups that the operator-dependent populators will rewrite are cleared.
  -- Polygon containment + non-operator-derived governance/dogs stay.
  if p_mode = 'full' then
    delete from public.beach_enrichment_provenance
     where gold_fid = p_fid
       and field_group in ('dogs','practical','governance','access','polygon_containment');
  else  -- 'operator_only'
    -- Only clear rows from sources that the operator-dependent populators
    -- write. Other BEP rows (cpad, pad_us, pad_us_dogs, state_default,
    -- research, etc.) survive untouched.
    delete from public.beach_enrichment_provenance
     where gold_fid = p_fid
       and source in (
         'park_operators',      -- from populate_from_park_operators_gold
         'operator',            -- from populate_from_operators_gold
         'city_policy',         -- from populate_from_city_dog_policy_gold
         'county_policy',       -- from populate_from_county_dog_policy_gold
         'operator_city',       -- ditto
         'operator_county'      -- ditto
       );
  end if;

  -- ─── Upstream-data-only populators (skipped in operator_only mode) ──
  if p_mode = 'full' then
    perform public.populate_polygon_containment_gold(p_fid);
    perform public.populate_from_cpad_gold(p_fid);
    perform public.populate_from_pad_us_gold(p_fid);                 -- access only
    perform public._emit_evidence_from_pad_us_dogs_policy(p_fid);    -- 2026-05-13: dogs, agency-default + per-unit
    perform public.populate_from_state_default_gold(p_fid);
    perform public.populate_from_research_gold(p_fid);
    perform public.populate_from_park_url_gold(p_fid);
    perform public.populate_from_park_url_governance_gold(p_fid);
    perform public.populate_from_unified_v1_gold(p_fid);
    perform public._emit_evidence_from_osm_amenities(p_fid);
    perform public.populate_accessibility_from_osm_amenities(p_fid);
  end if;

  -- ─── Operator-dependent populators (always run) ─────────────────────
  perform public.populate_from_park_operators_gold(p_fid);
  perform public.populate_from_operators_gold(p_fid);
  perform public.populate_from_city_dog_policy_gold(p_fid);
  perform public.populate_from_county_dog_policy_gold(p_fid);

  -- ─── Resolvers (always run) ─────────────────────────────────────────
  -- In operator_only mode, polygon_containment didn't change so
  -- _resolve_polygon_containment is a no-op (idempotent — re-sets the same
  -- is_canonical flags). Skip it to save time.
  if p_mode = 'full' then
    perform public._resolve_polygon_containment(p_fid);
  end if;
  perform public.populate_governance_gold(p_fid);
  perform public._resolve_governance_gold(p_fid);
  perform public._resolve_dogs_gold(p_fid);
  perform public._resolve_practical_gold(p_fid);
  perform public._resolve_field_group_gold('access', p_fid);
  perform public._resolve_field_group_gold('accessibility', p_fid);

  -- ─── Consensus + consumer-table promotion (always run) ──────────────
  perform public.compute_beach_field_consensus(p_fid);
  perform public.promote_canonical_to_consumer_tables(p_fid);
  perform public.promote_canonical_accessibility_to_beach_amenities(p_fid);
end $function$;

grant execute on function public.build_beach_evidence(bigint, text)
  to anon, authenticated, service_role;

-- ── Single-arg variant becomes a thin wrapper → full mode ──────────────

create or replace function public.build_beach_evidence(p_fid bigint)
returns void
language sql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
  select public.build_beach_evidence(p_fid, 'full');
$function$;

grant execute on function public.build_beach_evidence(bigint)
  to anon, authenticated, service_role;

commit;
