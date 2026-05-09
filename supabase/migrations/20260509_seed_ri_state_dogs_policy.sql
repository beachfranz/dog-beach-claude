-- 20260509_seed_ri_state_dogs_policy.sql
--
-- Seed RI's row in state_dogs_policy and apply the RI DEM seasonal
-- closure (Apr 1 – Sep 30) to RI state-managed beaches.
--
-- "FOR NOW" — interim seed before deeper curation. Captures both:
--   1. Statewide default: dogs allowed, leashed, year-round at non-state
--      public/town beaches (RI Public Trust Doctrine + town ordinances).
--   2. RI DEM seasonal closure: dogs prohibited Apr 1 – Sep 30 at the
--      state-managed swim beaches (Misquamicut, Scarborough, East
--      Matunuck, Roger Wheeler, Charlestown Breachway, Goddard Mem.,
--      Burlingame). Applies to beaches contained in any PAD-US polygon
--      whose Loc_Mang/Loc_Own resolves to "RHODE ISLAND DEPARTMENT OF
--      ENVIRONMENTAL MANAGEMENT" or "Rhode Island State Parks".
--
-- After this migration, refire_bep_cascade(RI fids) will pick up the
-- new state row in the populator chain and write 46 beach_dog_policy
-- rows. The seasonal_closure UPDATE then sets the per-beach prohibited
-- window directly on beach_dog_policy (mirrors the OR/WA plover seed
-- pattern from 20260508_seed_plover_closures.sql).

begin;

-- ── 1. Statewide RI default ─────────────────────────────────────────
insert into public.state_dogs_policy
  (state_code, state_name, county_fips_filter,
   dogs_allowed, default_rule, has_on_leash, has_off_leash,
   source_quote, source_url, ordinance_ref, scope_notes, confidence, notes)
values
  ('RI', 'Rhode Island', null,
   'mixed', 'mixed', true, false,
   'Rhode Island Public Trust Doctrine — tidelands and the foreshore below mean high water are public. RI General Laws §4-13-15 requires dogs under owner control; most municipal beaches and conservation areas permit leashed dogs year-round. RI DEM state swim beaches prohibit dogs Apr 1 – Sep 30 (10 a.m.–6 p.m.); allowed off-season.',
   'https://www.riparks.ri.gov/Programs/RulesRegulations.php',
   'RI Gen. Laws §4-13-15; RI DEM Rule 250-RICR-100-00-1',
   'Statewide fallback. State-managed swim beaches have a seasonal closure (Apr 1 – Sep 30) applied separately via beach_dog_policy.dogs_prohibited_start/end in this migration.',
   0.40,
   'Interim seed pending deeper curation of per-municipal ordinances. Newport, Westerly, Narragansett, South Kingstown all have town-beach summer dog bans that should override.')
on conflict do nothing;

-- ── 2. Apply RI DEM seasonal closure to state-park-managed beaches ──
-- Applies to any RI active beach contained in a PAD-US polygon owned
-- or managed by RI DEM / RI State Parks. Sets dogs_prohibited_start/end
-- on beach_dog_policy.
--
-- NOTE: This UPDATE depends on beach_dog_policy rows existing for RI
-- beaches. They don't yet (RI run_id=4 produced 0 rows). The
-- migration's UPDATE is a no-op today — it will become effective once
-- refire_bep_cascade(RI fids) runs after the state_dogs_policy seed
-- above lands. The orchestrator's bep_refire phase will trigger that;
-- this UPDATE is idempotent and will apply on a subsequent re-run.

with ri_state_park_fids as (
  select distinct g.fid
    from public.beaches_gold g
    join public.pad_us_units pu
      on st_intersects(g.geom, pu.geom)
   where g.state = 'RI' and g.is_active
     and (
          pu.raw_attrs->>'Loc_Mang' ilike '%RHODE ISLAND DEPARTMENT OF ENVIRONMENTAL%'
       or pu.raw_attrs->>'Loc_Mang' ilike '%RI%STATE%PARK%'
       or pu.raw_attrs->>'Loc_Own'  ilike '%RHODE ISLAND DEPARTMENT OF ENVIRONMENTAL%'
       or pu.mng_name ilike '%RIDEM%'
       or pu.mng_name ilike '%STATE%'
     )
)
update public.beach_dog_policy bdp
   set dogs_prohibited_start = '04-01',
       dogs_prohibited_end   = '09-30',
       notes = coalesce(bdp.notes || E'\n', '') ||
               'RI DEM seasonal closure: dogs prohibited Apr 1 – Sep 30 (10 a.m.–6 p.m.). Source: RI DEM Rule 250-RICR-100-00-1; RI State Parks Rules. Applies at state-managed swim beaches only.'
  from ri_state_park_fids r
 where bdp.arena_group_id = r.fid;

commit;
