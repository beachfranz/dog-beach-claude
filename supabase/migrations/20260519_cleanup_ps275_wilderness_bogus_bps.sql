-- 20260519_cleanup_ps275_wilderness_bogus_bps.sql
--
-- Follow-up to 20260519_federal_codify_or_usfws_oregon_islands_nwr_wilderness.sql.
-- Per agent finding 2026-05-19: ps 275 (USFWS Oregon Islands NWR dog-walking
-- page, on_leash) is correctly attached to Coquille Point (fid 14154320),
-- but ALSO has 12 bogus bps rows attaching on_leash to Wilderness-Area
-- beaches that are actually closed to public access.
--
-- The new ps 298 (federal_regulation, 50 CFR §26.21, not_allowed) now
-- correctly covers those 12 Wilderness beaches. The 12 stale on_leash
-- rows for ps 275 contradict the not_allowed rule and create rank-by-
-- consensus work for no signal.
--
-- This DELETE removes the 12 stale bps rows where ps 275 references a
-- Wilderness-Area beach. Coquille Point stays. Counts row first for
-- the audit trail.

begin;

-- Explicit fid list — agent's PAD-US lookup identified these 12 as the
-- Wilderness Area beaches that should be `not_allowed` (now via ps 298),
-- not `on_leash`. OR PIP membership isn't populated yet so we can't
-- derive via beach_polygon_membership join; hardcoded list is safe +
-- auditable.
delete from public.beach_policy_source
 where policy_source_id = 275
   and beach_fid = ANY(ARRAY[
     9715::bigint, 9739, 9741, 9763, 9773, 9777, 9786, 9790,
     1439671, 1764302, 8314398, 17326257
   ]);

-- Verify ps 275 now has just Coquille Point
do $$
declare
  v_count int;
begin
  select count(*) into v_count
    from public.beach_policy_source
   where policy_source_id = 275;
  if v_count <> 1 then
    raise warning 'cleanup_ps275: expected 1 bps row remaining for ps 275, got %', v_count;
  end if;
end;
$$;

commit;
