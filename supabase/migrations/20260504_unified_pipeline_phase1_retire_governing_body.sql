-- 20260504_unified_pipeline_phase1_retire_governing_body.sql
--
-- Retire the `governing_body` evidence source. Background:
--   - The populator `populate_dogs_from_governing_body(integer)` was dropped
--     2026-05-03 in harmony cleanup (`20260503_harmony_phase7_3c_drop_legacy_fid.sql`)
--     because it was wired to legacy locations_stage. No _gold replacement
--     was written.
--   - 611 rows in beach_enrichment_provenance with source='governing_body'
--     remain as zombie evidence — read by every consensus computation, but
--     never refreshed. Coverage is 83% of consumer beaches; the 17% gap is
--     post-harmony promotions.
--   - city_policy + county_policy + cpad now cover the agency-class prior
--     with better-calibrated, actually-scraped data. governing_body's
--     generic-default rule layer is redundant.
--
-- Decision (2026-05-04, Franz): let governing_body decay. This migration
-- removes the rows + calibration weights + lookup table + family taxonomy
-- entry, then triggers re-resolve + re-consensus + re-promote for the
-- affected beaches.
--
-- Impact preview (run before this migration):
--   - 35 beaches have governing_body as is_canonical=true on field_group=dogs
--   - 6 consensus winners are governing_body-only (will go null on consumer)
--   - 141 consensus winners have governing_body alongside others (re-rank,
--     winner usually unchanged because other sources outweigh)
--   - 1270 consensus rows have no governing_body voter (no impact)

begin;

-- 1. Snapshot the affected fids so we can re-resolve + re-promote them.
create temp table _gov_affected on commit drop as
  select distinct gold_fid
    from public.beach_enrichment_provenance
   where source = 'governing_body';

-- 2. Delete the zombie evidence rows.
delete from public.beach_enrichment_provenance
 where source = 'governing_body';

-- 3. Delete the calibration rows (no source rows means no votes; rows would
--    just be orphaned weight references otherwise).
delete from public.field_source_calibration
 where source = 'governing_body';

-- 4. Drop the lookup table. agency_aliases stays — still used by
--    canonical_agency_name() in cpad/park_operators populators.
drop table if exists public.governing_body_dog_policies;

-- 5. Strip the 'governing_body' case from _source_family. Falls through to
--    `else p_source` so it would still resolve to 'governing_body' as its
--    own family — but with zero rows, the family contributes zero votes.
--    Remove the explicit case for cleanliness.
create or replace function public._source_family(p_source text)
returns text language sql immutable as $$
  select case p_source
    when 'manual'                      then 'manual'
    when 'manual_curator'              then 'manual'
    when 'park_url'                    then 'park_url'
    when 'park_url_governance'         then 'park_url'
    when 'park_url_buffer_attribution' then 'park_url'
    when 'park_operators'              then 'park_operators'
    when 'research'                    then 'research'
    when 'old_school_llm'              then 'llm_extraction'
    when 'unified_v1'                  then 'llm_extraction'
    when 'json_explode'                then 'llm_extraction'
    when 'llm'                         then 'llm_extraction'
    when 'cpad'                        then 'spatial'
    when 'jurisdictions'               then 'spatial'
    when 'counties'                    then 'spatial'
    when 'military_bases'              then 'spatial'
    when 'tribal_lands'                then 'spatial'
    when 'csp_parks'                   then 'spatial'
    when 'csp_places'                  then 'spatial'
    when 'nps_places'                  then 'spatial'
    when 'tiger_places'                then 'spatial'
    when 'pad_us'                      then 'spatial'
    when 'sma_code_mappings'           then 'spatial'
    when 'name'                        then 'name_heuristic'
    when 'plz'                         then 'plz'
    when 'ccc'                         then 'ccc'
    when 'web_scrape'                  then 'park_url'
    -- governing_body retired 2026-05-04. No active source emits it.
    else p_source
  end;
$$;

-- 6. Re-resolve canonical, re-compute consensus, re-promote to consumer
--    surface for the affected beaches. Without this step, the consumer
--    surface keeps stale beach_dog_policy values that came from the
--    governing_body voter that is now gone.
do $$
declare fid_iter bigint;
begin
  perform public._resolve_dogs_gold(null);  -- batch-mode resolver re-pick
  for fid_iter in select gold_fid from _gov_affected loop
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_to_consumer_tables(fid_iter);
  end loop;
end $$;

commit;
