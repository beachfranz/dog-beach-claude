-- 20260504_zone_rules_text_repass_source.sql
--
-- Add 'text_repass_v1' to the BEP source check constraint. This is the
-- source value the closed_zones_v3 text-repass populator emits — runs the
-- LLM over existing prose evidence to produce structured zone_rules jsonb.
--
-- See scripts/repass_zone_rules.py and project_zone_rules_design_locked.md.
-- Also wire the source into the family taxonomy (its own family — distinct
-- from llm_extraction because it operates on already-structured prose,
-- producing the structural layer rather than raw facts).

begin;

alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;

alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','manual_curator','plz','cpad','tiger_places','ccc','llm',
    'web_scrape','research','csp_parks','park_operators','nps_places',
    'tribal_lands','military_bases','pad_us','sma_code_mappings',
    'jurisdictions','csp_places','name','governing_body','park_url',
    'park_url_buffer_attribution','park_url_governance',
    'old_school_llm','counties','json_explode','unified_v1',
    'city_policy','county_policy','text_repass_v1'
  ]));

-- Map text_repass_v1 to its own family in _source_family. It re-extracts
-- structure from the same prose that other LLM sources already saw, so it's
-- correlated with llm_extraction. But the MAX-within-family aggregation in
-- consensus would mean a beach with old_school_llm + text_repass_v1 would
-- vote with only one of them, suppressing the structured signal. Solving
-- that properly requires per-claim-key family membership, not per-source.
-- For now: own family. Revisit when zone_rules enters consensus voting.
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
    when 'text_repass_v1'              then 'text_repass'
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
    else p_source
  end;
$$;

commit;
