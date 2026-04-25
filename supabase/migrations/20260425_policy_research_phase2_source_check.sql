-- Phase 2 of POLICY_RESEARCH_MIGRATION (2026-04-25)
--
-- Add 'old_school_llm' to beach_enrichment_provenance.source CHECK constraint.
-- Update source_precedence to slot it just below fresh 'research' so that
-- when both exist for the same beach+field_group, fresh research wins.
--
-- Pure additive — existing rows unaffected, no data migration.

-- ── 1. Extend source CHECK ─────────────────────────────────────────────
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','plz','cpad','tiger_places','ccc','llm','web_scrape','research',
    'csp_parks','park_operators','nps_places','tribal_lands','military_bases',
    'pad_us','sma_code_mappings','jurisdictions','csp_places','name',
    'governing_body','park_url','park_url_buffer_attribution',
    'old_school_llm'   -- NEW: v2-* legacy LLM output (Phase 2 of POLICY_RESEARCH_MIGRATION)
  ]));

-- ── 2. Update source_precedence ────────────────────────────────────────
-- 'research' moves from default-99 to explicit 71.
-- 'old_school_llm' slots at 75 — beaten by fresh 'research', wins over 'llm'.
create or replace function public.source_precedence(p_source text)
returns int
language sql immutable
as $$
  select case p_source
    when 'manual'             then 0
    when 'cpad'               then 10
    when 'pad_us'             then 11
    when 'park_operators'     then 12
    when 'plz'                then 15
    when 'nps_places'         then 20
    when 'tribal_lands'       then 21
    when 'csp_parks'          then 22
    when 'sma_code_mappings'  then 23
    when 'ccc'                then 30
    when 'tiger_places'       then 40
    when 'military_bases'     then 50
    when 'state_config'       then 60
    when 'web_scrape'         then 70
    when 'research'           then 71  -- NEW explicit slot for fresh research
    when 'old_school_llm'     then 75  -- NEW: legacy LLM output, beaten by fresh research
    when 'llm'                then 80
    else                           99
  end;
$$;

comment on function public.source_precedence(text) is
  'Lower number = higher priority. Used as tiebreaker when multiple sources have equal confidence. Phase 1 ordering: cpad > pad_us > park_operators > plz > nps/tribal/csp/sma > ccc > tiger > military > state_config > web_scrape > research > old_school_llm > llm. Manual is special-cased in resolvers (always wins regardless of this ordering).';
