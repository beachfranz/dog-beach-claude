-- 20260503_harmony_phase8_7_evidence_audit_view.sql
--
-- Phase 8 slice 7: gold-side audit-trail surface.
--
-- Single view that surfaces all evidence for a beach across all sources
-- and field groups, with the canonical pick highlighted and rows ordered
-- by source priority. Intended for:
--   * Curator debugging — "why does dogs_allowed show as yes for this beach?"
--   * Calibration loops — compare canonical pick to manual_curator truth
--   * Cascade refactor work — read evidence-based answers instead of
--     re-deriving consensus per call
--
-- Anon RLS allows SELECT (no PII; same posture as beaches_gold). Writes
-- are blocked.

begin;

create or replace view public.gold_evidence_audit as
select e.gold_fid,
       g.name as beach_name,
       g.county_name,
       g.is_scoreable,
       e.field_group,
       e.source,
       public._gold_source_priority(e.source) as source_priority,
       e.is_canonical,
       e.confidence,
       e.claimed_values,
       e.source_url,
       e.cpad_unit_name,
       e.extraction_type,
       e.updated_at
  from public.beach_enrichment_provenance e
  join public.beaches_gold g on g.fid = e.gold_fid
 where e.gold_fid is not null
 order by e.gold_fid,
          e.field_group,
          public._gold_source_priority(e.source) asc,
          e.confidence desc nulls last;

comment on view public.gold_evidence_audit is
  'Phase 8 slice 7 of harmony migration. Per-beach per-field-group evidence audit trail. Ordered by source priority (canonical first) so consumers can see "what does each source say + which wins." Drives curator debugging, calibration loops, future cascade refactor.';

commit;
