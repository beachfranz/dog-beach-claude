-- Document the operator vs operators distinction at the schema layer so
-- anyone running `\d operator` / `\d operators` or browsing the schema
-- gets the disambiguating answer immediately.
--
-- This is the lightest-weight half of the "which table do I reach for?"
-- problem. The heavier half is scripts/common/operator.py which provides
-- typed accessors that hide the choice from Python callers. See also
-- the matching note in CLAUDE.md.
--
-- Both tables are LOAD-BEARING and serve DIFFERENT roles. Don't merge.
-- See the operate-two-table-finding HARD memory pin + the formal FK
-- added in 20260522_operators_canonical_bridge.sql.

COMMENT ON TABLE public.operator IS
  'CANONICAL CONSUMER-SURFACE REGISTRY. Hand-curated. ~159 rows. '
  'These are the operators that actually run beaches and show up on '
  'beach.html. Includes nonprofits, sub-agencies, and other entities '
  'not present in TIGER/CPAD/OSM bulk sources. '
  'Cascade-wired via entity_operator + beach_operator + '
  'policy_source.issuing_operator_id. '
  'When in doubt: this is the table you reach for from the consumer '
  'surface. The plural ''operators'' table is for extraction-side bulk.';

COMMENT ON TABLE public.operators IS
  'JURISDICTION/TIGER EXTRACTION POOL. Bulk-loaded from TIGER cities, '
  'CPAD units, OSM operator strings. ~9,275 rows. '
  'Most rows are level=city from TIGER. Has slug, level/subtype, '
  'state_code, cpad_unit_count, ccc_point_count, etc. '
  'Used by the operator_llm_extract pipeline (operator_dogs_policy + '
  'operator_policy_extractions FK here). '
  'Bridges to the singular ''operator'' table via canonical_operator_id '
  '(formal FK; see 20260522_operators_canonical_bridge.sql). '
  'When in doubt: this is the table for bulk geographic / extraction '
  'work. The singular ''operator'' table is the curated consumer surface.';

-- Also comment the bridge FK column itself
COMMENT ON COLUMN public.operators.canonical_operator_id IS
  'FK to public.operator(id) — the canonical consumer-surface operator '
  'row. NULL when this plural row has no curated singular counterpart '
  '(most TIGER cities — they exist as extraction targets but never get '
  'a consumer-surface operator entry). Backfilled 2026-05-22 LATE: 157 '
  'rows → 155 distinct singular operators (4 singular orphans are '
  'nonprofits not in TIGER, already cascade-wired directly).';
