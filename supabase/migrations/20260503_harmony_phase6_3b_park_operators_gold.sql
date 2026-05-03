-- 20260503_harmony_phase6_3b_park_operators_gold.sql
--
-- Phase 6.3b of the harmony migration (see docs/harmony.md and
-- project_harmony_pipeline_migration.md).
--
-- Un-defers the first of the three legacy non-containment populators —
-- park_operators. Adds populate_from_park_operators_gold(p_fid) that
-- captures the "CDPR-named state beach actually leased to a city/county"
-- pattern (project_state_park_operators.md) as governance evidence keyed
-- on gold_fid.
--
-- Mirrors populate_from_cpad_gold's shape (gov/access populator from
-- phase 3) — joins are different (csp_parks → park_operators lookup
-- instead of cpad_units spatial), but the emit pattern + ON CONFLICT
-- semantics are identical.
--
-- Joins:
--   beaches_gold (g)
--     × csp_parks (c)        on ST_Contains(c.geom, g.geom)
--     × park_operators (po)  on po.park_name = c.unit_name
--
-- Output: one canonical 'governance' evidence row per beach with
-- source='park_operators', polygon-area-asc tiebreak when multiple
-- containing csp polygons exist for the same beach.
--
-- This is the FIRST of 3 deferred populators (6.3b/c). Slice 6.3c
-- (park_url + research) requires more work — they touch
-- park_url_extractions / policy_research_extractions tables and the
-- buffer-rescued attribution path that joins us_beach_points +
-- beach_cpad_candidates. Save for a follow-up.

begin;

create or replace function public.populate_from_park_operators_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with matches as (
    select distinct on (g.fid) g.fid,
           po.operator_jurisdiction, po.operator_body, po.notes
    from public.beaches_gold g
    join public.csp_parks c on st_contains(c.geom, g.geom::geometry)
    join public.park_operators po on po.park_name = c.unit_name
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
    order by g.fid, st_area(c.geom::geography) asc
  ),
  with_type as (
    select fid, operator_body, notes,
      case operator_jurisdiction
        when 'governing city'    then 'city'
        when 'governing county'  then 'county'
        when 'governing state'   then 'state'
        when 'governing federal' then 'federal'
        when 'governing private' then 'private'
        else                          'unknown'
      end as gov_type
    from matches
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, notes, updated_at)
    select fid, 'governance', 'park_operators', 0.95,
      jsonb_build_object(
        'type', gov_type,
        'name', public.canonical_agency_name(gov_type, operator_body)
      ),
      notes, now()
    from with_type
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          notes          = excluded.notes,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end;
$$;

comment on function public.populate_from_park_operators_gold(bigint) is
  'Phase 6.3b of harmony migration. Captures "CDPR state beach actually leased to city/county" governance pattern (project_state_park_operators.md). Emits governance evidence keyed on gold_fid. Source priority: manual > llm > park_operators (within governance field_group). Populator IS the resolver — picks smallest containing csp_parks polygon as canonical.';

commit;
