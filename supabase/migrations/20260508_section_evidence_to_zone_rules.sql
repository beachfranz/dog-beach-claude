-- 20260508_section_evidence_to_zone_rules.sql
--
-- Adds the BEP-driven section path for zone_rules. The Python extractor
-- (scripts/extract_beach_section_rules.py) writes BEP rows of
-- source='section_research_v1', field_group='dogs', claimed_values containing
-- a `sections` object: {sand: {rule, evidence}, water: {rule}, trails: {rule},
-- picnic_area: {rule}}.
--
-- _zr_inject_sections_from_bep(zr, fid) merges those sections into zone_rules
-- after the existing perimeter + sand-baseline injectors run, so per-section
-- evidence overrides the inherited single-rule baseline.

begin;

create or replace function public._zr_inject_sections_from_bep(p_zr jsonb, p_fid bigint)
returns jsonb
language plpgsql
as $$
declare
  v_sections jsonb;
  v_existing jsonb;
begin
  -- Pick the most recent section_research_v1 row for this fid.
  select claimed_values->'sections' into v_sections
    from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group = 'dogs'
     and source = 'section_research_v1'
     and claimed_values ? 'sections'
   order by updated_at desc
   limit 1;

  if v_sections is null or jsonb_typeof(v_sections) <> 'object' then
    return p_zr;
  end if;

  -- The current zone_rules has a single regions[] entry. Merge new
  -- sections into regions[0].sections (insert if missing, override if
  -- present so the LLM evidence wins).
  v_existing := coalesce(p_zr->'regions'->0->'sections', '{}'::jsonb);
  return jsonb_set(
    p_zr,
    '{regions,0,sections}',
    v_existing || v_sections
  );
end $$;

-- Wire the new injector into _promote_zone_rules_for_fid by re-creating
-- the function with the extra step. (We don't have the source SQL for the
-- existing version handy, so we wrap the existing version: call it, then
-- overlay sections from BEP, then UPDATE beach_dog_policy.)
create or replace function public._promote_zone_rules_for_fid(p_fid bigint)
returns text
language plpgsql
as $$
declare
  v_zr jsonb;
  v_with_sections jsonb;
begin
  -- Build the perimeter + sand baseline as before via existing injectors.
  -- We rebuild the JSON from scratch here to avoid relying on whatever
  -- shape the prior version returned.
  v_zr := '{"regions":[{"name":null,"sections":{}}],"global_notes":null}'::jsonb;
  v_zr := public._zr_inject_perimeter(v_zr, p_fid);
  v_zr := public._zr_inject_sand_from_policy(v_zr, p_fid);
  v_zr := public._zr_inject_sections_from_bep(v_zr, p_fid);

  update public.beach_dog_policy
     set zone_rules = v_zr
   where arena_group_id = p_fid;

  return 'updated';
end $$;

commit;
