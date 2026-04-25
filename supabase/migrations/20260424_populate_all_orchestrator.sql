-- populate_all — simple sequential orchestrator (2026-04-24)
--
-- Calls every populator + resolver in a fixed sequence. Returns a jsonb
-- summary {step_name: rows_touched}.
--
-- Order:
--   1. Layer 1 direct-fill (spatial truth — needed by everything downstream)
--   2. Layer 2 populators (independent — order doesn't affect correctness)
--   3. Resolvers (one per field_group — order doesn't affect correctness)
--
-- DEFERRED (memory project_pipeline_step_ordering): make step order
-- configuration-driven via a steps registry table so order can be
-- changed without code edits, and Claude can re-order.

create or replace function public.populate_all(p_fid int default null)
returns jsonb
language plpgsql
as $$
declare
  result jsonb := '{}'::jsonb;
  c int;
begin
  -- Layer 1: direct-fill spatial truth
  c := public.populate_layer1_geographic(p_fid);
  result := result || jsonb_build_object('layer1_geographic', c);

  -- Layer 2: source-specific populators (each emits evidence rows)
  c := public.populate_from_cpad(p_fid);                 result := result || jsonb_build_object('cpad', c);
  c := public.populate_from_ccc(p_fid);                  result := result || jsonb_build_object('ccc', c);
  c := public.populate_from_jurisdictions(p_fid);        result := result || jsonb_build_object('jurisdictions', c);
  c := public.populate_from_csp_parks(p_fid);            result := result || jsonb_build_object('csp_parks', c);
  c := public.populate_from_park_operators(p_fid);       result := result || jsonb_build_object('park_operators', c);
  c := public.populate_from_nps_places(p_fid);           result := result || jsonb_build_object('nps_places', c);
  c := public.populate_from_tribal_lands(p_fid);         result := result || jsonb_build_object('tribal_lands', c);
  c := public.populate_from_military_bases(p_fid);       result := result || jsonb_build_object('military_bases', c);
  c := public.populate_from_private_land_zones(p_fid);   result := result || jsonb_build_object('private_land_zones', c);
  c := public.populate_from_research(p_fid);             result := result || jsonb_build_object('research', c);

  -- Resolvers: read all evidence per (fid, field_group), pick canonical,
  -- write back to staging columns. For each in-scope beach, run all 4.
  declare
    gov_count       int := 0;
    access_count    int := 0;
    dogs_count      int := 0;
    practical_count int := 0;
    f int;
  begin
    for f in
      select fid from public.us_beach_points_staging
      where p_fid is null or fid = p_fid
    loop
      if public.resolve_governance(f) is not null then gov_count       := gov_count + 1;       end if;
      if public.resolve_access(f)     is not null then access_count    := access_count + 1;    end if;
      if public.resolve_dogs(f)       is not null then dogs_count      := dogs_count + 1;      end if;
      if public.resolve_practical(f)  is not null then practical_count := practical_count + 1; end if;
    end loop;

    result := result || jsonb_build_object(
      'resolve_governance', gov_count,
      'resolve_access',     access_count,
      'resolve_dogs',       dogs_count,
      'resolve_practical',  practical_count
    );
  end;

  return result;
end;
$$;

comment on function public.populate_all(int) is
  'Sequential orchestrator: runs all populators + all 4 resolvers for one beach (when p_fid set) or every beach in staging (when null). Returns jsonb counts per step. Order is fixed in this Phase 1 implementation; future work makes it configuration-driven (see memory project_pipeline_step_ordering).';
