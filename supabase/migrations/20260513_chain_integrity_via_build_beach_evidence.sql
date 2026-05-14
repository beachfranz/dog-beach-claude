-- 20260513_chain_integrity_via_build_beach_evidence.sql
--
-- After the collapse migration, the populator calls that used to live
-- in promote_to_gold + refire_bep_cascade now live exclusively in
-- build_beach_evidence(p_fid). Both wrappers delegate.
--
-- Update assert_populator_chains_intact() to scan build_beach_evidence
-- for populator presence (the new chain holder), and verify both
-- wrappers correctly delegate to it. Column check stays on
-- promote_to_gold since that's where the INSERT lives.

begin;

create or replace function public.assert_populator_chains_intact()
returns boolean
language plpgsql
stable
as $function$
declare
  v_build_src    text;
  v_promote_src  text;
  v_refire_src   text;
  v_required text[] := array[
    'populate_polygon_containment_gold',
    'populate_from_cpad_gold',
    'populate_from_pad_us_gold',
    'populate_from_park_operators_gold',
    'populate_from_operators_gold',
    'populate_from_state_default_gold',
    'populate_from_research_gold',
    'populate_from_park_url_gold',
    'populate_from_park_url_governance_gold',
    'populate_from_unified_v1_gold',
    'populate_from_city_dog_policy_gold',
    'populate_from_county_dog_policy_gold',
    '_emit_evidence_from_osm_amenities'
  ];
  v_required_columns text[] := array[
    'county_fips',  -- ★ issue #2/#18 lesson
    'park_name',    -- ★ issue #2 lesson
    'state',
    'location_id',
    'name'
  ];
  v_fn text;
  v_col text;
  v_missing text[] := array[]::text[];
begin
  -- Fetch source for the canonical chain holder + both wrappers
  select pg_get_functiondef(p.oid) into v_build_src
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'public' and p.proname = 'build_beach_evidence'
   limit 1;

  select pg_get_functiondef(p.oid) into v_promote_src
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'public' and p.proname = 'promote_to_gold'
   limit 1;

  select pg_get_functiondef(p.oid) into v_refire_src
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'public' and p.proname = 'refire_bep_cascade'
   limit 1;

  if v_build_src is null then
    raise exception 'public.build_beach_evidence not found — collapse migration missing';
  end if;
  if v_promote_src is null then
    raise exception 'public.promote_to_gold not found';
  end if;
  if v_refire_src is null then
    raise exception 'public.refire_bep_cascade not found';
  end if;

  -- Each populator must appear in build_beach_evidence (the chain holder)
  foreach v_fn in array v_required loop
    if v_build_src not ilike '%' || v_fn || '%' then
      v_missing := v_missing || ('build_beach_evidence missing: ' || v_fn);
    end if;
  end loop;

  -- Both wrappers must delegate to build_beach_evidence (otherwise the
  -- collapse has been undone and populator calls have drifted back inline)
  if v_promote_src not ilike '%build_beach_evidence%' then
    v_missing := v_missing || 'promote_to_gold no longer delegates to build_beach_evidence';
  end if;
  if v_refire_src not ilike '%build_beach_evidence%' then
    v_missing := v_missing || 'refire_bep_cascade no longer delegates to build_beach_evidence';
  end if;

  -- Each required INSERT column must appear in promote_to_gold (still owns INSERT)
  foreach v_col in array v_required_columns loop
    if v_promote_src not ilike '%' || v_col || '%' then
      v_missing := v_missing || ('promote_to_gold INSERT missing column: ' || v_col);
    end if;
  end loop;

  if array_length(v_missing, 1) > 0 then
    raise exception
      'populator chain integrity FAIL — % drift(s) detected: %',
      array_length(v_missing, 1), v_missing;
  end if;

  return true;
end $function$;

commit;
