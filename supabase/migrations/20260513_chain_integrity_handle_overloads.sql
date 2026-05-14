-- 20260513_chain_integrity_handle_overloads.sql
--
-- After the mode-flag migration, build_beach_evidence has two overloads:
--   build_beach_evidence(bigint)         -- thin wrapper to (fid, 'full')
--   build_beach_evidence(bigint, text)   -- the canonical chain body
--
-- assert_populator_chains_intact() used `limit 1` on the pg_proc lookup,
-- so it picked the wrapper (no populator names in body) and erroneously
-- failed. Fix: concat ALL build_beach_evidence overloads' source so the
-- check inspects every variant.
--
-- Also: add _emit_evidence_from_pad_us_dogs_policy to the required list
-- so we can't accidentally remove the new pad-us-dogs wiring.

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
    '_emit_evidence_from_osm_amenities',
    '_emit_evidence_from_pad_us_dogs_policy'   -- 2026-05-13: agency-default dogs
  ];
  v_required_columns text[] := array[
    'county_fips', 'park_name', 'state', 'location_id', 'name'
  ];
  v_fn text;
  v_col text;
  v_missing text[] := array[]::text[];
begin
  -- Concatenate source of ALL build_beach_evidence overloads (wrapper + body)
  select string_agg(pg_get_functiondef(p.oid), E'\n')
    into v_build_src
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'public' and p.proname = 'build_beach_evidence';

  select pg_get_functiondef(p.oid) into v_promote_src
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'public' and p.proname = 'promote_to_gold'
   limit 1;

  select pg_get_functiondef(p.oid) into v_refire_src
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'public' and p.proname = 'refire_bep_cascade'
   limit 1;

  if v_build_src is null then
    raise exception 'public.build_beach_evidence not found';
  end if;
  if v_promote_src is null then
    raise exception 'public.promote_to_gold not found';
  end if;
  if v_refire_src is null then
    raise exception 'public.refire_bep_cascade not found';
  end if;

  foreach v_fn in array v_required loop
    if v_build_src not ilike '%' || v_fn || '%' then
      v_missing := v_missing || ('build_beach_evidence missing: ' || v_fn);
    end if;
  end loop;

  if v_promote_src not ilike '%build_beach_evidence%' then
    v_missing := v_missing || 'promote_to_gold no longer delegates to build_beach_evidence';
  end if;
  if v_refire_src not ilike '%build_beach_evidence%' then
    v_missing := v_missing || 'refire_bep_cascade no longer delegates to build_beach_evidence';
  end if;

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
