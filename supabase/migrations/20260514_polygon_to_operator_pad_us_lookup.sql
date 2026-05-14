-- 20260514_polygon_to_operator_pad_us_lookup.sql
--
-- Fourth fix in the OPRD chain. After the dispatcher's parent_chain fix,
-- spot-test on Cape Arago returned op_id=1742 correctly — but only because
-- I passed loc_mang explicitly in the test. The REAL polygon_containment
-- BEP row doesn't include loc_mang in its claimed_values jsonb. So when
-- populate_governance_gold calls polygon_to_operator with the actual
-- claimed_values, step 2 (loc_mang slug) is skipped.
--
-- Fix: when polygon_kind='pad_us_unit' and claimed_values lacks loc_mang,
-- look it up from pad_us_units by polygon_id. Same for mng_name.

begin;

create or replace function public.polygon_to_operator(p_polygon_kind text, p_claimed_values jsonb, p_state text)
 returns bigint
 language plpgsql
 stable
as $function$
declare
  v_op_id bigint;
  v_polygon_id text;
  v_polygon_name text;
  v_loc_mang text;
  v_mng_name text;
begin
  if p_polygon_kind is null or p_claimed_values is null or p_state is null then
    return null;
  end if;
  v_polygon_id   := p_claimed_values->>'polygon_id';
  v_polygon_name := p_claimed_values->>'polygon_name';

  case p_polygon_kind
    when 'c1_city' then
      if v_polygon_id is not null then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'city'
           and jurisdiction_id = v_polygon_id::bigint
           and is_active
         limit 1;
      end if;

    when 'cdp' then
      if v_polygon_id is not null then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'city'
           and jurisdiction_id = v_polygon_id::bigint
           and is_active
         limit 1;
      end if;

    when 'county' then
      if v_polygon_id is not null then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'county'
           and county_geoid = v_polygon_id
           and is_active
         limit 1;
      end if;

    -- ── PAD-US unit ──────────────────────────────────────────────────────
    -- 2026-05-14: when claimed_values lacks loc_mang/mng_name (which is
    -- usual — polygon_containment populator doesn't store them), look
    -- them up from pad_us_units by polygon_id.
    when 'pad_us_unit' then
      v_loc_mang := p_claimed_values->>'loc_mang';
      v_mng_name := p_claimed_values->>'mng_name';
      if (v_loc_mang is null or v_mng_name is null) and v_polygon_id is not null then
        select coalesce(v_loc_mang, pu.loc_mang),
               coalesce(v_mng_name, pu.mng_name)
          into v_loc_mang, v_mng_name
          from public.pad_us_units pu
         where pu.unit_id::text = v_polygon_id::text
           and pu.state = p_state
         limit 1;
      end if;

      -- (1) Try unit_name slug
      if v_polygon_name is not null and trim(v_polygon_name) <> '' then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and slug = public.slugify_agency(v_polygon_name || ' (' || p_state || ')')
           and is_active
         limit 1;
      end if;

      -- (2) Try loc_mang slug (now reliable — pulled from pad_us_units if missing)
      if v_op_id is null and v_loc_mang is not null and trim(v_loc_mang) <> '' then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and slug = public.slugify_agency(v_loc_mang || ' (' || p_state || ')')
           and is_active
         limit 1;
      end if;

      -- (3) operator_aliases on loc_mang
      if v_op_id is null and v_loc_mang is not null and trim(v_loc_mang) <> '' then
        select oa.canonical_operator_id into v_op_id
          from public.operator_aliases oa
          join public.operators o on o.id = oa.canonical_operator_id
         where o.state_code = p_state
           and o.is_active
           and (oa.alias_text = v_loc_mang
             or oa.pad_us_mng_name_variant = v_loc_mang)
         limit 1;
      end if;

      -- (4) pad_us_mng_name array containment on loc_mang
      if v_op_id is null and v_loc_mang is not null and trim(v_loc_mang) <> '' then
        select coalesce(o.parent_operator_id, o.id) into v_op_id
          from public.operators o
         where o.state_code = p_state
           and o.is_active
           and v_loc_mang = ANY(o.pad_us_mng_name)
         limit 1;
      end if;

      -- (5) Fuzzy fallback on unit_name
      if v_op_id is null and v_polygon_name is not null and trim(v_polygon_name) <> '' then
        select operator_id into v_op_id
          from public.resolve_agency(v_polygon_name, 'polygon_pad_us', p_state, 0.65)
         order by confidence desc limit 1;
      end if;

    when 'cpad_unit' then
      if v_polygon_name is not null then
        select operator_id into v_op_id
          from public.resolve_agency(v_polygon_name, 'cpad_unit', p_state, 0.65)
         order by confidence desc limit 1;
      end if;

    when 'military_base' then
      if v_polygon_name is not null then
        select operator_id into v_op_id
          from public.resolve_agency(v_polygon_name, 'military_base', p_state, 0.65)
         order by confidence desc limit 1;
      end if;

    when 'tribal_land' then
      if v_polygon_name is not null then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'tribal'
           and slug = public.slugify_agency(v_polygon_name || ' (' || p_state || ')')
           and is_active
         limit 1;
        if v_op_id is null then
          select operator_id into v_op_id
            from public.resolve_agency(v_polygon_name, 'tribal_land', p_state, 0.65)
           order by confidence desc limit 1;
        end if;
      end if;

    else
      v_op_id := null;
  end case;

  return v_op_id;
end $function$;

commit;
