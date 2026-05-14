-- 20260514_polygon_to_operator_parent_chain.sql
--
-- Third fix in the OPRD chain. After the operator_aliases stopgap and the
-- pad_us_mng_name variant merge, OPRD attribution STILL returned NULL.
--
-- Root cause: polygon_to_operator's slug-match filter was `parent_operator_id
-- IS NULL` (canonical only). The OPRD canonical (id=1742) has slug
-- `oregon-parks-recreation-department` (old format, no "and", no state
-- suffix), but the deduped CHILD operators (id=42502, etc.) have the
-- expected slug `oregon-parks-and-recreation-department-or`. So the
-- dispatcher's slug lookup matched a child row, then skipped it because
-- of the `parent_operator_id IS NULL` filter.
--
-- Fix: match against ANY operator (child or canonical), then follow the
-- parent_operator_id chain via coalesce to return the canonical id.

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
    -- Two-step lookup. 2026-05-14: now follows parent_operator_id chain
    -- via coalesce so we resolve to the canonical even when the slug match
    -- lands on a deduped child row. Also consults operator_aliases as a
    -- fourth fallback before resolve_agency fuzzy match.
    when 'pad_us_unit' then
      v_loc_mang := p_claimed_values->>'loc_mang';

      -- (1) Try unit_name slug (federal seed identity)
      if v_polygon_name is not null and trim(v_polygon_name) <> '' then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and slug = public.slugify_agency(v_polygon_name || ' (' || p_state || ')')
           and is_active
         limit 1;
      end if;

      -- (2) Try loc_mang slug (state-and-below seed identity)
      if v_op_id is null and v_loc_mang is not null and trim(v_loc_mang) <> '' then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and slug = public.slugify_agency(v_loc_mang || ' (' || p_state || ')')
           and is_active
         limit 1;
      end if;

      -- (3) NEW: try operator_aliases on loc_mang (catches Pass-12 false-negatives)
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

      -- (4) NEW: try pad_us_mng_name array containment (catches variant merge)
      if v_op_id is null and v_loc_mang is not null and trim(v_loc_mang) <> '' then
        select coalesce(o.parent_operator_id, o.id) into v_op_id
          from public.operators o
         where o.state_code = p_state
           and o.is_active
           and v_loc_mang = ANY(o.pad_us_mng_name)
         limit 1;
      end if;

      -- (5) Fuzzy fallback on unit_name (unchanged)
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
