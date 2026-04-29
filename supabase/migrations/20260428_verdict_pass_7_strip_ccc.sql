-- Pass 7 — strip CCC signals from the cascade.
--
-- Removes two tiers from compute_dogs_verdict:
--   * ccc_native (ccc_access_points.dog_friendly, weight 0.66)
--   * ccc_llm    (geo_entity_response_current entity_type='ccc')
--
-- Per Franz: "for the 10th time, don't use CCC data in this pipeline."
-- The verdict layer must work cross-state; CCC has no PAD-US equivalent.
-- CCC remains as raw point geometry for spatial joins (CPAD lookup,
-- operator_id attribution), but is NOT a policy signal in the cascade.
--
-- The Pass 6 version of compute_dogs_verdict that DID use CCC tiers is
-- preserved in supabase/migrations/20260428_verdict_pass_6_cpad_unit_and_namematch.sql
-- and remains callable as compute_dogs_verdict_ccc_friendly() so it
-- can be invoked for audit / A-B comparison if needed. It writes to a
-- separate column (dogs_verdict_ccc_friendly) to avoid clobbering the
-- live cascade.
--
-- Cascade after Pass 7:
--   1. cpad_manual override (admin URL — short-circuits)
--   2. weighted vote across:
--        cpad_unit_exception   (1.0) — per-beach quote in unit policy
--        cpad_unit_default     (0.7) — unit's default_rule
--        legacy cpad_web/llm   (var) — only if no cpad_unit row for unit
--        operator_exception    (1.0) — per-beach quote in operator policy
--        operator_default      (0.4) — operator's default_rule
--   3. margin guard, tie defaults to 'no'.

begin;

-- 1. Preserve the Pass 6 ccc-using version as compute_dogs_verdict_ccc_friendly.
--    Writes to ccc_access_points.dogs_verdict_ccc_friendly (separate column)
--    so it doesn't compete with the live CCC-free cascade.

alter table public.ccc_access_points
  add column if not exists dogs_verdict_ccc_friendly            text,
  add column if not exists dogs_verdict_ccc_friendly_confidence numeric,
  add column if not exists dogs_verdict_ccc_friendly_meta       jsonb;

create or replace function public.compute_dogs_verdict_ccc_friendly(p_objectid integer)
returns void
language plpgsql security definer as $$
declare
  v_pt          geometry;
  v_name        text;
  v_native      text;
  v_native_wt   constant numeric := 0.66;
  v_unit_id     integer;
  v_unit_name   text;
  v_cpad_val    text;  v_cpad_wt numeric;  v_cpad_url text;  v_cpad_kind text;
  v_cu_val      text;  v_cu_wt   numeric;  v_cu_tag   text;
  v_ccc_val     text;  v_ccc_wt  numeric;  v_ccc_url  text;
  v_op_id       bigint;
  v_op_val      text;  v_op_wt   numeric;  v_op_tag   text;
  v_yes_wt      numeric := 0;
  v_no_wt       numeric := 0;
  v_sources     text[]  := array[]::text[];
  v_verdict     text;   v_conf numeric;  v_margin numeric;
  v_review      boolean := false;
  v_meta        jsonb;
begin
  select c.geom, c.name,
         case lower(trim(c.dog_friendly)) when 'yes' then 'yes' when 'no' then 'no' else null end,
         c.operator_id
    into v_pt, v_name, v_native, v_op_id
    from public.ccc_access_points c where c.objectid = p_objectid;
  if v_pt is null then return; end if;

  if v_native is not null then
    if v_native = 'yes' then v_yes_wt := v_yes_wt + v_native_wt;
    else                     v_no_wt  := v_no_wt  + v_native_wt; end if;
    v_sources := array_append(v_sources, 'ccc_native');
  end if;

  select cu.unit_id, cu.unit_name into v_unit_id, v_unit_name
    from public.cpad_units cu
   where st_contains(cu.geom, v_pt::geometry)
   order by
     (cu.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
     similarity(public.clean_beach_name(coalesce(v_name,'')),
                public.clean_beach_name(coalesce(cu.unit_name,''))) desc,
     (cu.unit_name ~* '\mbeach\M')::int desc,
     st_area(cu.geom) asc
   limit 1;

  if v_unit_id is not null then
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_cu_val
      from public.cpad_unit_dogs_policy p, jsonb_array_elements(coalesce(p.exceptions,'[]'::jsonb)) e
     where p.cpad_unit_id = v_unit_id and v_name is not null
       and e->>'beach_name' is not null and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and ( similarity(public.clean_beach_name(v_name), public.clean_beach_name(e->>'beach_name')) >= 0.65
          or public.clean_beach_name(v_name) like '%' || public.clean_beach_name(e->>'beach_name') || '%'
          or public.clean_beach_name(e->>'beach_name') like '%' || public.clean_beach_name(v_name) || '%' )
     order by (public.clean_beach_name(v_name) = public.clean_beach_name(e->>'beach_name'))::int desc,
              similarity(public.clean_beach_name(v_name), public.clean_beach_name(e->>'beach_name')) desc
     limit 1;
    if v_cu_val is not null then v_cu_wt := 1.0; v_cu_tag := 'cpad_unit_exception';
    else
      select case when default_rule in ('yes','restricted') then 'yes'
                  when default_rule = 'no' then 'no' else null end
        into v_cu_val from public.cpad_unit_dogs_policy where cpad_unit_id = v_unit_id;
      if v_cu_val is not null then v_cu_wt := 0.7; v_cu_tag := 'cpad_unit_default'; end if;
    end if;
  end if;

  if v_cu_val is null and v_unit_id is not null then
    select case when r.response_value in ('yes','restricted','seasonal') then 'yes'
                when r.response_value = 'no' then 'no' else null end,
           r.response_confidence, r.source_url
      into v_cpad_val, v_cpad_wt, v_cpad_url
      from public.geo_entity_response_current r
     where r.entity_type = 'cpad' and r.entity_id = v_unit_id and r.response_scope = 'dogs_allowed';
    v_cpad_kind := case
      when v_cpad_url is null              then null
      when v_cpad_url like 'admin://%'     then 'cpad_manual'
      when v_cpad_url like 'llm-prior://%' then 'cpad_llm'
      else 'cpad_web'
    end;
  end if;

  select case when response_value in ('yes','restricted','seasonal') then 'yes'
              when response_value = 'no' then 'no' else null end,
         response_confidence, source_url
    into v_ccc_val, v_ccc_wt, v_ccc_url
    from public.geo_entity_response_current
   where entity_type = 'ccc' and entity_id = p_objectid and response_scope = 'dogs_allowed';

  if v_op_id is not null and v_name is not null then
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_op_val
      from public.operator_dogs_policy odp, jsonb_array_elements(odp.exceptions) e
     where odp.operator_id = v_op_id and e->>'rule' is not null
       and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and ( similarity(public.clean_beach_name(v_name), public.clean_beach_name(e->>'beach_name')) >= 0.65
          or public.clean_beach_name(v_name) like '%' || public.clean_beach_name(e->>'beach_name') || '%'
          or public.clean_beach_name(e->>'beach_name') like '%' || public.clean_beach_name(v_name) || '%' )
     order by (public.clean_beach_name(v_name) = public.clean_beach_name(e->>'beach_name'))::int desc,
              similarity(public.clean_beach_name(v_name), public.clean_beach_name(e->>'beach_name')) desc
     limit 1;
    if v_op_val is not null then v_op_wt := 1.0; v_op_tag := 'operator_exception';
    else
      select case when default_rule in ('yes','restricted') then 'yes'
                  when default_rule = 'no' then 'no' else null end
        into v_op_val from public.operator_dogs_policy where operator_id = v_op_id;
      v_op_wt := 0.4; v_op_tag := 'operator_default';
    end if;
  end if;

  if v_cpad_kind = 'cpad_manual' and v_cpad_val is not null then
    v_verdict := v_cpad_val; v_conf := coalesce(v_cpad_wt,1.0); v_margin := null; v_review := false;
    v_sources := array['cpad_manual'];
    v_meta := jsonb_build_object('override','manual','sources',to_jsonb(v_sources),
                                 'cpad_unit_id', v_unit_id, 'review', false, 'computed_at', now());
  else
    if v_cu_val   is not null then if v_cu_val   = 'yes' then v_yes_wt := v_yes_wt + v_cu_wt;   else v_no_wt := v_no_wt + v_cu_wt;   end if; v_sources := array_append(v_sources, v_cu_tag); end if;
    if v_cpad_val is not null then if v_cpad_val = 'yes' then v_yes_wt := v_yes_wt + v_cpad_wt; else v_no_wt := v_no_wt + v_cpad_wt; end if; v_sources := array_append(v_sources, v_cpad_kind); end if;
    if v_ccc_val  is not null then if v_ccc_val  = 'yes' then v_yes_wt := v_yes_wt + v_ccc_wt;  else v_no_wt := v_no_wt + v_ccc_wt;  end if; v_sources := array_append(v_sources, 'ccc_llm'); end if;
    if v_op_val   is not null then if v_op_val   = 'yes' then v_yes_wt := v_yes_wt + v_op_wt;   else v_no_wt := v_no_wt + v_op_wt;   end if; v_sources := array_append(v_sources, v_op_tag); end if;

    if array_length(v_sources, 1) is null then
      v_verdict := null; v_conf := null; v_margin := null;
    else
      if v_yes_wt > v_no_wt then
        v_verdict := 'yes'; v_conf := v_yes_wt / (v_yes_wt + v_no_wt); v_margin := v_yes_wt - v_no_wt;
      elsif v_no_wt > v_yes_wt then
        v_verdict := 'no';  v_conf := v_no_wt  / (v_yes_wt + v_no_wt); v_margin := v_no_wt - v_yes_wt;
      else
        v_verdict := 'no';  v_conf := 0.5; v_margin := 0;
      end if;
      v_review := (v_yes_wt > 0 and v_no_wt > 0 and v_margin < 0.10);
    end if;
    v_meta := jsonb_build_object('yes_weight', round(v_yes_wt,4), 'no_weight', round(v_no_wt,4),
                                 'margin', case when v_margin is not null then round(v_margin,4) end,
                                 'sources', to_jsonb(v_sources), 'cpad_unit_id', v_unit_id,
                                 'review', v_review, 'computed_at', now());
  end if;

  update public.ccc_access_points
     set dogs_verdict_ccc_friendly            = v_verdict,
         dogs_verdict_ccc_friendly_confidence = case when v_conf is not null then round(v_conf,4) end,
         dogs_verdict_ccc_friendly_meta       = v_meta
   where objectid = p_objectid;
end;
$$;

-- 2. Replace compute_dogs_verdict with the CCC-free version.

create or replace function public.compute_dogs_verdict(p_objectid integer)
returns void
language plpgsql security definer as $$
declare
  v_pt          geometry;
  v_name        text;
  v_unit_id     integer;
  v_unit_name   text;

  v_cpad_val    text;  v_cpad_wt numeric;  v_cpad_url text;  v_cpad_kind text;
  v_cu_val      text;  v_cu_wt   numeric;  v_cu_tag   text;

  v_op_id       bigint;
  v_op_val      text;  v_op_wt   numeric;  v_op_tag   text;

  v_yes_wt      numeric := 0;
  v_no_wt       numeric := 0;
  v_sources     text[]  := array[]::text[];
  v_verdict     text;   v_conf numeric;  v_margin numeric;
  v_review      boolean := false;
  v_meta        jsonb;
begin
  -- CCC point used only for geometry + operator_id, NOT as a policy signal.
  select c.geom, c.name, c.operator_id
    into v_pt, v_name, v_op_id
    from public.ccc_access_points c
   where c.objectid = p_objectid;
  if v_pt is null then return; end if;

  -- Containing CPAD unit — full Tier 1 ranking.
  select cu.unit_id, cu.unit_name into v_unit_id, v_unit_name
    from public.cpad_units cu
   where st_contains(cu.geom, v_pt::geometry)
   order by
     (cu.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
     similarity(public.clean_beach_name(coalesce(v_name,'')),
                public.clean_beach_name(coalesce(cu.unit_name,''))) desc,
     (cu.unit_name ~* '\mbeach\M')::int desc,
     st_area(cu.geom) asc
   limit 1;

  -- cpad_unit_dogs_policy first — exception then default.
  if v_unit_id is not null then
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_cu_val
      from public.cpad_unit_dogs_policy p, jsonb_array_elements(coalesce(p.exceptions,'[]'::jsonb)) e
     where p.cpad_unit_id = v_unit_id and v_name is not null
       and e->>'beach_name' is not null and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and ( similarity(public.clean_beach_name(v_name), public.clean_beach_name(e->>'beach_name')) >= 0.65
          or public.clean_beach_name(v_name) like '%' || public.clean_beach_name(e->>'beach_name') || '%'
          or public.clean_beach_name(e->>'beach_name') like '%' || public.clean_beach_name(v_name) || '%' )
     order by (public.clean_beach_name(v_name) = public.clean_beach_name(e->>'beach_name'))::int desc,
              similarity(public.clean_beach_name(v_name), public.clean_beach_name(e->>'beach_name')) desc
     limit 1;
    if v_cu_val is not null then v_cu_wt := 1.0; v_cu_tag := 'cpad_unit_exception';
    else
      select case when default_rule in ('yes','restricted') then 'yes'
                  when default_rule = 'no' then 'no' else null end
        into v_cu_val from public.cpad_unit_dogs_policy where cpad_unit_id = v_unit_id;
      if v_cu_val is not null then v_cu_wt := 0.7; v_cu_tag := 'cpad_unit_default'; end if;
    end if;
  end if;

  -- Legacy GERC 'cpad' fallback only when cpad_unit policy absent.
  if v_cu_val is null and v_unit_id is not null then
    select case when r.response_value in ('yes','restricted','seasonal') then 'yes'
                when r.response_value = 'no' then 'no' else null end,
           r.response_confidence, r.source_url
      into v_cpad_val, v_cpad_wt, v_cpad_url
      from public.geo_entity_response_current r
     where r.entity_type = 'cpad' and r.entity_id = v_unit_id and r.response_scope = 'dogs_allowed';
    v_cpad_kind := case
      when v_cpad_url is null              then null
      when v_cpad_url like 'admin://%'     then 'cpad_manual'
      when v_cpad_url like 'llm-prior://%' then 'cpad_llm'
      else 'cpad_web'
    end;
  end if;

  -- Operator (exception → default).
  if v_op_id is not null and v_name is not null then
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_op_val
      from public.operator_dogs_policy odp, jsonb_array_elements(odp.exceptions) e
     where odp.operator_id = v_op_id and e->>'rule' is not null
       and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and ( similarity(public.clean_beach_name(v_name), public.clean_beach_name(e->>'beach_name')) >= 0.65
          or public.clean_beach_name(v_name) like '%' || public.clean_beach_name(e->>'beach_name') || '%'
          or public.clean_beach_name(e->>'beach_name') like '%' || public.clean_beach_name(v_name) || '%' )
     order by (public.clean_beach_name(v_name) = public.clean_beach_name(e->>'beach_name'))::int desc,
              similarity(public.clean_beach_name(v_name), public.clean_beach_name(e->>'beach_name')) desc
     limit 1;
    if v_op_val is not null then v_op_wt := 1.0; v_op_tag := 'operator_exception';
    else
      select case when default_rule in ('yes','restricted') then 'yes'
                  when default_rule = 'no' then 'no' else null end
        into v_op_val from public.operator_dogs_policy where operator_id = v_op_id;
      v_op_wt := 0.4; v_op_tag := 'operator_default';
    end if;
  end if;

  -- cpad_manual override short-circuits.
  if v_cpad_kind = 'cpad_manual' and v_cpad_val is not null then
    v_verdict := v_cpad_val; v_conf := coalesce(v_cpad_wt,1.0); v_margin := null; v_review := false;
    v_sources := array['cpad_manual'];
    v_meta := jsonb_build_object('override','manual','sources',to_jsonb(v_sources),
                                 'cpad_unit_id', v_unit_id, 'review', false, 'computed_at', now());
  else
    if v_cu_val   is not null then if v_cu_val   = 'yes' then v_yes_wt := v_yes_wt + v_cu_wt;   else v_no_wt := v_no_wt + v_cu_wt;   end if; v_sources := array_append(v_sources, v_cu_tag); end if;
    if v_cpad_val is not null then if v_cpad_val = 'yes' then v_yes_wt := v_yes_wt + v_cpad_wt; else v_no_wt := v_no_wt + v_cpad_wt; end if; v_sources := array_append(v_sources, v_cpad_kind); end if;
    if v_op_val   is not null then if v_op_val   = 'yes' then v_yes_wt := v_yes_wt + v_op_wt;   else v_no_wt := v_no_wt + v_op_wt;   end if; v_sources := array_append(v_sources, v_op_tag); end if;

    if array_length(v_sources, 1) is null then
      v_verdict := null; v_conf := null; v_margin := null;
    else
      if v_yes_wt > v_no_wt then
        v_verdict := 'yes'; v_conf := v_yes_wt / (v_yes_wt + v_no_wt); v_margin := v_yes_wt - v_no_wt;
      elsif v_no_wt > v_yes_wt then
        v_verdict := 'no';  v_conf := v_no_wt  / (v_yes_wt + v_no_wt); v_margin := v_no_wt - v_yes_wt;
      else
        v_verdict := 'no';  v_conf := 0.5; v_margin := 0;
      end if;
      v_review := (v_yes_wt > 0 and v_no_wt > 0 and v_margin < 0.10);
    end if;
    v_meta := jsonb_build_object('yes_weight', round(v_yes_wt,4), 'no_weight', round(v_no_wt,4),
                                 'margin', case when v_margin is not null then round(v_margin,4) end,
                                 'sources', to_jsonb(v_sources), 'cpad_unit_id', v_unit_id,
                                 'review', v_review, 'computed_at', now());
  end if;

  update public.ccc_access_points
     set dogs_verdict            = v_verdict,
         dogs_verdict_confidence = case when v_conf is not null then round(v_conf,4) end,
         dogs_verdict_meta       = v_meta
   where objectid = p_objectid;
end;
$$;

-- 3. Recompute_all variant for the ccc-friendly fork (callable on demand).

create or replace function public.recompute_all_dogs_verdicts_ccc_friendly()
returns integer
language plpgsql security definer as $$
declare n integer := 0; r record;
begin
  for r in (select objectid from public.ccc_access_points
             where (archived is null or archived <> 'Yes')) loop
    perform public.compute_dogs_verdict_ccc_friendly(r.objectid);
    n := n + 1;
  end loop;
  return n;
end;
$$;

commit;
