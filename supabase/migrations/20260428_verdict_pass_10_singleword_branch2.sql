-- Pass 10 — single-word cleaned exception names require exact match;
-- multi-word cleaned exceptions allow fuzzy (trigram or substring).
--
-- Truth-set comparison surfaced Coronado Dog Beach as a false negative
-- (3 externals say off-leash; we said no). Diagnosis:
--
-- City of Coronado's operator_dogs_policy.exceptions[] contains both
--   {beach_name: "Dog Beach",       rule: off_leash}
--   {beach_name: "Coronado Beach",  rule: prohibited}
--
-- Beach "Coronado Dog Beach" cleans to "coronado dog". Both exceptions
-- match via branch 2 (clean(beach) LIKE '%clean(exc)%'):
--   "coronado dog" LIKE '%dog%'      → true
--   "coronado dog" LIKE '%coronado%' → true
--
-- ORDER BY trigram desc picks "Coronado" (similarity 0.5) over "Dog"
-- (similarity 0.27), so the prohibition wins. Wrong answer for the
-- city's official off-leash dog beach.
--
-- Pattern: when the cleaned exception name is a single token like
-- "dog" / "coronado" / "pismo", branch 2 fires too generously because
-- the beach name happens to contain that one common word.
--
-- Quantified: of 40 branch-2-only matches across active surface, 37
-- have single-word cleaned exception names — the buggy pattern.
--
-- Fix: branch 2 now requires the cleaned exception to be multi-word
-- (contain at least one space). Single-word cleaned exceptions still
-- match if they're a strong trigram match for the beach (branch 1) —
-- which catches "Pismo State Beach" matching exception "Pismo State
-- Beach" via similarity=1, while rejecting "Coronado Dog Beach"
-- matching exception "Dog Beach" via the buggy substring path.

create or replace function public.compute_dogs_verdict_core(
  p_geom geometry, p_name text, p_operator_id bigint
) returns table (verdict text, confidence numeric, meta jsonb)
language plpgsql security definer as $$
declare
  v_unit_id     integer;
  v_unit_name   text;
  v_cpad_val    text;  v_cpad_wt numeric;  v_cpad_url text;  v_cpad_kind text;
  v_cu_val      text;  v_cu_wt   numeric;  v_cu_tag   text;
  v_op_val      text;  v_op_wt   numeric;  v_op_tag   text;
  v_yes_wt      numeric := 0;
  v_no_wt       numeric := 0;
  v_sources     text[]  := array[]::text[];
  v_verdict     text;   v_conf numeric;  v_margin numeric;
  v_review      boolean := false;
  v_meta        jsonb;
begin
  if p_geom is null then
    return query select null::text, null::numeric, null::jsonb;
    return;
  end if;

  select cu.unit_id, cu.unit_name into v_unit_id, v_unit_name
    from public.cpad_units cu
   where st_contains(cu.geom, p_geom::geometry)
   order by
     (cu.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
     similarity(public.clean_beach_name(coalesce(p_name,'')),
                public.clean_beach_name(coalesce(cu.unit_name,''))) desc,
     (cu.unit_name ~* '\mbeach\M')::int desc,
     st_area(cu.geom) asc
   limit 1;

  if v_unit_id is not null then
    -- cpad_unit_exception: branch 2 requires multi-word cleaned exception
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_cu_val
      from public.cpad_unit_dogs_policy p, jsonb_array_elements(coalesce(p.exceptions,'[]'::jsonb)) e
     where p.cpad_unit_id = v_unit_id and p_name is not null
       and e->>'beach_name' is not null and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and (
         -- multi-word cleaned exception: trigram OR substring
         ( position(' ' in public.clean_beach_name(e->>'beach_name')) > 0
           and (
             similarity(public.clean_beach_name(p_name), public.clean_beach_name(e->>'beach_name')) >= 0.65
             or public.clean_beach_name(p_name) like '%' || public.clean_beach_name(e->>'beach_name') || '%'
           )
         )
         or
         -- single-word cleaned exception: exact match only
         ( position(' ' in public.clean_beach_name(e->>'beach_name')) = 0
           and public.clean_beach_name(p_name) = public.clean_beach_name(e->>'beach_name')
         )
       )
     order by (public.clean_beach_name(p_name) = public.clean_beach_name(e->>'beach_name'))::int desc,
              similarity(public.clean_beach_name(p_name), public.clean_beach_name(e->>'beach_name')) desc
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

  if p_operator_id is not null and p_name is not null then
    -- operator_exception: branch 2 requires multi-word cleaned exception
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_op_val
      from public.operator_dogs_policy odp, jsonb_array_elements(odp.exceptions) e
     where odp.operator_id = p_operator_id and e->>'rule' is not null
       and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and (
         -- multi-word cleaned exception: trigram OR substring
         ( position(' ' in public.clean_beach_name(e->>'beach_name')) > 0
           and (
             similarity(public.clean_beach_name(p_name), public.clean_beach_name(e->>'beach_name')) >= 0.65
             or public.clean_beach_name(p_name) like '%' || public.clean_beach_name(e->>'beach_name') || '%'
           )
         )
         or
         -- single-word cleaned exception: exact match only
         ( position(' ' in public.clean_beach_name(e->>'beach_name')) = 0
           and public.clean_beach_name(p_name) = public.clean_beach_name(e->>'beach_name')
         )
       )
     order by (public.clean_beach_name(p_name) = public.clean_beach_name(e->>'beach_name'))::int desc,
              similarity(public.clean_beach_name(p_name), public.clean_beach_name(e->>'beach_name')) desc
     limit 1;
    if v_op_val is not null then v_op_wt := 1.0; v_op_tag := 'operator_exception';
    else
      select case when default_rule in ('yes','restricted') then 'yes'
                  when default_rule = 'no' then 'no' else null end
        into v_op_val from public.operator_dogs_policy where operator_id = p_operator_id;
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

  return query select v_verdict,
                      case when v_conf is not null then round(v_conf,4) end,
                      v_meta;
end;
$$;
