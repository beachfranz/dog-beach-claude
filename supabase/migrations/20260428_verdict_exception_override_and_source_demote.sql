-- Two cascade refinements:
--
-- (#1) Hard-override for operator_exception. When a per-beach
-- exception matches, verdict = exception's rule, conf = 1.0, math
-- skipped — same shape as cpad_manual. Removes the case where three
-- weak upstream signals could outvote a primary-source quote naming
-- the specific beach. Precedence: cpad_manual > operator_exception
-- > weighted vote.
--
-- (#2) Source-dependence demotion. When cpad_web or ccc_llm share a
-- hostname with operator_dogs_policy.source_url, they're likely two
-- LLM reads of the same agency page (not independent evidence). Halve
-- those weights in the vote so we don't double-count. Only applies in
-- the weighted-vote branch (overrides skip the math anyway).

create or replace function public.compute_dogs_verdict(p_objectid integer)
returns void
language plpgsql security definer as $$
declare
  v_pt          geometry;
  v_name        text;
  v_native      text;
  v_native_wt   constant numeric := 0.66;

  v_cpad_val    text;
  v_cpad_wt     numeric;
  v_cpad_url    text;
  v_cpad_kind   text;

  v_ccc_val     text;
  v_ccc_wt      numeric;
  v_ccc_url     text;

  v_op_id       bigint;
  v_op_val      text;
  v_op_wt       numeric;
  v_op_tag      text;
  v_op_src_url  text;
  v_op_host     text;

  v_yes_wt      numeric := 0;
  v_no_wt       numeric := 0;
  v_sources     text[]  := array[]::text[];
  v_verdict     text;
  v_conf        numeric;
  v_margin      numeric;
  v_review      boolean := false;
  v_meta        jsonb;
begin
  select c.geom, c.name,
         case lower(trim(c.dog_friendly))
           when 'yes' then 'yes' when 'no'  then 'no' else null
         end,
         c.operator_id
    into v_pt, v_name, v_native, v_op_id
    from public.ccc_access_points c
   where c.objectid = p_objectid;

  if v_pt is null then return; end if;

  if v_native is not null then
    if v_native = 'yes' then v_yes_wt := v_yes_wt + v_native_wt;
    else                     v_no_wt  := v_no_wt  + v_native_wt;
    end if;
    v_sources := array_append(v_sources, 'ccc_native');
  end if;

  select
    case
      when r.response_value in ('yes','restricted','seasonal') then 'yes'
      when r.response_value = 'no' then 'no'
      else null
    end,
    r.response_confidence, r.source_url
    into v_cpad_val, v_cpad_wt, v_cpad_url
    from public.cpad_units cu
    join public.geo_entity_response_current r
      on r.entity_type = 'cpad' and r.entity_id = cu.unit_id
     and r.response_scope = 'dogs_allowed'
   where st_contains(cu.geom, v_pt::geometry)
   order by
     (cu.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
     st_area(cu.geom) asc
   limit 1;

  v_cpad_kind := case
    when v_cpad_url is null              then null
    when v_cpad_url like 'admin://%'     then 'cpad_manual'
    when v_cpad_url like 'llm-prior://%' then 'cpad_llm'
    else 'cpad_web'
  end;

  select
    case
      when response_value in ('yes','restricted','seasonal') then 'yes'
      when response_value = 'no' then 'no'
      else null
    end,
    response_confidence, source_url
    into v_ccc_val, v_ccc_wt, v_ccc_url
    from public.geo_entity_response_current
   where entity_type = 'ccc'
     and entity_id   = p_objectid
     and response_scope = 'dogs_allowed';

  -- Operator policy: exception (per-beach) or default ─────────────────
  if v_op_id is not null and v_name is not null then
    select source_url into v_op_src_url
      from public.operator_dogs_policy
     where operator_id = v_op_id;
    v_op_host := lower(regexp_replace(coalesce(v_op_src_url,''),
                                      '^https?://([^/]+).*$', '\1'));

    select
      case
        when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
        when e->>'rule' in ('prohibited','no')                         then 'no'
        else null
      end
      into v_op_val
      from public.operator_dogs_policy odp,
           jsonb_array_elements(odp.exceptions) e
     where odp.operator_id = v_op_id
       and e->>'rule' is not null
       and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and (
         similarity(public.clean_beach_name(v_name),
                    public.clean_beach_name(e->>'beach_name')) >= 0.65
         or public.clean_beach_name(v_name)
            like '%' || public.clean_beach_name(e->>'beach_name') || '%'
         or public.clean_beach_name(e->>'beach_name')
            like '%' || public.clean_beach_name(v_name) || '%'
       )
     order by
       (public.clean_beach_name(v_name)
         = public.clean_beach_name(e->>'beach_name'))::int desc,
       (public.clean_beach_name(v_name)
         like '%' || public.clean_beach_name(e->>'beach_name'))::int desc,
       (public.clean_beach_name(v_name)
         like public.clean_beach_name(e->>'beach_name') || '%')::int desc,
       similarity(public.clean_beach_name(v_name),
                  public.clean_beach_name(e->>'beach_name')) desc
     limit 1;

    if v_op_val is not null then
      v_op_wt  := 1.0;
      v_op_tag := 'operator_exception';
    else
      select
        case
          when default_rule in ('yes','restricted') then 'yes'
          when default_rule = 'no' then 'no'
          else null
        end
        into v_op_val
        from public.operator_dogs_policy
       where operator_id = v_op_id;
      v_op_wt  := 0.4;
      v_op_tag := 'operator_default';
    end if;
  end if;

  -- ── (#1) Hard overrides skip the math ────────────────────────────
  if v_cpad_kind = 'cpad_manual' and v_cpad_val is not null then
    v_verdict := v_cpad_val;
    v_conf    := coalesce(v_cpad_wt, 1.0);
    v_sources := array['cpad_manual'];
    v_meta := jsonb_build_object(
      'override','manual','sources',to_jsonb(v_sources),
      'review',false,'computed_at',now());

  elsif v_op_tag = 'operator_exception' and v_op_val is not null then
    v_verdict := v_op_val;
    v_conf    := 1.0;
    v_sources := array['operator_exception'];
    v_meta := jsonb_build_object(
      'override','operator_exception','sources',to_jsonb(v_sources),
      'review',false,'computed_at',now());

  else
    -- ── (#2) Source-dependence demotion ──────────────────────────
    -- Halve cpad_web / ccc_llm weight when their hostname matches the
    -- operator's policy source hostname (same agency page = correlated
    -- evidence, not independent).
    if v_cpad_kind = 'cpad_web' and v_cpad_val is not null
       and v_op_host <> '' and v_op_host is not null
       and lower(regexp_replace(coalesce(v_cpad_url,''),
                                '^https?://([^/]+).*$', '\1')) = v_op_host then
      v_cpad_wt := v_cpad_wt * 0.5;
    end if;

    if v_ccc_val is not null
       and v_op_host <> '' and v_op_host is not null
       and lower(regexp_replace(coalesce(v_ccc_url,''),
                                '^https?://([^/]+).*$', '\1')) = v_op_host then
      v_ccc_wt := v_ccc_wt * 0.5;
    end if;

    if v_cpad_val is not null then
      if v_cpad_val = 'yes' then v_yes_wt := v_yes_wt + v_cpad_wt;
      else                       v_no_wt  := v_no_wt  + v_cpad_wt;
      end if;
      v_sources := array_append(v_sources, v_cpad_kind);
    end if;

    if v_ccc_val is not null then
      if v_ccc_val = 'yes' then v_yes_wt := v_yes_wt + v_ccc_wt;
      else                      v_no_wt  := v_no_wt  + v_ccc_wt;
      end if;
      v_sources := array_append(v_sources, 'ccc_llm');
    end if;

    if v_op_val is not null then
      if v_op_val = 'yes' then v_yes_wt := v_yes_wt + v_op_wt;
      else                     v_no_wt  := v_no_wt  + v_op_wt;
      end if;
      v_sources := array_append(v_sources, v_op_tag);
    end if;

    if array_length(v_sources, 1) is null then
      v_verdict := null; v_conf := null; v_margin := null;
    else
      if v_yes_wt > v_no_wt then
        v_verdict := 'yes';
        v_conf    := v_yes_wt / (v_yes_wt + v_no_wt);
        v_margin  := v_yes_wt - v_no_wt;
      elsif v_no_wt > v_yes_wt then
        v_verdict := 'no';
        v_conf    := v_no_wt  / (v_yes_wt + v_no_wt);
        v_margin  := v_no_wt - v_yes_wt;
      else
        v_verdict := 'no';
        v_conf    := 0.5;
        v_margin  := 0;
      end if;
      v_review := (v_yes_wt > 0 and v_no_wt > 0 and v_margin < 0.10);
    end if;

    v_meta := jsonb_build_object(
      'yes_weight',  round(v_yes_wt, 4),
      'no_weight',   round(v_no_wt,  4),
      'margin',      case when v_margin is not null then round(v_margin, 4) end,
      'sources',     to_jsonb(v_sources),
      'review',      v_review,
      'computed_at', now());
  end if;

  update public.ccc_access_points
     set dogs_verdict            = v_verdict,
         dogs_verdict_confidence = case when v_conf is not null then round(v_conf, 4) end,
         dogs_verdict_meta       = v_meta
   where objectid = p_objectid;
end;
$$;
