-- Add Pass 4 to compute_dogs_verdict and the lite RPC's verdict
-- aggregation: operator_dogs_policy.default_rule as a fallback signal
-- when per-beach CCC native / CPAD / CCC-LLM are silent.
--
-- Per Franz's "if dogs can be there at all → yes" framing:
--   policy_default in ('yes','restricted') contributes to yes_weight
--   policy_default = 'no' contributes to no_weight
-- Weight 0.4 — lower than per-beach CCC native (0.66) since operator
-- policy is generic, not per-beach specific.

-- ── compute_dogs_verdict: add Pass 4 ────────────────────────────────
create or replace function public.compute_dogs_verdict(p_objectid integer)
returns void
language plpgsql security definer as $$
declare
  v_pt          geometry;
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
  v_op_wt       constant numeric := 0.4;

  v_yes_wt      numeric := 0;
  v_no_wt       numeric := 0;
  v_sources     text[]  := array[]::text[];
  v_verdict     text;
  v_conf        numeric;
  v_margin      numeric;
  v_review      boolean := false;
  v_meta        jsonb;
begin
  select c.geom,
         case lower(trim(c.dog_friendly))
           when 'yes' then 'yes' when 'no'  then 'no' else null
         end,
         c.operator_id
    into v_pt, v_native, v_op_id
    from public.ccc_access_points c
   where c.objectid = p_objectid;

  if v_pt is null then return; end if;

  -- ── CCC native ────────────────────────────────────────────────────
  if v_native is not null then
    if v_native = 'yes' then v_yes_wt := v_yes_wt + v_native_wt;
    else                     v_no_wt  := v_no_wt  + v_native_wt;
    end if;
    v_sources := array_append(v_sources, 'ccc_native');
  end if;

  -- ── CPAD signal ───────────────────────────────────────────────────
  select
    case
      when r.response_value in ('yes','restricted','seasonal') then 'yes'
      when r.response_value = 'no' then 'no'
      else null
    end,
    r.response_confidence,
    r.source_url
    into v_cpad_val, v_cpad_wt, v_cpad_url
    from public.cpad_units cu
    join public.geo_entity_response_current r
      on r.entity_type   = 'cpad'
     and r.entity_id     = cu.unit_id
     and r.response_scope= 'dogs_allowed'
   where st_contains(cu.geom, v_pt::geometry)
   order by st_area(cu.geom) asc
   limit 1;

  v_cpad_kind := case
    when v_cpad_url is null              then null
    when v_cpad_url like 'admin://%'     then 'cpad_manual'
    when v_cpad_url like 'llm-prior://%' then 'cpad_llm'
    else 'cpad_web'
  end;

  -- ── CCC-LLM signal ────────────────────────────────────────────────
  select
    case
      when response_value in ('yes','restricted','seasonal') then 'yes'
      when response_value = 'no' then 'no'
      else null
    end,
    response_confidence,
    source_url
    into v_ccc_val, v_ccc_wt, v_ccc_url
    from public.geo_entity_response_current
   where entity_type   = 'ccc'
     and entity_id     = p_objectid
     and response_scope= 'dogs_allowed';

  -- ── Operator default policy (NEW Pass 4) ──────────────────────────
  -- Only consulted if the CCC has an operator_id. Conservative weight.
  if v_op_id is not null then
    select
      case
        when default_rule in ('yes','restricted') then 'yes'
        when default_rule = 'no' then 'no'
        else null
      end
      into v_op_val
      from public.operator_dogs_policy
     where operator_id = v_op_id;
  end if;

  -- ── Manual hard-override (CPAD admin) ─────────────────────────────
  if v_cpad_kind = 'cpad_manual' and v_cpad_val is not null then
    v_verdict := v_cpad_val;
    v_conf    := coalesce(v_cpad_wt, 1.0);
    v_margin  := null;
    v_review  := false;
    v_sources := array['cpad_manual'];
    v_meta := jsonb_build_object(
      'override',     'manual',
      'sources',      to_jsonb(v_sources),
      'review',       v_review,
      'computed_at',  now()
    );
  else
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
      v_sources := array_append(v_sources, 'operator_default');
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
      'computed_at', now()
    );
  end if;

  update public.ccc_access_points
     set dogs_verdict            = v_verdict,
         dogs_verdict_confidence = case when v_conf is not null then round(v_conf, 4) end,
         dogs_verdict_meta       = v_meta
   where objectid = p_objectid;
end;
$$;


-- ── all_coastal_features_lite: add operator_default fallback ────────
-- For OSM/UBP rows whose associated-CCC verdict is null, fall back to
-- their operator's default_rule (mapped restricted→yes).
drop function if exists public.all_coastal_features_lite(text[]);
create or replace function public.all_coastal_features_lite(
  p_counties text[] default null
)
returns table (
  layer              text,
  origin_key         text,
  name               text,
  feature_type       text,
  origin_source      text,
  operator_canonical text,
  dogs_verdict       text,
  description        text,
  lat                float8,
  lng                float8
)
language sql stable security definer
as $$
  -- OSM beach polygons. Verdict: associated CCC same_beach majority,
  -- else operator policy default.
  select 'beach'::text,
         'osm/' || o.osm_type || '/' || o.osm_id::text,
         o.name, o.feature_type, 'osm'::text,
         op.canonical_name,
         coalesce(
           (select case
              when bool_or(c.dogs_verdict='yes') then 'yes'
              when bool_and(c.dogs_verdict='no')  then 'no'
              else null end
            from public.feature_associations fa
            join public.ccc_access_points c on c.objectid::text = fa.a_id
            where fa.a_source='ccc' and fa.b_source='osm'
              and fa.relationship='same_beach'
              and fa.b_id = o.osm_type || '/' || o.osm_id::text
              and c.dogs_verdict in ('yes','no')),
           (select case
              when odp.default_rule in ('yes','restricted') then 'yes'
              when odp.default_rule = 'no' then 'no'
              else null end
            from public.operator_dogs_policy odp
            where odp.operator_id = o.operator_id)
         ) as dogs_verdict,
         null::text,
         st_y(o.geom), st_x(o.geom)
  from public.osm_features o
  left join public.operators op on op.id = o.operator_id
  where o.feature_type in ('beach','dog_friendly_beach')
    and (o.admin_inactive is null or o.admin_inactive = false)
    and (p_counties is null or o.county_name_tiger = any(p_counties))

  union all

  -- UBP-CA. Verdict: associated CCC, else operator policy default.
  select 'beach',
         'ubp/' || u.fid::text,
         u.name, 'beach', 'ubp',
         op.canonical_name,
         coalesce(
           (select case
              when bool_or(c.dogs_verdict='yes') then 'yes'
              when bool_and(c.dogs_verdict='no')  then 'no'
              else null end
            from public.feature_associations fa
            join public.ccc_access_points c on c.objectid::text = fa.b_id
            where fa.a_source='ubp' and fa.b_source='ccc'
              and fa.relationship='same_beach'
              and fa.a_id = u.fid::text
              and c.dogs_verdict in ('yes','no')),
           (select case
              when odp.default_rule in ('yes','restricted') then 'yes'
              when odp.default_rule = 'no' then 'no'
              else null end
            from public.operator_dogs_policy odp
            where odp.operator_id = u.operator_id)
         ) as dogs_verdict,
         null::text,
         st_y(u.geom), st_x(u.geom)
  from public.us_beach_points u
  left join public.operators op on op.id = u.operator_id
  where u.state = 'CA'
    and (u.admin_inactive is null or u.admin_inactive = false)
    and (p_counties is null or u.county_name_tiger = any(p_counties))

  union all

  -- CCC: own dogs_verdict (already includes operator_default Pass 4)
  select case when coalesce(c.inferred_type, '') in ('beach','named_beach') then 'beach'
              else 'access' end,
         'ccc/' || c.objectid::text,
         c.name,
         coalesce(c.inferred_type, 'unknown'),
         'ccc',
         op.canonical_name,
         c.dogs_verdict,
         c.description,
         st_y(c.geom), st_x(c.geom)
  from public.ccc_access_points c
  left join public.operators op on op.id = c.operator_id
  where (c.archived is null or c.archived <> 'Yes')
    and (c.admin_inactive is null or c.admin_inactive = false)
    and c.latitude is not null
    and (p_counties is null or c.county_name_tiger = any(p_counties));
$$;

grant execute on function public.all_coastal_features_lite(text[]) to anon, authenticated;
