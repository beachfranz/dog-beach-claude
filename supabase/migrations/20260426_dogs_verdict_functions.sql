-- Weighted-vote consensus across three signals for one CCC objectid:
--   1. CCC-native (ccc_access_points.dog_friendly: yes/no)  weight 0.66
--   2. CPAD       (geo_entity_response, entity_type='cpad') weight = stored response_confidence
--   3. CCC-LLM    (geo_entity_response, entity_type='ccc')  weight = stored response_confidence
--
-- Rollup before voting: yes/restricted/seasonal -> 'yes'; no -> 'no';
-- unknown/null/blank -> ignored. CCC-native is already binary.
--
-- Manual overrides ('admin://') skip the math entirely — verdict is
-- whatever the admin row says, confidence 1.00.
--
-- Margin guard: if both sides have weight and |yes_wt - no_wt| < 0.10,
-- mark review=true and default to the most-restrictive ('no').
--
-- If a CCC point falls inside multiple CPAD polygons (overlap), use
-- the smallest-area one — Tier 1 ranking, most-specific wins.

create or replace function public.compute_dogs_verdict(p_objectid int)
returns void
language plpgsql
security definer
as $$
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
           when 'yes' then 'yes'
           when 'no'  then 'no'
           else null
         end
    into v_pt, v_native
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

  -- ── CPAD signal (smallest-area containing polygon) ────────────────
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

  -- ── Manual hard-override (skip math) ──────────────────────────────
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
    -- Aggregate CPAD into the vote
    if v_cpad_val is not null then
      if v_cpad_val = 'yes' then v_yes_wt := v_yes_wt + v_cpad_wt;
      else                       v_no_wt  := v_no_wt  + v_cpad_wt;
      end if;
      v_sources := array_append(v_sources, v_cpad_kind);
    end if;

    -- Aggregate CCC-LLM into the vote
    if v_ccc_val is not null then
      if v_ccc_val = 'yes' then v_yes_wt := v_yes_wt + v_ccc_wt;
      else                      v_no_wt  := v_no_wt  + v_ccc_wt;
      end if;
      v_sources := array_append(v_sources, 'ccc_llm');
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
        -- exact tie -> default to 'no' (more restrictive)
        v_verdict := 'no';
        v_conf    := 0.5;
        v_margin  := 0;
      end if;
      -- Margin guard fires only when both sides actually voted
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

-- Sweep all CCC sandy+named-beach points (the same universe the
-- LLM-knowledge pass and the map filter use). Returns the count
-- recomputed.
create or replace function public.recompute_all_dogs_verdicts()
returns int
language plpgsql
security definer
as $$
declare
  v_count int := 0;
  r       record;
begin
  for r in
    select objectid
      from public.ccc_access_points
     where (archived is null or archived <> 'Yes')
       and latitude is not null and longitude is not null
       and sandy_beach = 'Yes'
       and name ilike '%beach%'
  loop
    perform public.compute_dogs_verdict(r.objectid);
    v_count := v_count + 1;
  end loop;
  return v_count;
end;
$$;
