-- Pass 6 — fix CPAD unit lookup + wire cpad_unit_dogs_policy.
--
-- Two issues addressed:
--
-- A. Lookup bug: compute_dogs_verdict (and the cpad_unit_for_beach
--    precompute) picked the smallest containing CPAD polygon. That ignores
--    name signal — a "Davenport Landing Picnic Area" sub-unit beats
--    "Davenport Landing Beach" just because it's smaller. Switch to the
--    full Tier 1 ranking already used by _rank_park_url_evidence:
--      1. demote environmental overlays
--      2. trigram similarity to beach name (closest match wins)
--      3. "Beach" in unit name preferred (catches generic-name patterns)
--      4. smallest area (final tiebreak only)
--
-- B. cpad_unit_dogs_policy not in cascade: 226 unit-level extractions
--    (117 classified) are unused by compute_dogs_verdict. Add two tiers:
--      cpad_unit_exception — beach-name match in unit's exceptions[]:
--        weight 1.0 (per-beach quote, on par with operator_exception)
--      cpad_unit_default   — unit's default_rule:
--        weight 0.7 (more specific than operator_default 0.4, less than
--        per-beach exception)
--    When cpad_unit_dogs_policy has the unit, skip the legacy
--    geo_entity_response_current 'cpad' signal — same grain, newer wins.

begin;

-- ── A. Rebuild cpad_unit_for_beach with name-aware ranking ─────────
truncate public.cpad_unit_for_beach;

with universe as (
  select bl.origin_key, bl.name, bl.geom from public.beach_locations bl
  union
  select 'ccc/' || c.objectid::text as origin_key, c.name, c.geom
    from public.ccc_access_points c
   where (c.archived is null or c.archived <> 'Yes')
     and (c.admin_inactive is null or c.admin_inactive = false)
     and c.latitude is not null
     and c.sandy_beach = 'Yes'
     and c.inferred_type = 'beach'
     and 'ccc/' || c.objectid::text not in (select origin_key from public.beach_locations)
)
insert into public.cpad_unit_for_beach (origin_key, beach_name, beach_county, lat, lng, unit_id, unit_area_m2)
  select u.origin_key,
         u.name,
         (select c.name from public.counties c where st_intersects(c.geom, u.geom) limit 1),
         st_y(u.geom)::float8,
         st_x(u.geom)::float8,
         cu.unit_id,
         cu.area_m2
    from universe u
    left join lateral (
      select cu2.unit_id, st_area(cu2.geom::geography) as area_m2
        from public.cpad_units cu2
       where st_contains(cu2.geom, u.geom)
       order by
         (cu2.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
         similarity(public.clean_beach_name(coalesce(u.name, '')),
                    public.clean_beach_name(coalesce(cu2.unit_name, ''))) desc,
         (cu2.unit_name ~* '\mbeach\M')::int desc,
         st_area(cu2.geom) asc
       limit 1
    ) cu on true;

-- ── B. Rewrite compute_dogs_verdict with cpad_unit tier + name match ──
create or replace function public.compute_dogs_verdict(p_objectid integer)
returns void
language plpgsql security definer as $$
declare
  v_pt          geometry;
  v_name        text;
  v_native      text;
  v_native_wt   constant numeric := 0.66;

  v_unit_id     integer;       -- the matched containing CPAD unit
  v_unit_name   text;

  v_cpad_val    text;
  v_cpad_wt     numeric;
  v_cpad_url    text;
  v_cpad_kind   text;

  v_cu_val      text;          -- cpad_unit_dogs_policy verdict
  v_cu_wt       numeric;
  v_cu_tag      text;

  v_ccc_val     text;
  v_ccc_wt      numeric;
  v_ccc_url     text;

  v_op_id       bigint;
  v_op_val      text;
  v_op_wt       numeric;
  v_op_tag      text;

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

  -- Pick the best containing CPAD unit ONCE, with full Tier 1 ranking.
  -- This unit_id then drives both cpad_unit_dogs_policy lookup AND the
  -- legacy geo_entity_response_current fallback.
  select cu.unit_id, cu.unit_name
    into v_unit_id, v_unit_name
    from public.cpad_units cu
   where st_contains(cu.geom, v_pt::geometry)
   order by
     (cu.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
     similarity(public.clean_beach_name(coalesce(v_name, '')),
                public.clean_beach_name(coalesce(cu.unit_name, ''))) desc,
     (cu.unit_name ~* '\mbeach\M')::int desc,
     st_area(cu.geom) asc
   limit 1;

  -- 1) Try cpad_unit_dogs_policy first (newer extraction, same grain).
  if v_unit_id is not null then
    -- 1a. Exception match: any exceptions[] entry whose beach_name fuzzy-
    --     matches this CCC point's name.
    select
      case
        when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
        when e->>'rule' in ('prohibited','no')                         then 'no'
        else null
      end
      into v_cu_val
      from public.cpad_unit_dogs_policy p,
           jsonb_array_elements(coalesce(p.exceptions, '[]'::jsonb)) e
     where p.cpad_unit_id = v_unit_id
       and v_name is not null
       and e->>'beach_name' is not null
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
       similarity(public.clean_beach_name(v_name),
                  public.clean_beach_name(e->>'beach_name')) desc
     limit 1;

    if v_cu_val is not null then
      v_cu_wt  := 1.0;
      v_cu_tag := 'cpad_unit_exception';
    else
      -- 1b. Default rule for the unit.
      select
        case
          when default_rule in ('yes','restricted') then 'yes'
          when default_rule = 'no' then 'no'
          else null
        end
        into v_cu_val
        from public.cpad_unit_dogs_policy
       where cpad_unit_id = v_unit_id;
      if v_cu_val is not null then
        v_cu_wt  := 0.7;
        v_cu_tag := 'cpad_unit_default';
      end if;
    end if;
  end if;

  -- 2) Legacy geo_entity_response_current CPAD signal — used ONLY when
  --    cpad_unit_dogs_policy didn't yield a value (avoid double-counting
  --    the same unit-grain evidence from two extractions).
  if v_cu_val is null and v_unit_id is not null then
    select
      case
        when r.response_value in ('yes','restricted','seasonal') then 'yes'
        when r.response_value = 'no' then 'no'
        else null
      end,
      r.response_confidence, r.source_url
      into v_cpad_val, v_cpad_wt, v_cpad_url
      from public.geo_entity_response_current r
     where r.entity_type = 'cpad'
       and r.entity_id = v_unit_id
       and r.response_scope = 'dogs_allowed';

    v_cpad_kind := case
      when v_cpad_url is null              then null
      when v_cpad_url like 'admin://%'     then 'cpad_manual'
      when v_cpad_url like 'llm-prior://%' then 'cpad_llm'
      else 'cpad_web'
    end;
  end if;

  -- 3) CCC-LLM (per-point response).
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

  -- 4) Operator (exception → default).
  if v_op_id is not null and v_name is not null then
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

  -- 5) Hard override: cpad_manual (admin URL) short-circuits everything.
  if v_cpad_kind = 'cpad_manual' and v_cpad_val is not null then
    v_verdict := v_cpad_val;
    v_conf    := coalesce(v_cpad_wt, 1.0);
    v_margin  := null;
    v_review  := false;
    v_sources := array['cpad_manual'];
    v_meta := jsonb_build_object(
      'override','manual','sources',to_jsonb(v_sources),
      'cpad_unit_id', v_unit_id,
      'review',v_review,'computed_at',now());
  else
    -- 6) Weighted vote.
    if v_cu_val is not null then
      if v_cu_val = 'yes' then v_yes_wt := v_yes_wt + v_cu_wt;
      else                     v_no_wt  := v_no_wt  + v_cu_wt;
      end if;
      v_sources := array_append(v_sources, v_cu_tag);
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
      'yes_weight',   round(v_yes_wt, 4),
      'no_weight',    round(v_no_wt,  4),
      'margin',       case when v_margin is not null then round(v_margin, 4) end,
      'sources',      to_jsonb(v_sources),
      'cpad_unit_id', v_unit_id,
      'review',       v_review,
      'computed_at',  now());
  end if;

  update public.ccc_access_points
     set dogs_verdict            = v_verdict,
         dogs_verdict_confidence = case when v_conf is not null then round(v_conf, 4) end,
         dogs_verdict_meta       = v_meta
   where objectid = p_objectid;
end;
$$;

commit;
