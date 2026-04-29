-- Pass 8 — re-key the verdict pipeline off CCC. (cleanup #2)
--
-- Until now the cascade was keyed on `ccc_access_points.objectid` and
-- stored verdict on `ccc_access_points.dogs_verdict`. Both are CCC
-- artifacts, even though the cascade itself stopped reading CCC policy
-- data in Pass 7. This pass moves the cascade to a beach-level identity
-- (origin_key) and a CCC-independent storage table.
--
-- Three pieces:
--
--   1. Pure cascade core: compute_dogs_verdict_core(geom, name, op_id)
--      returns (verdict, confidence, meta). No table writes; CCC-free.
--
--   2. Entry point: compute_dogs_verdict_by_origin(origin_key) parses
--      'ccc/<id>' / 'ubp/<id>' / 'osm/<type>/<id>', resolves to source
--      row for geom+name+operator_id, calls core, writes to a new
--      table public.beach_verdicts(origin_key text PK, ...).
--
--   3. Batch: recompute_all_dogs_verdicts_by_origin() walks the union
--      of beach_locations origin keys, OSM beach polygons, and active
--      CCC access points. Calls by_origin per row.
--
-- The OSM branch of all_coastal_features_lite is rewired in a sibling
-- migration to read dogs_verdict from beach_verdicts directly,
-- eliminating the CCC-association lookup.
--
-- The CCC-keyed compute_dogs_verdict(integer) entry remains as a
-- legacy wrapper (becomes a thin shim calling by_origin('ccc/<id>'))
-- so anything still calling it produces consistent output.
--
-- The ccc-friendly fork (compute_dogs_verdict_ccc_friendly) is
-- untouched — it remains keyed on ccc_access_points and writes to
-- dogs_verdict_ccc_friendly columns.

begin;

-- ── 1. beach_verdicts table ────────────────────────────────────────
create table if not exists public.beach_verdicts (
  origin_key              text primary key,
  dogs_verdict            text,
  dogs_verdict_confidence numeric,
  dogs_verdict_meta       jsonb,
  computed_at             timestamptz not null default now()
);

create index if not exists beach_verdicts_verdict_idx on public.beach_verdicts (dogs_verdict);

grant select on public.beach_verdicts to anon, authenticated;

-- ── 2. Pure cascade core (CCC-free; identical logic to Pass 7) ────
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
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_cu_val
      from public.cpad_unit_dogs_policy p, jsonb_array_elements(coalesce(p.exceptions,'[]'::jsonb)) e
     where p.cpad_unit_id = v_unit_id and p_name is not null
       and e->>'beach_name' is not null and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and ( similarity(public.clean_beach_name(p_name), public.clean_beach_name(e->>'beach_name')) >= 0.65
          or public.clean_beach_name(p_name) like '%' || public.clean_beach_name(e->>'beach_name') || '%'
          or public.clean_beach_name(e->>'beach_name') like '%' || public.clean_beach_name(p_name) || '%' )
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
    select case when e->>'rule' in ('off_leash','allowed','yes','restricted') then 'yes'
                when e->>'rule' in ('prohibited','no')                         then 'no' else null end
      into v_op_val
      from public.operator_dogs_policy odp, jsonb_array_elements(odp.exceptions) e
     where odp.operator_id = p_operator_id and e->>'rule' is not null
       and length(e->>'beach_name') >= 6
       and lower(e->>'beach_name') not in ('the beach','beach','city beach','city beaches')
       and ( similarity(public.clean_beach_name(p_name), public.clean_beach_name(e->>'beach_name')) >= 0.65
          or public.clean_beach_name(p_name) like '%' || public.clean_beach_name(e->>'beach_name') || '%'
          or public.clean_beach_name(e->>'beach_name') like '%' || public.clean_beach_name(p_name) || '%' )
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

-- ── 3. Origin-keyed entry point ───────────────────────────────────
create or replace function public.compute_dogs_verdict_by_origin(p_origin_key text)
returns void language plpgsql security definer as $$
declare
  v_source text;
  v_geom   geometry;
  v_name   text;
  v_op_id  bigint;
  v_id_str text;
  v_result record;
begin
  v_source := split_part(p_origin_key, '/', 1);

  if v_source = 'ccc' then
    v_id_str := split_part(p_origin_key, '/', 2);
    select c.geom, c.name, c.operator_id into v_geom, v_name, v_op_id
      from public.ccc_access_points c
     where c.objectid = nullif(v_id_str,'')::int;

  elsif v_source = 'ubp' then
    v_id_str := split_part(p_origin_key, '/', 2);
    select u.geom, u.name, u.operator_id into v_geom, v_name, v_op_id
      from public.us_beach_points u
     where u.fid = nullif(v_id_str,'')::int;

  elsif v_source = 'osm' then
    -- origin_key shape: 'osm/<type>/<id>'
    select st_centroid(o.geom)::geometry, o.name, o.operator_id
      into v_geom, v_name, v_op_id
      from public.osm_features o
     where o.osm_type = split_part(p_origin_key, '/', 2)
       and o.osm_id   = nullif(split_part(p_origin_key, '/', 3),'')::bigint;

  else
    return;  -- unknown origin source
  end if;

  if v_geom is null then return; end if;

  select * into v_result from public.compute_dogs_verdict_core(v_geom, v_name, v_op_id);

  insert into public.beach_verdicts (origin_key, dogs_verdict, dogs_verdict_confidence, dogs_verdict_meta, computed_at)
  values (p_origin_key, v_result.verdict, v_result.confidence, v_result.meta, now())
  on conflict (origin_key) do update
    set dogs_verdict            = excluded.dogs_verdict,
        dogs_verdict_confidence = excluded.dogs_verdict_confidence,
        dogs_verdict_meta       = excluded.dogs_verdict_meta,
        computed_at             = now();
end;
$$;

-- ── 4. Batch driver ───────────────────────────────────────────────
create or replace function public.recompute_all_dogs_verdicts_by_origin()
returns integer language plpgsql security definer as $$
declare
  n integer := 0;
  rec record;
begin
  for rec in (
    select bl.origin_key from public.beach_locations bl
    union
    select 'osm/' || o.osm_type || '/' || o.osm_id::text
      from public.osm_features o
     where o.feature_type in ('beach','dog_friendly_beach')
       and (o.admin_inactive is null or o.admin_inactive = false)
    union
    select 'ccc/' || c.objectid::text
      from public.ccc_access_points c
     where (c.archived is null or c.archived <> 'Yes')
       and (c.admin_inactive is null or c.admin_inactive = false)
       and c.latitude is not null
  ) loop
    perform public.compute_dogs_verdict_by_origin(rec.origin_key);
    n := n + 1;
  end loop;
  return n;
end;
$$;

-- ── 5. Backward-compat shim: old CCC-keyed entry maps to by_origin ─
create or replace function public.compute_dogs_verdict(p_objectid integer)
returns void language plpgsql security definer as $$
declare
  v_result record;
begin
  perform public.compute_dogs_verdict_by_origin('ccc/' || p_objectid::text);

  -- Also write to ccc_access_points.dogs_verdict for any legacy callers
  -- still reading the column directly. Will be removed once all
  -- downstream readers move to beach_verdicts.
  select dogs_verdict, dogs_verdict_confidence, dogs_verdict_meta
    into v_result
    from public.beach_verdicts
   where origin_key = 'ccc/' || p_objectid::text;

  update public.ccc_access_points
     set dogs_verdict            = v_result.dogs_verdict,
         dogs_verdict_confidence = v_result.dogs_verdict_confidence,
         dogs_verdict_meta       = v_result.dogs_verdict_meta
   where objectid = p_objectid;
end;
$$;

commit;
