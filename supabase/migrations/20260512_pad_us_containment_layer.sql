-- PAD-US as a multi-state polygon_containment layer.
--
-- Adds the missing protected-areas layer to the polygon_containment
-- pipeline for all 50 states. CPAD covers CA only (it's the California
-- Protected Areas Database by definition); PAD-US is the federal
-- aggregator that covers every state with the same schema convention.
--
-- Architecture mirrors populate_cpad_containment_gold:
--   - Find PAD-US polygons containing (or within p_buffer_m of) the beach
--   - Rank by env_overlay / has_beach_word / name_token_overlap / area
--   - Pick best per beach, write polygon_containment row
--   - polygon_kind = 'pad_us_unit', source = 'pad_us'
--
-- Also extends populate_governance_from_polygon_gold to derive
-- governance from the new pad_us_unit polygon_kind. Mirrors the
-- existing city/county derivation pattern.
--
-- 100m default buffer captures coastal beaches just seaward of park
-- polygon boundaries (Oregon Beach Bill / wet-sand strip cases).

begin;

-- ─── 1. New PAD-US containment populator ──────────────────────────────
create or replace function public.populate_pad_us_containment_gold(
  p_fid bigint default null,
  p_buffer_m int default 100
)
returns integer
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, pu.unit_id, pu.unit_name, pu.own_type, pu.own_name,
           pu.mng_type, pu.mng_name, pu.des_tp, pu.category,
           st_area(pu.geom::geography) as area_m2,
           cardinality(public.shared_name_tokens(
             coalesce(g.display_name_override, g.name), pu.unit_name)) as name_token_overlap,
           pu.unit_name ~* '\mbeach\M' as has_beach_word,
           pu.unit_name ~* '(wilderness|wsa|acec|wsr|ira|nca)'
             as is_env_overlay,
           pu.raw_attrs
      from public.beaches_gold g
      join public.pad_us_units pu
        on (case
              when p_buffer_m = 0 then st_intersects(pu.geom, g.geom::geometry)
              else st_dwithin(pu.geom::geography,
                              g.geom::geography, p_buffer_m)
            end)
       and pu.state = g.state
     where (p_fid is null or g.fid = p_fid)
       and g.geom is not null
       and g.is_active
  ),
  ranked as (
    select *,
      row_number() over (
        partition by fid
        order by
          is_env_overlay     asc,   -- demote wilderness designations
          has_beach_word     desc,
          name_token_overlap desc,
          area_m2            asc,   -- smaller polygon wins (more specific)
          unit_id            asc
      ) as rnk
      from candidates
  ),
  best as (select * from ranked where rnk = 1)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, updated_at)
  select fid, 'polygon_containment', 'pad_us',
    case
      when has_beach_word and name_token_overlap >= 1 then 0.95
      when has_beach_word                              then 0.85
      when name_token_overlap >= 1                     then 0.80
      else                                                  0.65
    end,
    jsonb_build_object(
      'polygon_kind',       'pad_us_unit',
      'polygon_id',         unit_id,
      'polygon_name',       unit_name,
      'own_type',           own_type,
      'own_name',           own_name,
      'mng_type',           mng_type,
      'mng_name',           mng_name,
      'des_tp',             des_tp,
      'category',           category,
      'pub_access',         raw_attrs->>'Pub_Access',
      'date_est',           raw_attrs->>'Date_Est',
      'gis_acres',          raw_attrs->>'GIS_Acres',
      'loc_ds',             raw_attrs->>'Loc_Ds',
      'is_env_overlay',     is_env_overlay,
      'has_beach_word',     has_beach_word,
      'name_token_overlap', name_token_overlap,
      'area_m2',            area_m2
    ),
    now()
  from best
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update
    set confidence     = excluded.confidence,
        claimed_values = excluded.claimed_values,
        updated_at     = now(),
        is_canonical   = false;

  get diagnostics rows_touched = row_count;
  return rows_touched;
end;
$$;


-- ─── 2. Wire into the orchestrator ────────────────────────────────────
create or replace function public.populate_polygon_containment_gold(p_fid bigint default null)
returns table(emitted integer, resolved integer, promoted integer)
language plpgsql
set statement_timeout to '600s'
as $$
declare
  e_cpad int; e_pad_us int; e_juris int; e_county int; e_mil int; e_tribal int;
  r_count int; p_count int;
begin
  e_cpad   := public.populate_cpad_containment_gold(p_fid);
  e_pad_us := public.populate_pad_us_containment_gold(p_fid, 100);
  e_juris  := public.populate_jurisdictions_containment_gold(p_fid);
  e_county := public.populate_counties_containment_gold(p_fid);
  e_mil    := public.populate_military_containment_gold(p_fid);
  e_tribal := public.populate_tribal_containment_gold(p_fid);
  r_count  := public._resolve_polygon_containment(p_fid);
  p_count  := public._promote_polygon_containment_to_gold(p_fid);
  return query select (e_cpad + e_pad_us + e_juris + e_county + e_mil + e_tribal),
                      r_count, p_count;
end;
$$;


-- ─── 3. Extend governance derivation to handle pad_us_unit kind ──────
-- Existing populate_governance_from_polygon_gold only reads c1_city
-- and county polygon_containment rows. Adds reading from pad_us_unit
-- to derive governance for non-CA states + protected-area beaches.
-- Priority: pad_us_unit (most specific operator) > city > county.

create or replace function public.populate_governance_from_polygon_gold(p_fid bigint)
returns integer
language plpgsql
as $$
declare
  v_inserted int := 0;
  v_state text;
  v_city_name text;
  v_county_name text;
  v_padus jsonb;
  v_padus_normalized jsonb;
  v_padus_op_id bigint;
begin
  select state into v_state from public.beaches_gold where fid = p_fid;

  -- Idempotency: clear prior derived rows from this source.
  delete from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group = 'governance'
     and source = 'polygon_containment_governance_v1';

  -- Layer 1: PAD-US unit (most specific — protected areas with named operator)
  select claimed_values into v_padus
    from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group = 'polygon_containment'
     and is_canonical = true
     and claimed_values->>'polygon_kind' = 'pad_us_unit'
   limit 1;

  if v_padus is not null then
    if v_padus->>'own_name' = 'DESG' then
      -- Designation areas (Wilderness, Wild & Scenic) — fall back to mng_*
      v_padus_normalized := public._normalize_pad_us_manager(
        v_padus->>'mng_type', v_padus->>'mng_name',
        v_padus->>'polygon_name', v_state);
    else
      v_padus_normalized := public._normalize_pad_us_manager(
        v_padus->>'own_type', v_padus->>'own_name',
        v_padus->>'polygon_name', v_state);
    end if;

    select operator_id into v_padus_op_id
      from public.resolve_agency(v_padus_normalized->>'name', 'polygon_containment',
                                 v_state, 0.65)
     order by confidence desc, method limit 1;

    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, is_canonical,
       updated_at, claimed_values, operator_id)
    values (p_fid, 'governance', 'polygon_containment_governance_v1', 0.75, false,
            now(),
            v_padus_normalized || jsonb_build_object(
              'pad_us_unit_id', v_padus->>'polygon_id',
              'pad_us_unit_name', v_padus->>'polygon_name'),
            v_padus_op_id);
    v_inserted := v_inserted + 1;
    return v_inserted;
  end if;

  -- Layer 2 (fallback): city polygon_containment
  select claimed_values->>'polygon_name' into v_city_name
    from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group = 'polygon_containment'
     and is_canonical = true
     and claimed_values->>'polygon_kind' = 'c1_city'
   limit 1;

  -- Layer 3 (fallback): county polygon_containment
  select claimed_values->>'polygon_name' into v_county_name
    from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group = 'polygon_containment'
     and is_canonical = true
     and claimed_values->>'polygon_kind' = 'county'
   limit 1;

  if v_city_name is not null then
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, is_canonical,
       updated_at, claimed_values)
    values (p_fid, 'governance', 'polygon_containment_governance_v1', 0.65, false,
            now(),
            jsonb_build_object('name', 'City of ' || v_city_name, 'type', 'city'));
    v_inserted := v_inserted + 1;
  end if;

  if v_county_name is not null and v_city_name is null then
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, is_canonical,
       updated_at, claimed_values)
    values (p_fid, 'governance', 'polygon_containment_governance_v1', 0.60, false,
            now(),
            jsonb_build_object('name', 'County of ' || v_county_name, 'type', 'county'));
    v_inserted := v_inserted + 1;
  end if;

  return v_inserted;
end;
$$;


-- ─── 4. Batch wrapper: run polygon→governance for many beaches ─────────
-- Used by the orchestration step to fire derivation for every active
-- beach at once instead of looping in Python.
create or replace function public.populate_governance_from_polygon_gold_batch(
  p_state text default null
)
returns integer
language plpgsql
as $$
declare
  v_fid bigint;
  v_total int := 0;
  v_n int;
begin
  for v_fid in
    select fid from public.beaches_gold
     where is_active and is_scoreable
       and (p_state is null or state = p_state)
  loop
    v_n := public.populate_governance_from_polygon_gold(v_fid);
    v_total := v_total + v_n;
  end loop;
  return v_total;
end;
$$;

commit;
