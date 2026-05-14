-- Multi-state governance attribution via PAD-US polygon containment.
--
-- Today's CA governance work (CPAD + polygon_containment + tiger_places)
-- doesn't extend to non-CA states because polygon_containment is CA-only
-- (uses CPAD which is California-Protected-Areas-Database). For OR, WA,
-- and the other 47 states we use PAD-US (Protected Areas Database of the
-- United States — federal-aggregated, all-state coverage), which is
-- already loaded into public.pad_us_units (308,001 rows, 9,598 for OR).
--
-- This migration adds:
--   1. populate_governance_from_pad_us_gold(p_fid) — picks the best
--      PAD-US polygon containing the beach and writes a governance
--      claim into beach_enrichment_provenance. Source =
--      'pad_us_governance_v1'. Idempotent.
--   2. Manager-name normalizer — PAD-US uses abbreviations (USFS, BLM,
--      SPR, FWS, NPS, CITY, CNTY, REG, NGO, ...); map to readable
--      operator names matching the CA convention.
--   3. Priority slot in _gold_field_group_source_priority — peer of
--      tiger_places (priority 6), so the resolver can pick between
--      tiger_places (CA) and pad_us_governance_v1 (other states).
--
-- Selection rule: within a beach's containing polygons, pick the
-- smallest-area polygon at the highest-priority manager type
-- (FED > STAT > DIST > LOC > others). Smallest area = most specific
-- operator (a beach inside Cape Lookout SP + Tillamook County will get
-- the SP attribution, not the county).

begin;

-- ─── Manager-name normalizer ────────────────────────────────────────
create or replace function public._normalize_pad_us_manager(
  p_mng_type text, p_mng_name text, p_unit_name text, p_state text
) returns jsonb
language plpgsql immutable
as $$
declare
  v_op_type text;
  v_op_name text;
begin
  v_op_type := case p_mng_type
    when 'FED'  then 'federal'
    when 'STAT' then 'state'
    when 'LOC'  then 'city'        -- refined below; could be county
    when 'DIST' then 'special_district'
    when 'NGO'  then 'nonprofit'
    when 'PVT'  then 'private'
    when 'TRIB' then 'tribal'
    when 'JNT'  then 'joint'
    else 'other'
  end;

  v_op_name := case p_mng_name
    when 'USFS' then 'United States Forest Service'
    when 'BLM'  then 'United States Bureau of Land Management'
    when 'FWS'  then 'United States Fish and Wildlife Service'
    when 'NPS'  then 'National Park Service'
    when 'DOD'  then 'United States Department of Defense'
    when 'USACE' then 'United States Army Corps of Engineers'
    when 'BOR'  then 'United States Bureau of Reclamation'
    when 'NRCS' then 'Natural Resources Conservation Service'
    when 'SPR'  then case p_state
                      when 'CA' then 'California Department of Parks and Recreation'
                      when 'OR' then 'Oregon Parks and Recreation Department'
                      when 'WA' then 'Washington State Parks and Recreation Commission'
                      else p_state || ' State Parks'
                    end
    when 'SFW'  then case p_state
                      when 'OR' then 'Oregon Department of Fish and Wildlife'
                      when 'CA' then 'California Department of Fish and Wildlife'
                      when 'WA' then 'Washington Department of Fish and Wildlife'
                      else p_state || ' Department of Fish and Wildlife'
                    end
    when 'OTHS' then p_state || ' Other State Agency'
    when 'SDOL' then case p_state
                      when 'OR' then 'Oregon Department of State Lands'
                      else p_state || ' Department of State Lands'
                    end
    when 'CITY' then 'City of ' || coalesce(p_unit_name, 'unknown')
    when 'CNTY' then 'County of ' || coalesce(p_unit_name, 'unknown')
    when 'REG'  then coalesce(p_unit_name, 'Regional Park District')
    when 'NGO'  then coalesce(p_unit_name, 'Nonprofit')
    when 'PVT'  then 'Private'
    when 'TRIB' then coalesce(p_unit_name, 'Tribal')
    else coalesce(p_mng_name, 'Unknown')
  end;

  -- LOC subtype refinement: distinguish city vs county
  if p_mng_type = 'LOC' and p_mng_name = 'CNTY' then
    v_op_type := 'county';
  end if;

  return jsonb_build_object('name', v_op_name, 'type', v_op_type,
                            'source_mng_type', p_mng_type,
                            'source_mng_name', p_mng_name);
end;
$$;

-- ─── populator ────────────────────────────────────────────────────────
-- p_buffer_m: extend each PAD-US polygon by this many meters before
-- testing containment. Default 0 (strict). For Oregon beaches, 100m
-- captures the wet-sand strip just seaward of state-park polygon
-- boundaries (Oregon Beach Bill territory). Uses ST_DWithin which is
-- GiST-index-friendly (same pattern as our 450m photo distance filter
-- and dedup proximity checks).
create or replace function public.populate_governance_from_pad_us_gold(
  p_fid bigint, p_buffer_m int default 0
)
returns integer
language plpgsql
as $$
declare
  v_inserted int := 0;
  v_beach_pt geometry;
  v_beach_geog geography;
  v_state text;
  v_op_id bigint;
  v_op_name text;
  v_normalized jsonb;
  r record;
begin
  select st_setsrid(st_makepoint(lon, lat), 4326),
         st_setsrid(st_makepoint(lon, lat), 4326)::geography,
         state
    into v_beach_pt, v_beach_geog, v_state
    from public.beaches_gold
   where fid = p_fid and is_active and lat is not null;

  if v_beach_pt is null then return 0; end if;

  delete from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group = 'governance'
     and source = 'pad_us_governance_v1';

  -- Find the best containing/nearby polygon. Use OWNER priority not MANAGER
  -- because PAD-US has data anomalies where mng_type says "LOC/CITY"
  -- on a polygon that's actually State Parks (own_type=STAT/SPR,
  -- unit_name='X State Park'). Owner is semantically stable; manager
  -- is occasionally just the local cooperator. We pass both mng AND
  -- own to the normalizer so it can prefer the most specific
  -- attribution.
  for r in
    select pu.unit_id, pu.mng_type, pu.mng_name, pu.own_type, pu.own_name,
           pu.unit_name, pu.state padus_state,
           st_area(pu.geom::geography) area_m2,
           -- Use mng_* priority when own_name='DESG' (designations like
           -- Wilderness Study Areas have owner='DESG' but the manager
           -- is the real federal agency). Else own_* priority.
           case
             when pu.own_name = 'DESG' then
               case pu.mng_type
                 when 'FED' then 10 when 'STAT' then 8 when 'DIST' then 6
                 when 'LOC' then 5 when 'TRIB' then 4 when 'JNT' then 3
                 when 'NGO' then 2 when 'PVT' then 1 else 0 end
             else
               case pu.own_type
                 when 'FED' then 10 when 'STAT' then 8 when 'DIST' then 6
                 when 'LOC' then 5 when 'TRIB' then 4 when 'JNT' then 3
                 when 'NGO' then 2 when 'PVT' then 1 else 0 end
           end priority
      from public.pad_us_units pu
     where pu.state = v_state
       and (case
              when p_buffer_m = 0 then st_intersects(pu.geom, v_beach_pt)
              else st_dwithin(pu.geom::geography, v_beach_geog, p_buffer_m)
            end)
     order by priority desc, st_area(pu.geom::geography) asc
     limit 1
  loop
    -- Use own_* as primary attribution, EXCEPT for DESG (designation
    -- areas) where owner is the designation type and the real operator
    -- is in mng_*.
    if r.own_name = 'DESG' then
      v_normalized := public._normalize_pad_us_manager(
        r.mng_type, r.mng_name, r.unit_name, r.padus_state);
    else
      v_normalized := public._normalize_pad_us_manager(
        r.own_type, r.own_name, r.unit_name, r.padus_state);
    end if;
    v_op_name := v_normalized->>'name';

    -- Run the normalized name through resolve_agency to get a canonical
    -- operator_id when one exists. Federal agencies are state-scoped in
    -- the current operators table (a known data-quality issue) — for
    -- OR/WA/etc. resolution will often miss and operator_id stays NULL.
    -- That's OK; the BEP row still has a correct normalized name.
    select operator_id into v_op_id
      from public.resolve_agency(v_op_name, 'pad_us', v_state, 0.65)
     order by confidence desc, method
     limit 1;

    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, is_canonical,
       updated_at, claimed_values, operator_id)
    values (p_fid, 'governance', 'pad_us_governance_v1', 0.70, false,
            now(),
            v_normalized
              || jsonb_build_object('pad_us_unit_id', r.unit_id,
                                    'pad_us_unit_name', r.unit_name),
            v_op_id);
    v_inserted := v_inserted + 1;
  end loop;

  return v_inserted;
end;
$$;

-- ─── priority slot ────────────────────────────────────────────────────
-- pad_us_governance_v1 sits at priority 6, peer of tiger_places + the
-- CA polygon_containment_governance_v1. _resolve_governance_gold picks
-- the highest-priority claim across all sources.
create or replace function public._gold_field_group_source_priority(p_field_group text, p_source text)
returns integer
language sql
immutable
as $$
  select case
    when p_field_group = 'governance' then case p_source
      when 'cpad_governance_v2' then 10
      when 'pad_us_governance_v1' then 6
      when 'polygon_containment_governance_v1' then 6
      when 'tiger_places' then 6
      when 'osm_tags' then 4
      when 'extracted_from_url' then 3
      when 'manual' then 9
      else 1
    end
    when p_field_group = 'park_url' then case p_source
      when 'cpad_park_url_overrides' then 10
      when 'manual' then 9
      when 'extracted_from_url' then 5
      else 1
    end
    else 1
  end;
$$;

commit;
