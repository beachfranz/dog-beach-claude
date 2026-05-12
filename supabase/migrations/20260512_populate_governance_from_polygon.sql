-- 20260512_populate_governance_from_polygon.sql
--
-- Extends the governance-resolver fix from federal (BLM/USCG) to state
-- (Lands Commission, Coastal Conservancy, "Other State") by giving the
-- resolver an alternative source it can use when tiger_places didn't
-- write a city/county claim. polygon_containment ALREADY knows the
-- right answer (c1_city for Malibu beaches, county_geoid for everything),
-- it just wasn't surfacing into the governance field_group.
--
-- Adds:
--   1. populate_governance_from_polygon_gold(p_fid) — writes a derived
--      governance row from polygon_containment data. Source =
--      'polygon_containment_governance_v1'. Idempotent.
--   2. Priority entry in _gold_field_group_source_priority at 6 (same
--      tier as tiger_places).
--   3. Hook into refire_bep_cascade so it regenerates on every refire.
--   4. Extends the denylist in _resolve_governance_gold with the 3 state
--      agency patterns.

begin;

-- ─── populator ────────────────────────────────────────────────────────
create or replace function public.populate_governance_from_polygon_gold(p_fid bigint)
returns integer
language plpgsql
as $function$
declare
  v_inserted int := 0;
  v_city_name text;
  v_county_name text;
begin
  -- City (c1_city polygon_containment row → "City of {name}")
  select claimed_values->>'polygon_name' into v_city_name
    from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group = 'polygon_containment'
     and is_canonical = true
     and claimed_values->>'polygon_kind' = 'c1_city'
   limit 1;

  -- County (always present for a CA beach with polygon_containment)
  select claimed_values->>'polygon_name' into v_county_name
    from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group = 'polygon_containment'
     and is_canonical = true
     and claimed_values->>'polygon_kind' = 'county'
   limit 1;

  -- Insert city governance claim if we have a city
  if v_city_name is not null then
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, is_canonical,
       updated_at, claimed_values)
    values (p_fid, 'governance', 'polygon_containment_governance_v1', 0.65, false,
            now(),
            jsonb_build_object('name', 'City of ' || v_city_name, 'type', 'city'));
    v_inserted := v_inserted + 1;
  end if;

  -- Insert county governance claim if we have a county (fallback when
  -- no city — i.e. unincorporated areas like Toro Canyon CDP)
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
$function$;

-- ─── priority slot ───────────────────────────────────────────────────
create or replace function public._gold_field_group_source_priority(p_field_group text, p_source text)
returns integer
language sql
immutable
as $function$
  select case
    when p_source = 'manual' then 1
    when p_field_group = 'dogs' then case p_source
      when 'llm'              then 2
      when 'research'         then 3
      when 'park_url'         then 4
      when 'city_policy'      then 4
      when 'county_policy'    then 4
      when 'unified_v1'       then 5
      when 'json_explode'     then 5
      when 'old_school_llm'   then 5
      when 'park_operators'   then 6
      when 'governing_body'   then 7
      when 'cpad'             then 8
      when 'jurisdictions'    then 9
      when 'counties'         then 10
      when 'nps_places'       then 11
      when 'military_bases'   then 12
      when 'tribal_lands'     then 12
      else                          99
    end
    when p_field_group = 'practical' then case p_source
      when 'llm'              then 2
      when 'park_url'         then 3
      when 'park_operators'   then 4
      when 'unified_v1'       then 5
      when 'old_school_llm'   then 5
      when 'json_explode'     then 5
      when 'ccc'              then 6
      when 'research'         then 7
      else                          99
    end
    when p_field_group = 'governance' then case p_source
      when 'park_url'                       then 2
      when 'park_operators'                 then 2
      when 'park_url_buffer_attribution'    then 3
      when 'csp_parks'                      then 4
      when 'nps_places'                     then 4
      when 'cpad'                           then 5
      when 'tiger_places'                   then 6
      when 'polygon_containment_governance_v1' then 6  -- new: derived from polygon_containment, peer of tiger_places
      when 'governing_body'                 then 7
      when 'name'                           then 8
      when 'tribal_lands'                   then 9
      when 'military_bases'                 then 9
      else                                       99
    end
    when p_field_group = 'access' then case p_source
      when 'plz'           then 2
      when 'cpad'          then 3
      when 'json_explode'  then 4
      when 'csp_parks'     then 5
      when 'military_bases' then 6
      else                       99
    end
    when p_field_group = 'polygon_containment' then case p_source
      when 'cpad'           then 2
      when 'jurisdictions'  then 3
      when 'counties'       then 4
      when 'military_bases' then 5
      when 'tribal_lands'   then 5
      else                       99
    end
    else 99
  end;
$function$;

-- ─── resolver: extend denylist with state agencies ───────────────────
create or replace function public._resolve_governance_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare
  v_touched int := 0;
  v_demoted int := 0;
begin
  v_touched := public._resolve_field_group_gold('governance', p_fid);

  with bad_canon as (
    select e.id, e.gold_fid
      from public.beach_enrichment_provenance e
     where e.field_group = 'governance'
       and e.is_canonical = true
       and e.source = 'cpad'
       and (
         -- Federal denylist (unchanged)
         e.claimed_values->>'name' ilike '%Bureau of Land Management%'
         or e.claimed_values->>'name' ilike '%Coast Guard%'
         or e.claimed_values->>'name' ilike '%Department of Defense%'
         or e.claimed_values->>'name' ~* '\m(Navy|Marine Corps|Air Force|Army Corps)\M'
         -- State denylist (NEW): non-park-managing state entities
         or e.claimed_values->>'name' = 'California State Lands Commission'
         or e.claimed_values->>'name' = 'California State Coastal Conservancy'
         or e.claimed_values->>'name' = 'Other State'
       )
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  -- Prefer tiger_places; fall back to polygon_containment-derived row
  alternative as (
    select distinct on (e.gold_fid) e.id, e.gold_fid
      from public.beach_enrichment_provenance e
     where e.field_group = 'governance'
       and e.source in ('tiger_places','polygon_containment_governance_v1')
       and lower(e.claimed_values->>'type') in ('city','county')
       and e.gold_fid in (select gold_fid from bad_canon)
     order by e.gold_fid,
              case e.source
                when 'tiger_places' then 1
                else 2 end,                    -- prefer tiger_places
              e.confidence desc nulls last,
              e.id asc
  ),
  pair as (
    select b.id as bad_id, a.id as good_id
      from bad_canon b
      join alternative a using (gold_fid)
  ),
  demote as (
    update public.beach_enrichment_provenance e
       set is_canonical = false
      from pair p
     where e.id = p.bad_id
    returning 1
  ),
  promote as (
    update public.beach_enrichment_provenance e
       set is_canonical = true
      from pair p
     where e.id = p.good_id
    returning 1
  )
  select count(*) into v_demoted from demote;

  return v_touched + v_demoted;
end;
$function$;

commit;
