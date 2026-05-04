-- 20260504_unified_pipeline_phase1_city_county_policy_populators.sql
--
-- Two new populators that close the city/county evidence gap. Mirror the
-- shape of populate_from_park_operators_gold:
--   - join beaches_gold → operators (via existing FK on the beach side)
--   - operator_dogs_policy where policy_found = true
--   - emit beach_enrichment_provenance row(s)
--   - ON CONFLICT (gold_fid, field_group, source) DO UPDATE
--   - is_canonical reset on update
--
-- Sources:
--   city_policy   — operators.level='city',   joined via c1_jurisdiction_id
--   county_policy — operators.level='county', joined via county_geoid
--
-- Both write field_group='dogs' (operator_dogs_policy is dog-policy-only).

begin;

-- 1. Source CHECK extended
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual', 'plz', 'cpad', 'tiger_places', 'ccc', 'llm', 'web_scrape',
    'research', 'csp_parks', 'park_operators', 'nps_places', 'tribal_lands',
    'military_bases', 'pad_us', 'sma_code_mappings', 'jurisdictions',
    'csp_places', 'name', 'governing_body', 'park_url',
    'park_url_buffer_attribution', 'old_school_llm', 'counties',
    'json_explode', 'unified_v1', 'city_policy', 'county_policy'
  ]));

-- 2. Source-family taxonomy: each is its own independent family
--    (city ordinance and county ordinance are genuinely different signals)
create or replace function public._source_family(p_source text)
returns text language sql immutable as $$
  select case p_source
    when 'manual'                      then 'manual'
    when 'manual_curator'              then 'manual'
    when 'park_url'                    then 'park_url'
    when 'park_url_governance'         then 'park_url'
    when 'park_url_buffer_attribution' then 'park_url'
    when 'park_operators'              then 'park_operators'
    when 'research'                    then 'research'
    when 'governing_body'              then 'governing_body'
    when 'city_policy'                 then 'city_policy'
    when 'county_policy'               then 'county_policy'
    when 'old_school_llm'              then 'llm_extraction'
    when 'unified_v1'                  then 'llm_extraction'
    when 'json_explode'                then 'llm_extraction'
    when 'llm'                         then 'llm_extraction'
    when 'cpad'                        then 'spatial'
    when 'jurisdictions'               then 'spatial'
    when 'counties'                    then 'spatial'
    when 'military_bases'              then 'spatial'
    when 'tribal_lands'                then 'spatial'
    when 'csp_parks'                   then 'spatial'
    when 'csp_places'                  then 'spatial'
    when 'nps_places'                  then 'spatial'
    when 'tiger_places'                then 'spatial'
    when 'pad_us'                      then 'spatial'
    when 'sma_code_mappings'           then 'spatial'
    when 'name'                        then 'name_heuristic'
    when 'plz'                         then 'plz'
    when 'ccc'                         then 'ccc'
    when 'web_scrape'                  then 'park_url'
    else p_source
  end;
$$;

-- 3. Field-group priority. city_policy + county_policy are authoritative
--    ordinance sources — slot just below park_url (priority 4) and
--    above llm_extraction (priority 5). Calibration weight will further
--    tune within the consensus.
create or replace function public._gold_field_group_source_priority(
  p_field_group text, p_source text
) returns integer language sql immutable as $$
  select case
    when p_source = 'manual' then 1
    when p_field_group = 'dogs' then case p_source
      when 'llm'              then 2
      when 'research'         then 3
      when 'park_url'         then 4
      when 'city_policy'      then 4   -- authoritative ordinance
      when 'county_policy'    then 4   -- authoritative ordinance
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
      when 'park_url'                    then 2
      when 'park_operators'              then 2
      when 'park_url_buffer_attribution' then 3
      when 'csp_parks'                   then 4
      when 'nps_places'                  then 4
      when 'cpad'                        then 5
      when 'tiger_places'                then 6
      when 'governing_body'              then 7
      when 'name'                        then 8
      when 'tribal_lands'                then 9
      when 'military_bases'              then 9
      else                                    99
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
$$;

-- 4. Seed calibration entries. We don't have ground-truth calibration yet
--    for city_policy/county_policy so use modest priors. Wilson-90 on n=10
--    bounds these to ~0.55-0.60 — moderate weight, will refine with data.
insert into public.field_source_calibration (field_name, source, raw_accuracy, sample_size, notes) values
  ('dogs_allowed', 'city_policy',   0.85, 10, 'Initial prior — extracted from city ordinance, authoritative source'),
  ('dogs_allowed', 'county_policy', 0.85, 10, 'Initial prior — extracted from county ordinance, authoritative source'),
  ('leash_policy', 'city_policy',   0.85, 10, 'Initial prior'),
  ('leash_policy', 'county_policy', 0.85, 10, 'Initial prior')
on conflict (field_name, source) do update
  set raw_accuracy = excluded.raw_accuracy,
      sample_size = excluded.sample_size,
      notes = excluded.notes,
      computed_at = now();

-- 5. populate_from_city_dog_policy_gold
--    Mirrors populate_from_park_operators_gold:
--    beaches_gold → operators (level='city', joined via jurisdiction_id)
--    → operator_dogs_policy (policy_found=true) → emit
create or replace function public.populate_from_city_dog_policy_gold(
  p_fid bigint default null
) returns integer language plpgsql as $$
declare rows_touched int;
begin
  with matches as (
    select g.fid,
           op.canonical_name as operator_name, op.dog_policy_url, op.id as op_id,
           odp.default_rule, odp.leash_required, odp.summary,
           odp.area_sand, odp.area_water, odp.area_picnic_area,
           odp.area_parking_lot, odp.area_trails, odp.area_campground,
           odp.designated_dog_zones, odp.prohibited_areas,
           odp.time_windows, odp.seasonal_closures, odp.spatial_zones,
           coalesce(odp.pass_b_confidence, odp.pass_a_confidence) as op_conf,
           odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.jurisdiction_id = g.c1_jurisdiction_id
       and op.level = 'city'
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id
       and odp.policy_found = true
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.c1_jurisdiction_id is not null
  ),
  built as (
    select fid, operator_name, dog_policy_url, op_conf, source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case
               when default_rule in ('yes','allowed','restricted','seasonal') then 'yes'
               when default_rule in ('no','prohibited','not_allowed') then 'no'
               else null end,
             'leash_required', case
               when leash_required is true  then 'on_leash'
               when leash_required is false then 'off_leash' else null end,
             'notes', summary,
             'area_sand',         area_sand,
             'area_water',        area_water,
             'area_picnic_area',  area_picnic_area,
             'area_parking_lot',  area_parking_lot,
             'area_trails',       area_trails,
             'area_campground',   area_campground,
             'designated_dog_zones', designated_dog_zones,
             'prohibited_areas',     prohibited_areas,
             'time_windows',         time_windows,
             'seasonal_closures',    seasonal_closures,
             'operator_name',        operator_name
           )) as v
      from matches
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', 'city_policy',
      coalesce(op_conf, 0.85),
      v, coalesce(source_url, dog_policy_url), now()
    from built where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end;
$$;

comment on function public.populate_from_city_dog_policy_gold(bigint) is
  '2026-05-04. Propagates city-ordinance dog policy (operators.level=city, '
  'operator_dogs_policy.policy_found=true) to beaches inside the city''s '
  'TIGER place polygon (joined via beaches_gold.c1_jurisdiction_id → '
  'operators.jurisdiction_id). Mirrors populate_from_park_operators_gold '
  'shape. Writes field_group=dogs, source=city_policy.';

-- 6. populate_from_county_dog_policy_gold
create or replace function public.populate_from_county_dog_policy_gold(
  p_fid bigint default null
) returns integer language plpgsql as $$
declare rows_touched int;
begin
  with matches as (
    select g.fid,
           op.canonical_name as operator_name, op.dog_policy_url, op.id as op_id,
           odp.default_rule, odp.leash_required, odp.summary,
           odp.area_sand, odp.area_water, odp.area_picnic_area,
           odp.area_parking_lot, odp.area_trails, odp.area_campground,
           odp.designated_dog_zones, odp.prohibited_areas,
           odp.time_windows, odp.seasonal_closures, odp.spatial_zones,
           coalesce(odp.pass_b_confidence, odp.pass_a_confidence) as op_conf,
           odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.county_geoid = g.county_geoid
       and op.level = 'county'
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id
       and odp.policy_found = true
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.county_geoid is not null
  ),
  built as (
    select fid, operator_name, dog_policy_url, op_conf, source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case
               when default_rule in ('yes','allowed','restricted','seasonal') then 'yes'
               when default_rule in ('no','prohibited','not_allowed') then 'no'
               else null end,
             'leash_required', case
               when leash_required is true  then 'on_leash'
               when leash_required is false then 'off_leash' else null end,
             'notes', summary,
             'area_sand',         area_sand,
             'area_water',        area_water,
             'area_picnic_area',  area_picnic_area,
             'area_parking_lot',  area_parking_lot,
             'area_trails',       area_trails,
             'area_campground',   area_campground,
             'designated_dog_zones', designated_dog_zones,
             'prohibited_areas',     prohibited_areas,
             'time_windows',         time_windows,
             'seasonal_closures',    seasonal_closures,
             'operator_name',        operator_name
           )) as v
      from matches
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', 'county_policy',
      coalesce(op_conf, 0.85),
      v, coalesce(source_url, dog_policy_url), now()
    from built where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end;
$$;

comment on function public.populate_from_county_dog_policy_gold(bigint) is
  '2026-05-04. Propagates county-ordinance dog policy (operators.level=county, '
  'operator_dogs_policy.policy_found=true) to beaches inside the county polygon '
  '(joined via beaches_gold.county_geoid → operators.county_geoid). Mirrors '
  'populate_from_park_operators_gold shape. Writes field_group=dogs, '
  'source=county_policy.';

-- 7. Update auto-promoter source whitelist for dogs
create or replace function public.promote_canonical_dogs_to_beach_dog_policy(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint) as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with consensus_dogs as (
    select gold_fid,
      max(winning_value) filter (where field_name = 'allowed')        as v_allowed,
      max(confidence)    filter (where field_name = 'allowed')        as conf_allowed,
      bool_or(disagreement) filter (where field_name = 'allowed')     as dis_allowed,
      max(winning_value) filter (where field_name = 'leash_required') as v_leash,
      max(confidence)    filter (where field_name = 'leash_required') as conf_leash,
      bool_or(disagreement) filter (where field_name = 'leash_required') as dis_leash
      from public.beach_field_consensus
     where field_group = 'dogs' and (p_fid is null or gold_fid = p_fid)
     group by gold_fid
  ),
  canon_payload as (
    select e.gold_fid as fid, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
     where e.field_group = 'dogs' and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  derived as (
    select c.gold_fid as fid,
           public._norm_dogs_allowed(c.v_allowed)        as new_dogs_allowed,
           public._norm_leash_policy(c.v_leash)          as new_leash_policy,
           public._norm_bool(p.claimed_values->'off_leash_exists') as new_off_leash_flag,
           public._norm_time_text(split_part(p.claimed_values->>'time_prohibited_hours','-',1)) as new_prohib_start,
           public._norm_time_text(split_part(p.claimed_values->>'time_prohibited_hours','-',2)) as new_prohib_end,
           coalesce(p.claimed_values->>'areas_evidence',
                    p.claimed_values->>'areas_boundaries',
                    p.claimed_values->>'designated_dog_zones',
                    p.claimed_values->>'prohibited_areas') as new_dogs_allowed_areas,
           greatest(c.conf_allowed, c.conf_leash) as new_confidence,
           coalesce(c.dis_allowed, false) or coalesce(c.dis_leash, false) as new_disagreement
      from consensus_dogs c
      left join canon_payload p on p.fid = c.gold_fid
  ),
  protected as (
    select arena_group_id from public.beach_dog_policy where source = 'manual_curator'
  ),
  upsert as (
    insert into public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       dogs_prohibited_start, dogs_prohibited_end, dogs_allowed_areas,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select d.fid, d.new_dogs_allowed, d.new_leash_policy, d.new_off_leash_flag,
           d.new_prohib_start, d.new_prohib_end, d.new_dogs_allowed_areas,
           'auto_promoted_from_consensus', now(),
           format('Phase B consensus: dogs_allowed=%s (conf=%s%s), leash=%s (conf=%s%s)',
                  d.new_dogs_allowed, coalesce(d.new_confidence, 0),
                  case when d.new_disagreement then ', DISAGREEMENT' else '' end,
                  d.new_leash_policy, coalesce(d.new_confidence, 0),
                  case when d.new_disagreement then ', DISAGREEMENT' else '' end),
           d.new_confidence, d.new_disagreement
      from derived d
     where d.fid not in (select arena_group_id from protected)
       and (d.new_dogs_allowed is not null or d.new_leash_policy is not null
            or d.new_off_leash_flag is not null or d.new_prohib_start is not null
            or d.new_dogs_allowed_areas is not null)
       and coalesce(d.new_confidence, 1) >= p_min_confidence
    on conflict (arena_group_id) do update
      set dogs_allowed=excluded.dogs_allowed,
          leash_policy=excluded.leash_policy,
          off_leash_flag=excluded.off_leash_flag,
          dogs_prohibited_start=excluded.dogs_prohibited_start,
          dogs_prohibited_end=excluded.dogs_prohibited_end,
          dogs_allowed_areas=excluded.dogs_allowed_areas,
          source=excluded.source, curated_at=now(), notes=excluded.notes,
          consensus_confidence=excluded.consensus_confidence,
          disagreement_flag=excluded.disagreement_flag
      where beach_dog_policy.source <> 'manual_curator'
        and (beach_dog_policy.dogs_allowed is distinct from excluded.dogs_allowed
          or beach_dog_policy.leash_policy is distinct from excluded.leash_policy
          or beach_dog_policy.off_leash_flag is distinct from excluded.off_leash_flag
          or beach_dog_policy.dogs_prohibited_start is distinct from excluded.dogs_prohibited_start
          or beach_dog_policy.dogs_prohibited_end is distinct from excluded.dogs_prohibited_end
          or beach_dog_policy.dogs_allowed_areas is distinct from excluded.dogs_allowed_areas
          or beach_dog_policy.consensus_confidence is distinct from excluded.consensus_confidence
          or beach_dog_policy.disagreement_flag is distinct from excluded.disagreement_flag)
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upsert;
  return query select ins::bigint, upd::bigint, skp::bigint;
end;
$$ language plpgsql;

-- 8. Wire into promote_to_gold (per-fid loop on newly-promoted; also
--    runs on all p_fids for the --with-extraction re-entry case).
create or replace function public.promote_to_gold(p_fids bigint[], p_score boolean default false)
returns table(rows_promoted bigint, rows_already_in_gold bigint, containment_evidence bigint,
              source_evidence bigint, beach_dog_policy_changed bigint, beach_amenities_changed bigint,
              noaa_assigned bigint, slugs_assigned bigint) as $$
declare promoted int := 0; existing int := 0; fid_iter bigint;
        contain_ev int := 0; source_ev int := 0;
        bdp_changed int := 0; ba_changed int := 0;
        noaa_count int := 0; slug_count int := 0;
        new_fids bigint[];
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint;
    return;
  end if;

  with to_promote as (
    select a.* from public.arena a
     where a.fid = any(p_fids) and a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name, source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source, park_name, state, promoted_from, promoted_at,
       is_active, noaa_station_id, timezone, open_time, close_time, is_scoreable, geom)
    select t.fid, public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.lat, t.lon, t.county_name, t.source_code, t.source_id, coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source, null,
      public._infer_state_from_county(t.county_name),
      'promote_to_gold_v3', now(), true,
      public._nearest_noaa_station(st_setsrid(st_makepoint(t.lon, t.lat), 4326)::geography),
      'America/Los_Angeles', '05:00', '22:00', coalesce(p_score, false),
      st_setsrid(st_makepoint(t.lon, t.lat), 4326)
    from to_promote t
    returning fid, location_id, noaa_station_id
  )
  select array_agg(fid), count(*), count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into new_fids, promoted, slug_count, noaa_count from ins;
  existing := array_length(p_fids, 1) - promoted;

  -- Per-fid populators on NEWLY-PROMOTED fids
  if new_fids is not null then
    foreach fid_iter in array new_fids loop
      perform public.populate_polygon_containment_gold(fid_iter);
      perform public.populate_from_cpad_gold(fid_iter);
      perform public.populate_from_park_operators_gold(fid_iter);
      perform public.populate_from_research_gold(fid_iter);
      perform public.populate_from_park_url_gold(fid_iter);
      perform public.populate_from_park_url_governance_gold(fid_iter);
      perform public.populate_from_unified_v1_gold(fid_iter);
      perform public.populate_from_city_dog_policy_gold(fid_iter);   -- NEW
      perform public.populate_from_county_dog_policy_gold(fid_iter); -- NEW
    end loop;
  end if;

  -- For existing-in-gold p_fids, run unified_v1 + city + county
  -- (handles --with-extraction re-entry + catches city/county policy
  -- updates on later runs)
  foreach fid_iter in array p_fids loop
    if (new_fids is null or fid_iter <> all(new_fids)) then
      perform public.populate_from_unified_v1_gold(fid_iter);
      perform public.populate_from_city_dog_policy_gold(fid_iter);
      perform public.populate_from_county_dog_policy_gold(fid_iter);
    end if;
  end loop;

  perform public._resolve_polygon_containment(null);
  perform public._resolve_governance_gold(null);
  perform public._resolve_dogs_gold(null);
  perform public._resolve_practical_gold(null);
  perform public._resolve_field_group_gold('access', null);

  foreach fid_iter in array p_fids loop
    perform public.compute_beach_field_consensus(fid_iter);
  end loop;

  select count(*) into contain_ev from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  foreach fid_iter in array p_fids loop
    perform public.promote_canonical_to_consumer_tables(fid_iter);
  end loop;

  select count(*) into bdp_changed from public.beach_dog_policy
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
  select count(*) into ba_changed from public.beach_amenities
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';

  return query select promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
                      bdp_changed::bigint, ba_changed::bigint, noaa_count::bigint, slug_count::bigint;
end;
$$ language plpgsql;

commit;
