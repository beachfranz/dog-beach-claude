-- 20260513_retire_is_scoreable.sql
--
-- Retires beaches_gold.is_scoreable. Matrix C' (refresh_scoring_tier)
-- already produces 'hourly'|'daily'|'none', and 'none' excludes Tier 4
-- + low-traffic Tier 3. The separate is_scoreable gate added stricter
-- constraints (T1+T2 only, NOAA-required) that are now stale — Matrix C'
-- permits high-traffic Tier 3 to refresh daily.
--
-- NOAA absence becomes a data-quality flag (noaa_station_id IS NULL),
-- not a hard exclusion. Tide score will be null for beaches without a
-- station; other metrics compute normally.
--
-- Net delta on active beaches: +217 daily + 36 hourly start refreshing;
-- -9 stale 'none' stop. New denominator: scoring_tier IN ('daily','hourly').

begin;

-- 1. Drop dependent views (will recreate below).
drop view if exists public.gold_evidence_audit cascade;
drop view if exists public.scoreability_review_queue cascade;
drop view if exists public.beach_pipeline_status cascade;
drop view if exists public.coord_audit_pairs cascade;
drop view if exists public.beach_governance_status cascade;
drop view if exists public.vw_pad_us_method_comparison cascade;

-- 2. Drop writer functions + their triggers (CASCADE).

drop function if exists public.align_is_scoreable_to_tier(text);
drop function if exists public._recompute_is_scoreable_for_fid(bigint);
drop function if exists public.assert_scoreable_have_noaa_for_state(text);
drop function if exists public.tg_auto_scoreable_socal_gold() CASCADE;
drop function if exists public.tg_demote_scoreable_when_no_dogs() CASCADE;
drop function if exists public.tg_inactive_clears_scoreable() CASCADE;
drop function if exists public.tg_recompute_scoreable_on_dog_policy() CASCADE;


-- 3a. Drop the two beaches_gold triggers whose WHEN clauses reference
--     is_scoreable directly (DROP COLUMN otherwise refuses). They get
--     recreated as scoring_tier-based triggers in section 5b.
drop trigger if exists tg_beaches_gold_score_on_flip   on public.beaches_gold;
drop trigger if exists tg_beaches_gold_score_on_insert on public.beaches_gold;

-- 3b. Drop the column. The partial index beaches_gold_is_scoreable_idx
--     auto-drops with it (DROP COLUMN cascades to dependent indexes).
alter table public.beaches_gold drop column if exists is_scoreable;

-- 3c. Replacement index on scoring_tier for the daily-refresh fan-out's
--     hot filter (WHERE state=$1 AND is_active AND scoring_tier IN ('daily','hourly')).
create index if not exists beaches_gold_scoring_tier_active_idx
  on public.beaches_gold (state, scoring_tier)
  where is_active and scoring_tier in ('daily','hourly');

-- 6. Recreate views with scoring_tier predicate.


-- view gold_evidence_audit
create or replace view public.gold_evidence_audit as
SELECT e.gold_fid,
    g.name AS beach_name,
    g.county_name,
    (g.scoring_tier in ('daily','hourly')) AS is_scoring_active,
    e.field_group,
    e.source,
    _gold_source_priority(e.source) AS source_priority,
    e.is_canonical,
    e.confidence,
    e.claimed_values,
    e.source_url,
    e.cpad_unit_name,
    e.extraction_type,
    e.updated_at
   FROM (beach_enrichment_provenance e
     JOIN beaches_gold g ON ((g.fid = e.gold_fid)))
  WHERE (e.gold_fid IS NOT NULL)
  ORDER BY e.gold_fid, e.field_group, (_gold_source_priority(e.source)), e.confidence DESC NULLS LAST;


-- view beach_pipeline_status
create or replace view public.beach_pipeline_status as
WITH base AS (
         SELECT bg.fid,
            bg.name,
            bg.county_name,
            bg.is_active,
            (bg.scoring_tier in ('daily','hourly')) AS is_scoring_active,
            bg.location_id,
            (EXISTS ( SELECT 1
                   FROM beach_enrichment_provenance e
                  WHERE ((e.gold_fid = bg.fid) AND (e.field_group = 'dogs'::text)))) AS has_bep_dogs,
            (EXISTS ( SELECT 1
                   FROM beach_dog_policy p
                  WHERE (p.arena_group_id = bg.fid))) AS has_policy_row,
            ( SELECT bool_or((p.dogs_allowed IS NOT NULL)) AS bool_or
                   FROM beach_dog_policy p
                  WHERE (p.arena_group_id = bg.fid)) AS has_dogs_allowed,
            ( SELECT bool_or((p.dogs_allowed = 'no'::text)) AS bool_or
                   FROM beach_dog_policy p
                  WHERE (p.arena_group_id = bg.fid)) AS dogs_explicitly_no,
            ( SELECT bool_or((p.zone_rules IS NOT NULL)) AS bool_or
                   FROM beach_dog_policy p
                  WHERE (p.arena_group_id = bg.fid)) AS has_zone_rules,
            (EXISTS ( SELECT 1
                   FROM beach_day_recommendations r
                  WHERE ((r.arena_group_id = bg.fid) AND (r.local_date >= (CURRENT_DATE - '1 day'::interval))))) AS has_recent_score,
            ( SELECT max(e.updated_at) AS max
                   FROM beach_enrichment_provenance e
                  WHERE ((e.gold_fid = bg.fid) AND (e.field_group = 'dogs'::text))) AS last_bep_at,
            ( SELECT max(COALESCE(p.zone_rules_updated_at, p.curated_at)) AS max
                   FROM beach_dog_policy p
                  WHERE (p.arena_group_id = bg.fid)) AS last_policy_at
           FROM beaches_gold bg
        )
 SELECT fid,
    name,
    county_name,
    is_active,
    is_scoring_active,
    location_id,
    has_bep_dogs,
    has_policy_row,
    has_dogs_allowed,
    dogs_explicitly_no,
    has_zone_rules,
    has_recent_score,
    last_bep_at,
    last_policy_at,
        CASE
            WHEN (NOT is_active) THEN '00_inactive'::text
            WHEN (NOT has_bep_dogs) THEN '01_no_evidence'::text
            WHEN (NOT has_policy_row) THEN '02_evidence_only'::text
            WHEN (NOT COALESCE(has_dogs_allowed, false)) THEN '03_canonical_pending'::text
            WHEN (NOT COALESCE(has_zone_rules, false)) THEN '04_dogs_resolved_no_zones'::text
            WHEN COALESCE(dogs_explicitly_no, false) THEN '05_terminal_no_dogs'::text
            WHEN (NOT has_recent_score) THEN '05_zone_rules_pending_score'::text
            ELSE '06_complete'::text
        END AS phase,
    GREATEST(COALESCE(last_bep_at, '1970-01-01 00:00:00+00'::timestamp with time zone), COALESCE(last_policy_at, '1970-01-01 00:00:00+00'::timestamp with time zone)) AS last_touched_at
   FROM base b;


-- view coord_audit_pairs
create or replace view public.coord_audit_pairs as
WITH pairs AS (
         SELECT b.fid,
            COALESCE(b.display_name_override, b.name) AS beach_name,
            b.state,
            b.county_name,
            b.is_active,
            (b.scoring_tier in ('daily','hourly')) AS is_scoring_active,
            beach_location_tier(p.dogs_allowed, p.has_off_leash, p.has_on_leash, p.dogs_prohibited_start) AS tier,
            b.lat AS our_lat,
            b.lon AS our_lon,
            t.source,
            t.source_id,
            t.name AS their_name,
            t.lat AS their_lat,
            t.lng AS their_lng,
            t.source_url AS their_url,
            (st_distance((b.geom)::geography, (st_setsrid(st_makepoint((t.lng)::double precision, (t.lat)::double precision), 4326))::geography))::integer AS divergence_m
           FROM ((truth_external t
             JOIN beaches_gold b ON (((b.state = 'CA'::text) AND st_dwithin((b.geom)::geography, (st_setsrid(st_makepoint((t.lng)::double precision, (t.lat)::double precision), 4326))::geography, (2000)::double precision))))
             LEFT JOIN beach_dog_policy p ON ((p.arena_group_id = b.fid)))
          WHERE ((t.source = 'dogtrekker'::text) AND (t.lat IS NOT NULL))
        ), closest_per_fid AS (
         SELECT DISTINCT ON (pairs.fid) pairs.fid,
            pairs.beach_name,
            pairs.state,
            pairs.county_name,
            pairs.is_active,
            pairs.is_scoring_active,
            pairs.tier,
            pairs.our_lat,
            pairs.our_lon,
            pairs.source,
            pairs.source_id,
            pairs.their_name,
            pairs.their_lat,
            pairs.their_lng,
            pairs.their_url,
            pairs.divergence_m
           FROM pairs
          ORDER BY pairs.fid, pairs.divergence_m
        )
 SELECT fid,
    beach_name,
    state,
    county_name,
    is_active,
    is_scoring_active,
    tier,
    our_lat,
    our_lon,
    source,
    source_id,
    their_name,
    their_lat,
    their_lng,
    their_url,
    divergence_m
   FROM closest_per_fid
  WHERE (divergence_m >= 100)
  ORDER BY divergence_m DESC;


-- view beach_governance_status
create or replace view public.beach_governance_status as
SELECT g.fid,
    g.state,
    g.is_active,
    (g.scoring_tier in ('daily','hourly')) AS is_scoring_active,
    COALESCE(g.display_name_override, g.name) AS name,
    (b.claimed_values ->> 'name'::text) AS operator_name,
    (b.claimed_values ->> 'type'::text) AS operator_type,
    b.source AS governance_source,
    b.operator_id,
    ((b.claimed_values ->> 'name'::text) IS NOT NULL) AS is_attributed
   FROM (beaches_gold g
     LEFT JOIN beach_enrichment_provenance b ON (((b.gold_fid = g.fid) AND (b.field_group = 'governance'::text) AND (b.is_canonical = true))));


-- view vw_pad_us_method_comparison
create or replace view public.vw_pad_us_method_comparison as
SELECT g.fid,
    g.state,
    g.county_name,
    COALESCE(g.display_name_override, g.name) AS name,
    (a.claimed_values ->> 'polygon_name'::text) AS a_polygon,
    (a.claimed_values ->> 'own_name'::text) AS a_own_name,
    (a.claimed_values ->> 'mng_name'::text) AS a_mng_name,
    ((a.claimed_values ->> 'area_m2'::text))::numeric AS a_area_m2,
    a.confidence AS a_conf,
    (b.claimed_values ->> 'polygon_name'::text) AS b_polygon,
    (b.claimed_values ->> 'own_name'::text) AS b_own_name,
    (b.claimed_values ->> 'mng_name'::text) AS b_mng_name,
    ((b.claimed_values ->> 'dist_m'::text))::integer AS b_dist_m,
    ((b.claimed_values ->> 'name_token_overlap'::text))::integer AS b_name_tokens,
    (b.claimed_values ->> 'match_type'::text) AS b_match_type,
    b.confidence AS b_conf,
        CASE
            WHEN ((a.claimed_values IS NULL) AND (b.claimed_values IS NULL)) THEN 'both_missing'::text
            WHEN (a.claimed_values IS NULL) THEN 'a_missing_b_found'::text
            WHEN (b.claimed_values IS NULL) THEN 'b_missing_a_found'::text
            WHEN ((a.claimed_values ->> 'polygon_id'::text) = (b.claimed_values ->> 'polygon_id'::text)) THEN 'agree'::text
            ELSE 'disagree_different_polygon'::text
        END AS comparison
   FROM ((beaches_gold g
     LEFT JOIN beach_enrichment_provenance a ON (((a.gold_fid = g.fid) AND (a.field_group = 'polygon_containment'::text) AND (a.source = 'pad_us'::text))))
     LEFT JOIN beach_enrichment_provenance b ON (((b.gold_fid = g.fid) AND (b.field_group = 'polygon_containment'::text) AND (b.source = 'pad_us_v2'::text))))
  WHERE (g.is_active AND (g.scoring_tier in ('daily','hourly')));


-- scoreability_review_queue retired entirely — its whole purpose was the
-- T4-visible-but-not-scoreable cohort, which no longer exists.

-- 5. Recreate reader functions with scoring_tier predicate.


-- _dedup_richness_score (filter)

CREATE OR REPLACE FUNCTION public._dedup_richness_score(p_fid bigint)
 RETURNS integer
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select
    -- + 20 manual_curator BEP rows (human-blessed truth)
    (case when exists(select 1 from public.beach_enrichment_provenance
                      where gold_fid = p_fid and source = 'manual_curator') then 20 else 0 end)
    -- + 20 zone_rules JSONB populated (#2 strategic asset)
    + (case when (select zone_rules from public.beach_dog_policy
                  where arena_group_id = p_fid) is not null then 20 else 0 end)
    -- + 15 curator was_moved=true
    + (case when exists(select 1 from public.beach_location_curation_status
                        where arena_group_id = p_fid and was_moved) then 15 else 0 end)
    -- + 10 has clear operator (park_url / park_operators / operator_default BEP source)
    + (case when exists(select 1 from public.beach_enrichment_provenance
                        where gold_fid = p_fid
                          and source in ('park_operators','operator_default','park_url',
                                         'park_url_buffer_attribution')) then 10 else 0 end)
    -- + 8 has dogs_prohibited_start (real schedule)
    + (case when (select dogs_prohibited_start from public.beach_dog_policy
                  where arena_group_id = p_fid) is not null then 8 else 0 end)
    -- + 5 has beach_dog_policy with non-null dogs_allowed
    + (case when exists(select 1 from public.beach_dog_policy
                        where arena_group_id = p_fid and dogs_allowed is not null) then 5 else 0 end)
    -- + 5 has photos
    + (case when exists(select 1 from public.beach_photos
                        where arena_group_id = p_fid) then 5 else 0 end)
    -- + 3 (scoring_tier in ('daily','hourly'))
    + (case when (select (scoring_tier in ('daily','hourly')) from public.beaches_gold
                  where fid = p_fid) then 3 else 0 end)
    -- + 2 has noaa_station_id
    + (case when (select noaa_station_id from public.beaches_gold
                  where fid = p_fid) is not null then 2 else 0 end)
    -- + 1 per BEP row, capped at 5
    + least((select count(*) from public.beach_enrichment_provenance
             where gold_fid = p_fid)::int, 5)
    -- - 5 location_id ends with -<fid> (sloppy slug from later promoter)
    - (case when (select location_id from public.beaches_gold where fid = p_fid)
              ~ ('-' || p_fid::text || '$') then 5 else 0 end);
$function$;


-- _seed_operators_from_pad_us_unified (filter)

CREATE OR REPLACE FUNCTION public._seed_operators_from_pad_us_unified(p_state text, p_state_fp text, p_mng_types text[], p_target_level text, p_subtype text, p_origin_source text, p_beach_touching_only boolean DEFAULT false)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare v_count int := 0;
begin
  -- Group by the FINAL slug (not normalized loc_mang) so name variants
  -- that collapse to the same slug ("City of X" / "X, City of") get
  -- combined before INSERT — prevents 'ON CONFLICT cannot affect row twice'.
  with ranked as (
    select public.slugify_agency(pu.loc_mang || ' (' || p_state || ')') as slug,
           max(pu.loc_mang)                as canonical_name,
           array_agg(distinct pu.loc_mang) as aliases,
           array_agg(distinct pu.loc_mang) as loc_mangs,
           st_multi(st_union(pu.geom))     as state_geom
      from public.pad_us_units pu
     where pu.state = p_state
       and (
         pu.mng_type = ANY(p_mng_types)
         or public._classify_loc_mang_level(pu.loc_mang, pu.mng_type) = p_target_level
       )
       and public._normalize_agency_text(pu.loc_mang) is not null
       and (
         not p_beach_touching_only
         or exists (
           select 1 from public.beaches_gold g
            where g.state = p_state and g.is_active and (g.scoring_tier in ('daily','hourly'))
              and st_intersects(g.geom, pu.geom)
         )
       )
     group by public.slugify_agency(pu.loc_mang || ' (' || p_state || ')')
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      state_code, fips_state, geom, pad_us_mng_name, origin_source
    )
    select
      slug,
      canonical_name,
      canonical_name,
      aliases,
      p_target_level,
      p_subtype,
      p_state, p_state_fp, state_geom, loc_mangs,
      p_origin_source
    from ranked
    on conflict (slug) do update set
      geom            = excluded.geom,
      pad_us_mng_name = (select array_agg(distinct n)
                         from unnest(coalesce(public.operators.pad_us_mng_name, '{}'::text[])
                                     || excluded.pad_us_mng_name) n),
      aliases         = (select array_agg(distinct a)
                         from unnest(public.operators.aliases || excluded.aliases) a),
      updated_at      = now()
    returning 1
  )
  select count(*) into v_count from ins;
  return v_count;
end $function$;


-- beach_coord_audit (filter)

CREATE OR REPLACE FUNCTION public.beach_coord_audit(p_state text DEFAULT NULL::text)
 RETURNS TABLE(fid bigint, name text, state text, county_name text, lat double precision, lon double precision, in_beach_polygon boolean, nearest_beach_polygon_m integer, nearest_waterway_m integer, quality text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with target as (
    select g.fid, coalesce(g.display_name_override, g.name) as name,
           g.state, g.county_name, g.lat, g.lon
      from public.beaches_gold g
     where g.is_active and (g.scoring_tier in ('daily','hourly')) and g.lat is not null
       and (p_state is null or g.state = p_state)
  ),
  pt as (
    select t.fid,
           st_setsrid(st_makepoint(t.lon, t.lat), 4326) as g,
           st_setsrid(st_makepoint(t.lon, t.lat), 4326)::geography as gg
      from target t
  ),
  in_poly as (
    select t.fid,
           exists(
             select 1 from public.osm_landing ol
              where ol.is_active and ol.tags->>'natural' = 'beach'
                and ol.geom_full is not null
                and st_geometrytype(ol.geom_full) in ('ST_Polygon','ST_MultiPolygon')
                and st_contains(ol.geom_full, pt.g)
           ) as inside
      from target t join pt using (fid)
  ),
  nearest_beach as (
    select t.fid,
           (select round(st_distance(pt.gg, ol.geom_full::geography))::int
              from public.osm_landing ol
             where ol.is_active and ol.tags->>'natural' = 'beach'
               and ol.geom_full is not null
             order by ol.geom_full::geography <-> pt.gg
             limit 1) as dist_m
      from target t join pt using (fid)
  ),
  nearest_water as (
    select t.fid,
           (select round(st_distance(pt.gg, ol.geom_full::geography))::int
              from public.osm_landing ol
             where ol.is_active
               and (ol.tags->>'waterway' in ('river','stream','canal')
                    or ol.tags->>'natural' in ('water','bay')
                    or (ol.tags ? 'water' and ol.tags->>'water' != 'no'))
               and ol.geom_full is not null
             order by ol.geom_full::geography <-> pt.gg
             limit 1) as dist_m
      from target t join pt using (fid)
  )
  select t.fid, t.name, t.state, t.county_name, t.lat, t.lon,
         ip.inside as in_beach_polygon,
         nb.dist_m as nearest_beach_polygon_m,
         nw.dist_m as nearest_waterway_m,
         case
           when ip.inside then 'in_beach_polygon'
           when nb.dist_m is not null and nb.dist_m <= 200 then 'near_beach_polygon'
           when nb.dist_m is not null and nb.dist_m <= 1000 then 'kind_of_close'
           when nw.dist_m is not null and nw.dist_m <= 200 then 'lake_or_river_beach'
           when nb.dist_m is not null and nb.dist_m <= 5000 then 'no_beach_polygon'
           else 'very_far'
         end as quality
    from target t
    join in_poly ip using (fid)
    join nearest_beach nb using (fid)
    join nearest_water nw using (fid)
   order by t.state, t.county_name, t.name;
$function$;


-- count_unattributed_beaches (filter)

CREATE OR REPLACE FUNCTION public.count_unattributed_beaches(p_state text DEFAULT NULL::text)
 RETURNS TABLE(state text, unattributed_count bigint, total_scoreable bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select g.state,
         count(*) filter (where not coalesce(s.is_attributed, false)) as unattributed_count,
         count(*) as total_scoreable
    from public.beaches_gold g
    left join public.beach_governance_status s on s.fid = g.fid
   where g.is_active
     and (g.scoring_tier in ('daily','hourly'))
     and (p_state is null or g.state = p_state)
   group by g.state
   order by unattributed_count desc;
$function$;


-- enqueue_missing_extractions (filter)

CREATE OR REPLACE FUNCTION public.enqueue_missing_extractions()
 RETURNS TABLE(dogs_enqueued bigint, park_url_enqueued bigint, queue_total bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
declare
  v_dogs bigint := 0;
  v_purl bigint := 0;
  v_total bigint := 0;
begin
  -- Beaches with no dogs BEP at all (need LLM extraction)
  with applied as (
    insert into public.beach_extraction_queue (fid, kind)
    select g.fid, 'dogs_extraction'
      from public.beaches_gold g
     where g.is_active and (g.scoring_tier in ('daily','hourly'))
       and not exists (select 1 from public.beach_enrichment_provenance b
                        where b.gold_fid = g.fid and b.field_group = 'dogs')
       and not exists (select 1 from public.beach_extraction_queue q
                        where q.fid = g.fid and q.kind = 'dogs_extraction'
                          and q.completed_at is null)
    on conflict (fid, kind) do nothing
    returning 1
  )
  select count(*) into v_dogs from applied;

  -- Beaches with no park_url BEP and no manual park_url (URL discovery candidates)
  with applied as (
    insert into public.beach_extraction_queue (fid, kind)
    select g.fid, 'park_url_discovery'
      from public.beaches_gold g
     where g.is_active and (g.scoring_tier in ('daily','hourly'))
       and not exists (select 1 from public.beach_enrichment_provenance b
                        where b.gold_fid = g.fid
                          and b.source in ('park_url','park_url_buffer_attribution'))
       and not exists (select 1 from public.beach_extraction_queue q
                        where q.fid = g.fid and q.kind = 'park_url_discovery'
                          and q.completed_at is null)
    on conflict (fid, kind) do nothing
    returning 1
  )
  select count(*) into v_purl from applied;

  select count(*) into v_total
    from public.beach_extraction_queue
   where completed_at is null;

  return query select v_dogs, v_purl, v_total;
end;
$function$;


-- find_alternatives_for_detail (filter)

CREATE OR REPLACE FUNCTION public.find_alternatives_for_detail(p_fid bigint, p_date date, p_max_km double precision DEFAULT 40, p_limit integer DEFAULT 5)
 RETURNS TABLE(fid bigint, location_id text, name text, distance_km double precision, has_on_leash boolean, has_off_leash boolean, day_status text, best_window_label text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with src as (
    select fid, geom from public.beaches_gold where fid = p_fid
  )
  select
    g.fid,
    g.location_id,
    coalesce(g.display_name_override, g.name) as name,
    st_distance(g.geom::geography, src.geom::geography) / 1000.0 as distance_km,
    bdp.has_on_leash,
    bdp.has_off_leash,
    dr.day_status,
    dr.best_window_label
  from public.beaches_gold g
  cross join src
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
  left join public.beach_day_recommendations dr
         on dr.arena_group_id = g.group_id and dr.local_date = p_date
  where g.is_active
    and (g.scoring_tier in ('daily','hourly'))
    and g.fid <> p_fid
    and (bdp.has_on_leash is true or bdp.has_off_leash is true)
    and st_dwithin(g.geom::geography, src.geom::geography, p_max_km * 1000)
  order by g.geom::geography <-> src.geom::geography
  limit greatest(p_limit, 1);
$function$;


-- find_distance_name_dup_pairs (filter)

CREATE OR REPLACE FUNCTION public.find_distance_name_dup_pairs(p_state text, p_max_distance_m integer DEFAULT 1500)
 RETURNS TABLE(f1 bigint, f2 bigint, lname text, county text, dist_m integer)
 LANGUAGE sql
 STABLE
AS $function$
  with active as (
    select fid, lower(coalesce(display_name_override, name)) as lname,
           county_name, state, lat, lon
      from public.beaches_gold
     where is_active and (scoring_tier in ('daily','hourly')) and lat is not null and state = p_state
  )
  select a.fid, b.fid, a.lname, a.county_name,
         round(st_distance(
           st_makepoint(a.lon, a.lat)::geography,
           st_makepoint(b.lon, b.lat)::geography
         ))::int as dist_m
    from active a join active b
      on a.lname = b.lname
     and a.county_name = b.county_name
     and a.fid < b.fid
   where st_distance(
           st_makepoint(a.lon, a.lat)::geography,
           st_makepoint(b.lon, b.lat)::geography
         ) < p_max_distance_m;
$function$;


-- get_no_photo_fids (filter)

CREATE OR REPLACE FUNCTION public.get_no_photo_fids(p_source text DEFAULT NULL::text)
 RETURNS TABLE(fid bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select g.fid
    from public.beaches_gold g
   where g.is_active
     and (g.scoring_tier in ('daily','hourly'))
     and not exists (
       select 1 from public.beach_photos bp
        where bp.arena_group_id = g.fid
          and (p_source is null or p_source = 'all' or bp.source = p_source)
     );
$function$;


-- list_unattributed_beaches (filter)

CREATE OR REPLACE FUNCTION public.list_unattributed_beaches(p_state text DEFAULT NULL::text)
 RETURNS TABLE(fid bigint, state text, name text, lat double precision, lon double precision)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select g.fid, g.state,
         coalesce(g.display_name_override, g.name) as name,
         g.lat, g.lon
    from public.beaches_gold g
    left join public.beach_governance_status s on s.fid = g.fid
   where g.is_active
     and (g.scoring_tier in ('daily','hourly'))
     and not coalesce(s.is_attributed, false)
     and (p_state is null or g.state = p_state)
   order by g.state, g.name;
$function$;


-- pad_us_units_by_mng_type_for_state (filter)

CREATE OR REPLACE FUNCTION public.pad_us_units_by_mng_type_for_state(p_state text, p_mng_types text[], p_beach_touching_only boolean DEFAULT false)
 RETURNS TABLE(unit_id bigint, mng_name text, mng_type text, unit_name text, loc_mang text, loc_own text, geom geometry)
 LANGUAGE sql
 STABLE
AS $function$
  select pu.unit_id::bigint,
         pu.mng_name::text,
         pu.mng_type::text,
         pu.unit_name::text,
         pu.loc_mang::text,
         pu.loc_own::text,
         pu.geom
    from public.pad_us_units pu
   where pu.state = p_state
     and pu.mng_type = ANY(p_mng_types)
     and (
       not p_beach_touching_only
       or exists (
         select 1 from public.beaches_gold g
          where g.state = p_state
            and g.is_active
            and (g.scoring_tier in ('daily','hourly'))
            and st_intersects(g.geom, pu.geom)
       )
     );
$function$;


-- populate_governance_from_polygon_gold_batch (filter)

CREATE OR REPLACE FUNCTION public.populate_governance_from_polygon_gold_batch(p_state text DEFAULT NULL::text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_fid bigint;
  v_total int := 0;
  v_n int;
begin
  for v_fid in
    select fid from public.beaches_gold
     where is_active and (scoring_tier in ('daily','hourly'))
       and (p_state is null or state = p_state)
  loop
    v_n := public.populate_governance_from_polygon_gold(v_fid);
    v_total := v_total + v_n;
  end loop;
  return v_total;
end;
$function$;


-- state_launcher_stats (filter)

CREATE OR REPLACE FUNCTION public.state_launcher_stats(p_state text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
declare
  v_state_fp text := case p_state
                       when 'CA' then '06'
                       when 'OR' then '41'
                       when 'WA' then '53'
                     end;
  v_arena bigint;
  v_arena_unpromoted bigint;
  v_gold bigint;
  v_gold_scoreable bigint;
  v_gold_with_bep bigint;
  v_gold_with_governance bigint;
  v_bep bigint;
  v_queue_pending bigint;
  v_queue_completed bigint;
  v_queue_total bigint;
  v_pad_us bigint;
  v_noaa bigint;
  v_worst_km numeric;
begin
  select count(*) into v_arena
    from public.arena a
   where a.is_active = true and a.county_fips is not null
     and exists (select 1 from public.counties c
                  where c.geoid = a.county_fips and c.state_fp = v_state_fp);

  select count(*) into v_arena_unpromoted
    from public.arena a
   where a.is_active = true and a.county_fips is not null
     and exists (select 1 from public.counties c
                  where c.geoid = a.county_fips and c.state_fp = v_state_fp)
     and not exists (select 1 from public.beaches_gold g where g.fid = a.fid);

  select count(*), count(*) filter (where (scoring_tier in ('daily','hourly')))
    into v_gold, v_gold_scoreable
    from public.beaches_gold where state = p_state and is_active = true;

  select count(*) into v_bep
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
   where g.state = p_state and g.is_active = true;

  select count(distinct bep.gold_fid) into v_gold_with_bep
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
   where g.state = p_state and g.is_active = true;

  select count(distinct bep.gold_fid) into v_gold_with_governance
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
   where g.state = p_state and g.is_active = true and bep.field_group = 'governance';

  select count(*) filter (where q.completed_at is null),
         count(*) filter (where q.completed_at is not null),
         count(*)
    into v_queue_pending, v_queue_completed, v_queue_total
    from public.beach_extraction_queue q
    join public.beaches_gold g on g.fid = q.fid
   where g.state = p_state;

  select count(*) into v_pad_us
    from public.pad_us_units where state = p_state;

  select count(*) into v_noaa
    from public.noaa_stations where state = p_state;

  select round(max(st_distance(g.geom::geography, n.geom::geography))::numeric / 1000, 1)
    into v_worst_km
    from public.beaches_gold g
    join public.noaa_stations n on n.station_id = g.noaa_station_id
   where g.state = p_state and g.is_active = true;

  return jsonb_build_object(
    'state',                 p_state,
    'arena_in_state',        v_arena,
    'arena_unpromoted',      v_arena_unpromoted,
    'gold_rows',             v_gold,
    'gold_scoreable',        v_gold_scoreable,
    'gold_with_bep',         v_gold_with_bep,
    'gold_with_governance',  v_gold_with_governance,
    'bep_rows',              v_bep,
    'queue_pending',         v_queue_pending,
    'queue_completed',       v_queue_completed,
    'queue_total',           v_queue_total,
    'pad_us_units',          v_pad_us,
    'noaa_stations',         v_noaa,
    'worst_noaa_km',         v_worst_km,
    'as_of',                 now()
  );
end;
$function$;


-- inactivate_tier3_beaches (filter+setter)

CREATE OR REPLACE FUNCTION public.inactivate_tier3_beaches(p_state text DEFAULT NULL::text)
 RETURNS TABLE(deactivated_count bigint, fids bigint[])
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
declare v_count bigint := 0; v_fids bigint[];
begin
  with picks as (
    select g.fid
      from public.beaches_gold g
      join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
     where g.is_active = true
       and bdp.dogs_allowed = 'no'
       and (p_state is null or g.state = p_state)
  ),
  upd as (
    update public.beaches_gold g
       set is_active = false
      from picks
     where g.fid = picks.fid
    returning g.fid
  )
  select count(*), array_agg(fid) into v_count, v_fids from upd;
  return query select v_count, coalesce(v_fids, '{}'::bigint[]);
end;
$function$;


-- promote_to_gold (remove_setter)

CREATE OR REPLACE FUNCTION public.promote_to_gold(p_fids bigint[], p_score boolean DEFAULT false, p_publish boolean DEFAULT true)
 RETURNS TABLE(rows_promoted bigint, rows_already_in_gold bigint, containment_evidence bigint, source_evidence bigint, beach_dog_policy_changed bigint, beach_amenities_changed bigint, noaa_assigned bigint, slugs_assigned bigint)
 LANGUAGE plpgsql
AS $function$
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
       and not exists (
         select 1 from public.beaches_gold g
           join public.arena a2 on a2.fid = g.fid
          where a2.group_id = coalesce(a.group_id, a.fid)
            and g.is_active and g.fid <> a.fid
       )
  ),
  to_promote_with_canonical as (
    select t.*,
           coalesce(osm.nav_lat, t.nav_lat, t.lat) as canonical_lat,
           coalesce(osm.nav_lon, t.nav_lon, t.lon) as canonical_lon
      from to_promote t
      left join lateral (
        select nav_lat, nav_lon
          from public.arena
         where group_id = coalesce(t.group_id, t.fid)
           and source_code = 'osm' and is_active = true
           and nav_lat is not null and nav_lon is not null
         limit 1
      ) osm on true
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name, county_fips,
       source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source, park_name, state, promoted_from, promoted_at,
       is_active, noaa_station_id, timezone, open_time, close_time, geom)
    select t.fid, public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.canonical_lat, t.canonical_lon, t.county_name, t.county_fips,
      t.source_code, t.source_id, coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source, t.park_name,
      coalesce(
        public._infer_state_from_county_fips(t.county_fips),
        public._infer_state_from_county(t.county_name)
      ),
      'promote_to_gold_v3', now(), true,
      public._nearest_noaa_station(st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)::geography),
      'America/Los_Angeles', '05:00', '22:00',
      st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)
    from to_promote_with_canonical t
    returning fid, location_id, noaa_station_id
  )
  select array_agg(fid), count(*), count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into new_fids, promoted, slug_count, noaa_count from ins;
  existing := array_length(p_fids, 1) - promoted;

  foreach fid_iter in array p_fids loop
    perform public.populate_polygon_containment_gold(fid_iter);
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_pad_us_gold(fid_iter);
    perform public.populate_from_park_operators_gold(fid_iter);
    perform public.populate_from_operators_gold(fid_iter);
    perform public.populate_from_state_default_gold(fid_iter);  -- â˜… RESTORED
    perform public.populate_from_research_gold(fid_iter);
    perform public.populate_from_park_url_gold(fid_iter);
    perform public.populate_from_park_url_governance_gold(fid_iter);
    perform public.populate_from_unified_v1_gold(fid_iter);
    perform public.populate_from_city_dog_policy_gold(fid_iter);
    perform public.populate_from_county_dog_policy_gold(fid_iter);
    perform public._emit_evidence_from_osm_amenities(fid_iter);

    perform public._resolve_polygon_containment(fid_iter);
    perform public._resolve_governance_gold(fid_iter);
    perform public._resolve_dogs_gold(fid_iter);
    perform public._resolve_practical_gold(fid_iter);
    perform public._resolve_field_group_gold('access', fid_iter);
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
$function$;


-- run_distance_name_dedup (remove_setter)

CREATE OR REPLACE FUNCTION public.run_distance_name_dedup(p_state text, p_max_distance_m integer DEFAULT 1500)
 RETURNS TABLE(pairs_found bigint, kills bigint, photos_migrated bigint, bep_migrated bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET statement_timeout TO '300s'
 SET search_path TO 'public', 'pg_catalog'
AS $function$
declare
  v_pairs bigint := 0;
  v_kills bigint := 0;
  v_photos_migrated bigint := 0;
  v_bep_migrated bigint := 0;
  v_tmp bigint;
  rec record;
begin
  -- Build candidate pairs into a temp table
  create temp table _dist_pairs on commit drop as
  select * from public.find_distance_name_dup_pairs(p_state, p_max_distance_m);

  v_pairs := (select count(*) from _dist_pairs);

  -- Score each fid involved and decide winner per pair.
  -- Skip pairs where either side is curator/manual-protected.
  for rec in
    with scored as (
      select p.f1, p.f2,
             public._dedup_richness_score(p.f1) s1,
             public._dedup_richness_score(p.f2) s2,
             exists(select 1 from public.beach_location_curation_status s
                     where s.arena_group_id in (p.f1, p.f2)
                       and (s.was_moved or s.needs_review)) any_curator,
             exists(select 1 from public.arena a
                     where a.fid in (p.f1, p.f2) and a.source_code='manual') any_manual
        from _dist_pairs p
    )
    select case when s1 >= s2 then f1 else f2 end as winner_fid,
           case when s1 >= s2 then f2 else f1 end as loser_fid
      from scored where not any_curator and not any_manual
  loop
    -- BEP migrate (skip on conflict)
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, cpad_role)
    select rec.winner_fid, bep.field_group, bep.source, bep.confidence,
           bep.claimed_values, bep.source_url,
           bep.cpad_unit_name, bep.extraction_type, bep.cpad_role
      from public.beach_enrichment_provenance bep
     where bep.gold_fid = rec.loser_fid
     on conflict (gold_fid, field_group, source) where gold_fid is not null
     do nothing;
    get diagnostics v_tmp = row_count;
    v_bep_migrated := v_bep_migrated + v_tmp;

    -- Promote curated metadata on conflicting photo rows
    update public.beach_photos w
       set curated_at    = l.curated_at,
           curated_by    = l.curated_by,
           sort_order    = l.sort_order,
           match_quality = coalesce(l.match_quality, w.match_quality)
      from public.beach_photos l
     where l.arena_group_id = rec.loser_fid and w.arena_group_id = rec.winner_fid
       and l.source = w.source and l.external_id = w.external_id
       and l.curated_at is not null and w.curated_at is null;

    -- Migrate non-conflicting loser photos, recompute distance vs winner
    update public.beach_photos bp
       set arena_group_id = rec.winner_fid,
           distance_m = round(st_distance(
             st_makepoint(bp.lng, bp.lat)::geography,
             (select st_makepoint(lon, lat)::geography
                from public.beaches_gold where fid = rec.winner_fid)
           ))::int
     where bp.arena_group_id = rec.loser_fid
       and not exists (
         select 1 from public.beach_photos p2
          where p2.arena_group_id = rec.winner_fid
            and p2.source = bp.source
            and p2.external_id = bp.external_id
       );
    get diagnostics v_tmp = row_count;
    v_photos_migrated := v_photos_migrated + v_tmp;

    -- Delete remaining (conflict) loser photos
    delete from public.beach_photos where arena_group_id = rec.loser_fid;

    -- Reset curation_status: drop both, re-insert winner if it has any curated photos
    delete from public.beach_curation_status
     where arena_group_id in (rec.winner_fid, rec.loser_fid);
    insert into public.beach_curation_status (arena_group_id, curated_at)
    select rec.winner_fid, max(curated_at) from public.beach_photos
     where arena_group_id = rec.winner_fid and curated_at is not null
    having max(curated_at) is not null;

    -- Deactivate loser
    update public.beaches_gold
       set is_active = false,
           inactive_reason = 'dupe_of_' || rec.winner_fid::text
     where fid = rec.loser_fid and is_active = true;

    v_kills := v_kills + 1;
  end loop;

  return query select v_pairs, v_kills, v_photos_migrated, v_bep_migrated;
end;
$function$;


-- tg_inactivate_on_no_dogs (remove_setter)

CREATE OR REPLACE FUNCTION public.tg_inactivate_on_no_dogs()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
  if NEW.dogs_allowed = 'no' then
    update public.beaches_gold
       set is_active = false
     where fid = NEW.arena_group_id and is_active = true;
  end if;
  return NEW;
end;
$function$;


-- get_beach_info (remove_returned_key)

CREATE OR REPLACE FUNCTION public.get_beach_info(p_fid bigint)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with selected_photos as (
    select p.source, p.image_url, p.thumb_url, p.attribution, p.license,
           p.page_url, p.captured_at, p.curated_at,
           p.sort_order, p.id,
           coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) keep_prob
      from public.beach_photos p
     where p.arena_group_id = p_fid
       and (
         p.curated_at is not null
         -- or p.source = 'cdpr'   -- UNCOMMENT FOR LAUNCH (option C)
         or coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) >= 0.65
       )
     order by
       (p.curated_at is null)::int asc,        -- curated (false=0) first
       -- case when p.source='cdpr' then 0 else 1 end,  -- UNCOMMENT FOR LAUNCH
       coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) desc,
       p.sort_order asc,
       p.id asc
     limit 6
  )
  select jsonb_build_object(
    'beach', jsonb_build_object(
      'fid',           g.fid,
      'location_id',   g.location_id,
      'name',          g.name,
      'display_name',  coalesce(g.display_name_override, g.name),
      'county',        g.county_name,
      'state',         g.state,
      'address',       g.address,
      'website',       g.website,
      'timezone',      g.timezone,
      'lat',           st_y(g.geom),
      'lng',           st_x(g.geom),
      'geom',          st_asgeojson(g.geom)::jsonb,
      'open_time',     g.open_time,
      'close_time',    g.close_time
      ),
    'dog_policy', case when bdp.arena_group_id is not null then
      jsonb_build_object(
        'dogs_allowed',           bdp.dogs_allowed,
        'leash_policy',           bdp.leash_policy,
        'has_off_leash',          bdp.has_off_leash,
        'has_on_leash',           bdp.has_on_leash,
        'access_rule',            bdp.access_rule,
        'off_leash_flag',         bdp.off_leash_flag,
        'dogs_prohibited_start',  bdp.dogs_prohibited_start,
        'dogs_prohibited_end',    bdp.dogs_prohibited_end,
        'zone_rules',             bdp.zone_rules,
        'notes',                  bdp.notes
      )
    end,
    'amenities', case when ba.arena_group_id is not null then
      to_jsonb(ba) - 'arena_group_id'
    end,
    'description', case when bd.arena_group_id is not null then
      jsonb_build_object(
        'text', bd.description,
        'generated_at', bd.generated_at
      )
    end,
    'photos', coalesce((
      select jsonb_agg(jsonb_build_object(
        'source',      sp.source,
        'image_url',   sp.image_url,
        'thumb_url',   sp.thumb_url,
        'attribution', sp.attribution,
        'license',     sp.license,
        'page_url',    sp.page_url,
        'captured_at', sp.captured_at,
        'curated',     sp.curated_at is not null,
        'keep_prob',   sp.keep_prob
      ) order by (sp.curated_at is null)::int, sp.keep_prob desc, sp.sort_order, sp.id)
      from selected_photos sp
    ), '[]'::jsonb)
  )
  from public.beaches_gold g
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
  left join public.beach_amenities  ba  on ba.arena_group_id  = g.group_id
  left join public.beach_descriptions bd on bd.arena_group_id = g.fid
  where g.fid = p_fid and g.is_active
  limit 1;
$function$;


-- tg_fire_refresh_on_score_flip (fire_on_tier)

CREATE OR REPLACE FUNCTION public.tg_fire_refresh_on_score_flip()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
begin
  -- Defensive: never fire on inactive rows (would just no-op anyway since
  -- daily-beach-refresh skips (scoring_tier is null or scoring_tier = 'none') beaches).
  if NEW.is_active is true and (NEW.scoring_tier in ('daily','hourly')) is true then
    perform public._fire_daily_beach_refresh_for_fid(NEW.fid);
  end if;
  return NEW;
end $function$;


-- tg_backfill_scoring_metadata_for_socal (fire_on_tier)
-- 2026-05-13 cutover: scoring_tier is NULL at BEFORE-INSERT time (set
-- later by refresh_scoring_tier()), so the original is_scoreable gate
-- can't be replaced with a scoring_tier check. Drop the gate entirely
-- — backfill always runs when NOAA / timezone are missing. Cheap.

CREATE OR REPLACE FUNCTION public.tg_backfill_scoring_metadata_for_socal()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare
  donor record;
  nearest_station text;
begin
  if NEW.noaa_station_id is not null and NEW.timezone is not null then
    return NEW;
  end if;

  -- Tier 1: nearest scoreable neighbor in same county
  select d.noaa_station_id, d.besttime_venue_id, d.timezone,
         d.open_time, d.close_time
    into donor
    from public.beaches_gold d
   where d.fid <> NEW.fid
     and d.county_name = NEW.county_name
     and (d.scoring_tier in ('daily','hourly'))
     and d.noaa_station_id is not null
     and exists (
       select 1 from public.noaa_stations s
        where s.station_id = d.noaa_station_id
          and s.reference_id is null   -- harmonic only
     )
     and NEW.geom is not null and d.geom is not null
   order by d.geom <-> NEW.geom limit 1;

  if found then
    NEW.noaa_station_id   := coalesce(NEW.noaa_station_id,   donor.noaa_station_id);
    NEW.besttime_venue_id := coalesce(NEW.besttime_venue_id, donor.besttime_venue_id);
    NEW.timezone          := coalesce(NEW.timezone,          donor.timezone);
    NEW.open_time         := coalesce(NEW.open_time,         donor.open_time);
    NEW.close_time        := coalesce(NEW.close_time,        donor.close_time);
    return NEW;
  end if;

  -- Tier 2: spatial nearest harmonic NOAA station
  if NEW.noaa_station_id is null and NEW.geom is not null then
    select s.station_id
      into nearest_station
      from public.noaa_stations s
     where s.state = 'CA'
       and s.reference_id is null    -- harmonic only
       and s.geom is not null
     order by s.geom <-> NEW.geom limit 1;
    NEW.noaa_station_id := nearest_station;
  end if;

  if NEW.timezone is null  then NEW.timezone  := 'America/Los_Angeles'; end if;
  if NEW.open_time is null  then NEW.open_time  := '05:00'; end if;
  if NEW.close_time is null then NEW.close_time := '22:00'; end if;
  return NEW;
end $function$;


-- 5a. Replace tg_recompute_scoreable_on_dog_policy with a scoring_tier
--     equivalent. When beach_dog_policy changes, the beach's tier may
--     shift (e.g. leash data finally arrives → tier promotes from
--     unknown→2_on-leash, so scoring_tier flips none→daily). Without
--     this, downstream daily-beach-refresh won't know to start scoring.
create or replace function public.tg_refresh_scoring_tier_on_dog_policy()
returns trigger
language plpgsql
security definer
set search_path to 'public'
as $function$
begin
  perform public.refresh_scoring_tier(NEW.arena_group_id);
  return NEW;
end $function$;

create trigger tg_after_change_refresh_scoring_tier
  after insert or update on public.beach_dog_policy
  for each row
  execute function public.tg_refresh_scoring_tier_on_dog_policy();

-- 5b. Recreate the refresh-fire triggers on scoring_tier instead of is_scoreable.
create trigger tg_beaches_gold_score_on_tier_flip
  after update of scoring_tier on public.beaches_gold
  for each row
  when (old.scoring_tier is distinct from new.scoring_tier
        and new.scoring_tier in ('daily','hourly'))
  execute function public.tg_fire_refresh_on_score_flip();

create trigger tg_beaches_gold_score_on_insert
  after insert on public.beaches_gold
  for each row
  when (new.scoring_tier in ('daily','hourly'))
  execute function public.tg_fire_refresh_on_score_flip();

commit;

-- Verify (informational):
--   SELECT scoring_tier, count(*) FROM public.beaches_gold
--    WHERE is_active GROUP BY 1 ORDER BY 1;
