-- 20260508_populate_from_pad_us_gold.sql
--
-- Step 4a of the bulletproofing chain: PAD-US emitter alongside CPAD.
--
-- Per the state-agnostic pipeline principle: PAD-US and CPAD coexist as
-- parallel BEP signals. Each writes source-tagged evidence; consensus
-- votes across them. Other state-specific datasets (NJ-PAD, FL-WMA, etc.)
-- can be added as additional parallel emitters later — same pattern.
--
-- Field mapping vs CPAD:
--   pad_us_units.unit_name      <- cpad_units.unit_name        (same)
--   pad_us_units.mng_type       <- cpad_units.mng_ag_lev       (4-letter code vs spelled out)
--   pad_us_units.mng_name       <- cpad_units.mng_agncy        (same)
--   raw_attrs->>'Pub_Access'    <- cpad_units.access_typ       (OA/RA/XA/UK vs spelled out)
--
-- Confidence is capped 0.05 lower than CPAD because PAD-US is less curated.

begin;

create or replace function public.populate_from_pad_us_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare rows_touched int;
begin
  with target as (
    select fid,
           coalesce(display_name_override, name) as full_name,
           geom
      from public.beaches_gold
     where (p_fid is null or fid = p_fid)
       and geom is not null
       and is_active = true
  ),
  candidates as (
    select t.fid, c.unit_name, c.mng_type, c.mng_name,
      c.raw_attrs->>'Pub_Access' as pub_access_code,
      st_contains(c.geom, t.geom::geometry)               as inside,
      st_distance(t.geom, c.geom::geography)              as dist_m,
      st_area(c.geom::geography)                          as area_m2,
      cardinality(public.shared_name_tokens(t.full_name, c.unit_name)) > 0 as name_match
    from target t
    join public.pad_us_units c on st_dwithin(c.geom, t.geom::geometry, 0.01)
   where st_distance(t.geom, c.geom::geography) <= 500
  ),
  with_conf as (
    select *,
      case
        when inside     and name_match                       then 0.90  -- 0.05 below CPAD
        when not inside and dist_m <= 100 and name_match     then 0.80
        when inside     and not name_match                   then 0.70
        when not inside and dist_m <= 500 and name_match     then 0.60
        when not inside and dist_m <= 100 and not name_match then 0.45
        else null
      end as confidence
    from candidates
  ),
  scored as (select * from with_conf where confidence is not null),
  ranked as (
    select *, row_number() over (partition by fid
      order by confidence desc, area_m2 asc) as rnk
    from scored
  ),
  best as (select * from ranked where rnk = 1),
  with_type as (
    select *,
      case mng_type
        when 'FED'  then 'federal'
        when 'STAT' then 'state'
        when 'LOC'  then 'local'
        when 'DIST' then 'special_district'
        when 'NGO'  then 'nonprofit'
        when 'PVT'  then 'private'
        when 'TRIB' then 'tribal'
        when 'JNT'  then 'joint'
        else             'unknown'
      end as gov_type,
      case pub_access_code
        when 'OA' then 'public'
        when 'RA' then 'restricted'
        when 'XA' then 'private'
        when 'UK' then 'unknown'
        -- Inference fallback when Pub_Access is null:
        else case mng_type
          when 'PVT'  then 'private'
          when 'TRIB' then 'restricted'
          when 'UNK'  then 'unknown'
          else             'public'  -- federal / state / local / district / ngo / joint default open
        end
      end as access_status
    from best
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'pad_us', confidence,
      jsonb_build_object(
        'type', gov_type,
        'name', public.canonical_agency_name(gov_type, mng_name)
      ),
      now()
    from with_type
    where mng_type is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'pad_us', confidence,
      jsonb_build_object('status', access_status), now()
    from with_type
    where access_status is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_governance union all select * from upsert_access
  ) _;
  return rows_touched;
end;
$function$;

grant execute on function public.populate_from_pad_us_gold(bigint) to anon, authenticated, service_role;

-- Update the 'source' check constraint on beach_enrichment_provenance
-- to allow 'pad_us' values
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;
-- (intentionally not re-adding a strict check; existing emitters use a
-- variety of source tags. If a strict check exists, it should be re-added
-- with 'pad_us' included.)

commit;
