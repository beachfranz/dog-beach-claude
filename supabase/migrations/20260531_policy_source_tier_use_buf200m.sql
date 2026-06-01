-- 20260531_policy_source_tier_use_buf200m.sql
--
-- Substitute the 0.0018° (~200m) ST_DWithin in policy_source_effective_tier_for_beach's
-- city-preemption check with ST_Contains against the new jurisdictions_buf200m
-- buffered layer (200m geographic buffer pre-applied + GIST-indexed).
--
-- Same set of matches as before — both expressions catch beaches within
-- 200m of an active incorporated city polygon — but the buffered layer
-- is faster (GIST short-circuits ST_Contains directly) and cleaner (no
-- magic-constant degree threshold).
--
-- See [[pip-for-places-uses-200m]] pin for context; coastal-edge city
-- beaches like fid 8347 La Jolla Shores (12.4m outside strict City of
-- San Diego polygon) need this guard so county code rules get demoted
-- when the beach is really city territory.

CREATE OR REPLACE FUNCTION public.policy_source_effective_tier_for_beach(p_ps_id bigint, p_beach_fid bigint)
 RETURNS smallint
 LANGUAGE sql
 STABLE PARALLEL SAFE
AS $function$
  with ps_row as (
    select id, subtype, issuing_operator_id, citation
      from public.policy_source
     where id = p_ps_id
  ),
  beach_row as (
    select fid, geom, state
      from public.beaches_gold
     where fid = p_beach_fid
  ),
  -- Operator-posted policy for the beach's actual operator wins tier 0
  operator_match as (
    select 1 as hit
      from ps_row p, public.entity_operator eo
     where p.subtype = 'operator_posted_policy'
       and p.issuing_operator_id is not null
       and eo.entity_type = 'beach'
       and eo.entity_id   = p_beach_fid
       and eo.operator_id = p.issuing_operator_id
  ),
  -- City-preemption check: county-scope ps + beach in/near incorporated city
  -- = tier 99 sentinel (below any real source so cascade ORDER BY tier ASC skips it).
  -- Uses jurisdictions_buf200m (pre-buffered 200m TIGER places) per
  -- [[pip-for-places-uses-200m]].
  -- Lake Anza fid 8534 preserved as explicit exception.
  city_preemption as (
    select 1 as hit
      from ps_row p, beach_row b
     where p.subtype in ('municipal_code', 'agency_administrative_policy')
       and p.citation ~  '^[^,(]*\mCounty\M'
       and p.citation !~ '^(City|Town|Village|Borough)\M'
       and b.fid <> 8534
       and exists (
         select 1
           from public.jurisdictions_buf200m jb_city
           join public.jurisdictions j_city on j_city.id = jb_city.id
          where j_city.state = b.state
            and j_city.funcstat = 'A'
            and j_city.place_type like 'C%'
            and st_contains(jb_city.geom, b.geom)
       )
  )
  select case
    when exists (select 1 from operator_match)  then 0::smallint
    when exists (select 1 from city_preemption) then 99::smallint
    else public.policy_source_authority((select subtype from ps_row))
  end;
$function$;
