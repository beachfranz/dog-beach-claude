-- 20260530_cascade_side_city_preemption.sql
--
-- Second-layer defense for [[codify-city-preemption]]. The codify script
-- patch (derive_policy_source_for_jurisdiction.py:2020-2080) prevents
-- NEW county-scope BPS rows from being attached to city-territory beaches.
-- This migration adds a CASCADE-side check so that even if a county-scope
-- BPS row sneaks through (manual insert, legacy migration, future
-- mistake), the tier function recognizes it as inapplicable and demotes
-- it below any real source.
--
-- Mechanism: extend `policy_source_effective_tier_for_beach` to detect
-- "ps is county-scope" + "beach is in or within 200m of an active
-- incorporated-city TIGER polygon" → return tier 99 (sentinel; below
-- any real authority tier). The cascade's `ORDER BY tier ASC` then
-- pushes those rows behind any city-jurisdiction alternative.
--
-- County-scope detection: subtype IN ('municipal_code', 'agency_administrative_policy')
-- AND citation matches `^[^,(]*\bCounty\b` (county is the issuing
-- authority at the start of the citation, not a parenthetical
-- "adoption-by-reference"), AND NOT prefixed by City/Town/Village.
-- Same regex as the demote pass; see [[codify-city-preemption]] pin.
--
-- City detection: 200m ST_DWithin against jurisdictions where
-- funcstat='A' AND place_type LIKE 'C%'. 200m covers TIGER coastal-edge
-- (polygon stops at high-water line, beach pins sit ~10-200m on water
-- side). See [[pip-for-places-uses-200m]] pin.
--
-- Lake Anza fid 8534 is preserved as the explicit exception via the
-- same fid<>8534 carve-out used in the demote migrations.

begin;

create or replace function public.policy_source_effective_tier_for_beach(
  p_ps_id bigint,
  p_beach_fid bigint
)
returns smallint
language sql
stable parallel safe
as $$
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
  -- Lake Anza fid 8534 preserved as explicit exception.
  city_preemption as (
    select 1 as hit
      from ps_row p, beach_row b
     where p.subtype in ('municipal_code', 'agency_administrative_policy')
       and p.citation ~  '^[^,(]*\mCounty\M'
       and p.citation !~ '^(City|Town|Village|Borough)\M'
       and b.fid <> 8534
       and exists (
         select 1 from public.jurisdictions j_city
          where j_city.state = b.state
            and j_city.funcstat = 'A'
            and j_city.place_type like 'C%'
            and st_dwithin(j_city.geom, b.geom, 0.0018)   -- ~200m
       )
  )
  select case
    when exists (select 1 from operator_match)  then 0::smallint
    when exists (select 1 from city_preemption) then 99::smallint
    else public.policy_source_authority((select subtype from ps_row))
  end;
$$;

comment on function public.policy_source_effective_tier_for_beach(bigint, bigint) is
  'Returns the effective authority tier of a policy_source for a specific beach. '
  'Tier 0 = operator-posted for this beach''s operator (top of cascade). '
  'Tier 99 = county-scope ps applied to a beach in/near an incorporated city (sentinel; below any real source). '
  'Otherwise: policy_source_authority(subtype). '
  'See pins [[codify-city-preemption]] and [[pip-for-places-uses-200m]].';

commit;
