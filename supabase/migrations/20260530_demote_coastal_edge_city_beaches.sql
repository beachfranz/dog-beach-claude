-- 20260530_demote_coastal_edge_city_beaches.sql
--
-- Second pass on the [[codify-city-preemption]] cleanup. The first pass
-- (20260530_demote_county_code_in_city_beaches.sql) used strict
-- ST_Contains and demoted 928 BPS rows for 223 beaches geographically
-- inside an incorporated city polygon.
--
-- That under-counted: TIGER place polygons for coastal cities stop at
-- the high-water line, so coastal beach pins (~12m–200m offshore of the
-- polygon's seaward edge, even though they're on the actual beach sand
-- ON the map) were missed. La Jolla Shores fid 8347 is the canonical
-- example — 12.4m outside the City of San Diego polygon but obviously
-- a San Diego City beach. Same issue for Mission/Pacific/Hollywood/etc.
--
-- This migration catches the remaining coastal-edge cases by using a
-- 200m ST_DWithin buffer instead of strict containment.
--
-- One explicit exclusion: fid 8534 (Lake Anza Beach in Tilden Regional
-- Park, EBRPD-managed). Berkeley is the nearest TIGER polygon (~147m
-- away) but Lake Anza is genuinely outside Berkeley's jurisdiction —
-- it's regional-park territory where county code IS the right
-- backstop. Demoting it would leave the section without a source.
--
-- Per Franz 2026-05-30 PM after eyeballing the 200m candidate list.

begin;

with city_or_coastal_edge_beaches as (
  select distinct bg.fid
    from public.beaches_gold bg
    join public.jurisdictions j
      on st_dwithin(j.geom, bg.geom, 0.0018)   -- 200m in degrees
   where bg.is_active
     and j.funcstat  = 'A'
     and j.place_type like 'C%'
     and bg.fid <> 8534   -- Lake Anza (EBRPD Tilden Regional Park)
),
demote_ids as (
  select bps.id
    from public.beach_policy_source bps
    join public.policy_source ps on ps.id = bps.policy_source_id
   where bps.operative_status = 'operative'
     and bps.beach_fid in (select fid from city_or_coastal_edge_beaches)
     and ps.citation ~* '\m(county code|county ordinance|county §|county ord)\M'
)
update public.beach_policy_source
   set operative_status = 'inapplicable_jurisdiction',
       status_note      = coalesce(status_note, '') ||
         case when status_note is null or status_note = '' then '' else ' | ' end ||
         '2026-05-30 demoted (coastal-edge 200m pass): county code attributed to beach within 200m of incorporated-city polygon (TIGER coastal-edge correction). See [[codify-city-preemption]] pin.'
 where id in (select id from demote_ids);

-- Re-promote affected beaches so consumer surface reflects the change.
do $$
declare
  r record;
  n int := 0;
begin
  for r in
    select distinct bps.beach_fid as fid
      from public.beach_policy_source bps
     where bps.operative_status = 'inapplicable_jurisdiction'
       and bps.status_note like '%coastal-edge 200m pass%'
  loop
    perform public._promote_zone_rules_for_fid(r.fid);
    n := n + 1;
  end loop;
  raise notice 'Re-promoted % coastal-edge beaches', n;
end $$;

commit;
