-- 20260530_demote_county_code_in_city_beaches.sql
--
-- Demotes 928 beach_policy_source rows attributing a county-level
-- municipal_code policy_source to beaches whose geom is inside an
-- incorporated city polygon. Per Franz 2026-05-30 PM after auditing
-- La Jolla Shores (fid 8347) and finding wrong "on-leash all day"
-- attribution sourced from San Diego County Code §62.669 (which
-- applies only to unincorporated San Diego County, not to City of
-- San Diego territory).
--
-- Scope (sized by audit):
--   223 beaches across 4 states (90 CA + 111 WA + 20 OR + 2 MD)
--   565 beach × section pairs
--   928 BPS rows total (some sections have multiple county-attributed
--                       sources stacked — tier 1 + tier 3)
--   37 distinct county-level policy_source rows are the culprits
--
-- Root cause: derive_policy_source_for_jurisdiction.py:2020-2038
-- attaches county-code BPS rows via `ST_Intersects(b.geom, p.geom)`
-- where p is the COUNTY polygon. It does NOT check whether the beach
-- is also inside an incorporated city polygon (which would preempt
-- county code per municipal home-rule).
--
-- This migration is data-cleanup only. The pipeline fix lands in a
-- separate patch to the codify script + a pin documenting the rule.
--
-- After demotion:
--   * 353/565 pairs (62%) have a city-level BPS row that promotes
--     naturally on the next cascade run — clean fix.
--   * 212/565 pairs (38%) have no city alternative — section drops
--     to no canonical source. Consumer surface shows nothing for
--     that section until a city-codify run fills the gap. This is
--     ACCEPTED per Franz: blank > wrong.
--
-- Reversible: all rows updated retain the `inapplicable_jurisdiction`
-- status; revert via `UPDATE ... SET operative_status='operative'
-- WHERE operative_status='inapplicable_jurisdiction'`.

begin;

-- 1. Add the new enum value
alter type public.operative_status add value if not exists 'inapplicable_jurisdiction';

commit;

-- pg requires the enum addition to be in its own transaction before
-- being usable in subsequent statements.
begin;

-- 2. Mass demote the misattributed BPS rows
with city_beaches as (
  select distinct bg.fid
    from public.beaches_gold bg
    join public.jurisdictions j on st_contains(j.geom, bg.geom)
   where bg.is_active
     and j.funcstat = 'A'
     and j.place_type like 'C%'
),
demote_ids as (
  select bps.id
    from public.beach_policy_source bps
    join public.policy_source ps on ps.id = bps.policy_source_id
   where bps.operative_status = 'operative'
     and bps.beach_fid in (select fid from city_beaches)
     and ps.citation ~* '\m(county code|county ordinance|county §|county ord)\M'
)
update public.beach_policy_source
   set operative_status = 'inapplicable_jurisdiction',
       status_note      = coalesce(status_note, '') ||
         case when status_note is null or status_note = '' then '' else ' | ' end ||
         '2026-05-30 demoted: county code attributed to incorporated-city beach (no preemption check at codify time). See [[codify-city-preemption]] pin.'
 where id in (select id from demote_ids);

-- 3. Re-promote zone_rules across affected beaches so the consumer
--    surface reflects the demotion.
do $$
declare
  r record;
  n int := 0;
begin
  for r in
    with city_beaches as (
      select distinct bg.fid
        from public.beaches_gold bg
        join public.jurisdictions j on st_contains(j.geom, bg.geom)
       where bg.is_active
         and j.funcstat = 'A'
         and j.place_type like 'C%'
    )
    select distinct bps.beach_fid as fid
      from public.beach_policy_source bps
     where bps.operative_status = 'inapplicable_jurisdiction'
       and bps.status_note like '%[[codify-city-preemption]]%'
       and bps.beach_fid in (select fid from city_beaches)
  loop
    perform public._promote_zone_rules_for_fid(r.fid);
    n := n + 1;
  end loop;
  raise notice 'Re-promoted % beaches', n;
end $$;

commit;
