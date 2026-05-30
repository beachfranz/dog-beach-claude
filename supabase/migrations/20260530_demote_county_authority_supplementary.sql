-- 20260530_demote_county_authority_supplementary.sql
--
-- Third pass on [[codify-city-preemption]]. The first two passes used
-- a narrow regex matching `(County Code|County Ordinance|County §|County Ord)`
-- which caught tier-1 county municipal_code rows but missed tier-3
-- supplementary citations like:
--   "San Diego County Department of Animal Services — Pet Ownership Laws"
--   "Anne Arundel County Recreation & Parks — Swimming Beaches Rules"
--   "Wicomico County, MD Code Ch 133 (Animal Control)"
--   "LA County Beaches & Harbors — Rules & Regulations"
-- These are county-authority rules just like the codes, and they suffer
-- the same misattribution: applied to city-territory beaches via the
-- codify pipeline's polygon spatial-join without preemption check.
--
-- Verified on fid 8347 (La Jolla Shores): tier-1 BPS row 471 demoted by
-- the prior pass, but tier-3 supplementary BPS row 515 ("San Diego
-- County Department of Animal Services") remained operative and was
-- now winning the cascade.
--
-- New regex: `^[^,(]*\bCounty\b` — matches "X County" before any comma
-- or paren (i.e. county is the issuing AUTHORITY at the start of the
-- citation, not a parenthetical reference). Negative guard against
-- `^(City|Town|Village|Borough)` so that "Morro Bay Municipal Code
-- §7.04.010 (Adoption and incorporation of San Luis Obispo County
-- Animal Control Code Title 9 by reference)" stays operative — that's
-- a city ordinance adopting county code by reference, which is city
-- law and DOES apply at La Jolla Shores-equivalent city beaches.
--
-- Lake Anza (fid 8534) exclusion preserved.

begin;

with city_or_coastal_edge_beaches as (
  select distinct bg.fid
    from public.beaches_gold bg
    join public.jurisdictions j
      on st_dwithin(j.geom, bg.geom, 0.0018)
   where bg.is_active
     and j.funcstat  = 'A'
     and j.place_type like 'C%'
     and bg.fid <> 8534
),
demote_ids as (
  select bps.id
    from public.beach_policy_source bps
    join public.policy_source ps on ps.id = bps.policy_source_id
   where bps.operative_status = 'operative'
     and bps.beach_fid in (select fid from city_or_coastal_edge_beaches)
     and ps.citation ~  '^[^,(]*\mCounty\M'
     and ps.citation !~ '^(City|Town|Village|Borough)\M'
)
update public.beach_policy_source
   set operative_status = 'inapplicable_jurisdiction',
       status_note      = coalesce(status_note, '') ||
         case when status_note is null or status_note = '' then '' else ' | ' end ||
         '2026-05-30 demoted (county-authority supplementary pass): county-issued rule attributed to incorporated-city beach. See [[codify-city-preemption]] pin.'
 where id in (select id from demote_ids);

-- Re-promote affected beaches.
do $$
declare
  r record;
  n int := 0;
begin
  for r in
    select distinct bps.beach_fid as fid
      from public.beach_policy_source bps
     where bps.operative_status = 'inapplicable_jurisdiction'
       and bps.status_note like '%county-authority supplementary pass%'
  loop
    perform public._promote_zone_rules_for_fid(r.fid);
    n := n + 1;
  end loop;
  raise notice 'Re-promoted % beaches after supplementary demote', n;
end $$;

commit;
