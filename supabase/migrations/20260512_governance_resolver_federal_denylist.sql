-- 20260512_governance_resolver_federal_denylist.sql
--
-- Refines _resolve_governance_gold to demote CPAD-derived canonical rows
-- when the claimed agency is a non-park-managing federal entity AND a
-- tiger_places city/county claim exists for the same beach. Same shape
-- as the HDB-Bolsa Chica bug, different source surface: CPAD's spatial
-- join sometimes matches tiny inland parcels (e.g. a BLM mineral lease
-- near La Jolla) and inherits that agency as the beach's governance.
--
-- Concrete cases (Franz audit, 2026-05-12):
--   fid 3188  Bird Rock Beach          canonical=BLM federal  (should be SD city)
--   fid 8349  Children's Pool Beach    canonical=BLM federal  (should be SD city)
--   fid 6199  Rancho Palos Verdes Beach canonical=USCG federal (should be city/county)
--
-- Denylist (federal agencies that don't manage actual park beaches):
--   - Bureau of Land Management (mineral/grazing leases — non-recreation)
--   - U.S. Coast Guard / United States Coast Guard (small stations only)
--   - Department of Defense + military branches (covered by military_bases
--     source which has its own pipeline; CPAD's DOD claims are stale or
--     adjacent-parcel artifacts)
--
-- Trusted federal agencies (NPS, USFWS, USFS, BOR) stay canonical when
-- CPAD attributes them — those actually manage park-and-beach land.
--
-- Post-resolution model: keep the standard priority sort as Step 1,
-- then run a Step 2 demote pass for the denylisted cases. Lighter
-- intervention than touching the generic _resolve_field_group_gold
-- function. Idempotent.

begin;

create or replace function public._resolve_governance_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare
  v_touched int := 0;
  v_demoted int := 0;
begin
  -- Step 1: standard priority-based canonical assignment.
  v_touched := public._resolve_field_group_gold('governance', p_fid);

  -- Step 2: demote the federal-denylist CPAD canonicals when a tiger_places
  -- city/county alternative exists. Pairs each "bad" canonical id with its
  -- alternative; both flips happen in one CTE so we don't transiently
  -- leave a beach with zero canonical governance rows.
  with bad_canon as (
    select e.id, e.gold_fid
      from public.beach_enrichment_provenance e
     where e.field_group = 'governance'
       and e.is_canonical = true
       and e.source = 'cpad'
       and (
         e.claimed_values->>'name' ilike '%Bureau of Land Management%'
         or e.claimed_values->>'name' ilike '%Coast Guard%'
         or e.claimed_values->>'name' ilike '%Department of Defense%'
         or e.claimed_values->>'name' ~* '\m(Navy|Marine Corps|Air Force|Army Corps)\M'
       )
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  alternative as (
    select distinct on (e.gold_fid) e.id, e.gold_fid
      from public.beach_enrichment_provenance e
     where e.field_group = 'governance'
       and e.source = 'tiger_places'
       and lower(e.claimed_values->>'type') in ('city','county')
       and e.gold_fid in (select gold_fid from bad_canon)
     order by e.gold_fid, e.confidence desc nulls last, e.id asc
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
