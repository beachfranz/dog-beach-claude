-- 20260503_harmony_phase3_cpad_pilot.sql
--
-- Phase 3 of the harmony migration (see docs/harmony.md).
--
-- Two changes, both reversible:
--   1. ALTER beach_enrichment_provenance.fid → nullable. The legacy fid is
--      no longer required for new evidence rows; gold_fid alone is enough.
--      Existing rows untouched (all currently have fid set).
--   2. CREATE populate_from_cpad_gold(p_fid) — gold-spine sibling of the
--      legacy populate_from_cpad. Reads beaches_gold (not locations_stage),
--      writes evidence keyed on gold_fid (not fid). Same Tier-1-ish
--      ranking, same field_group/source semantics, same
--      governance/access split.
--
-- The legacy populate_from_cpad is untouched and continues operating on
-- locations_stage.fid. Both populators run side-by-side until phase 6
-- migrates the rest and phase 7 deprecates the legacy path.

begin;

-- 1. Make legacy fid nullable so new gold-spine evidence rows can be
--    inserted with only gold_fid set.
alter table public.beach_enrichment_provenance
  alter column fid drop not null;

-- 2. Gold-spine populator. Mirrors populate_from_cpad's structure with
--    these substitutions:
--      locations_stage           → beaches_gold
--      s.fid                     → g.fid (becomes gold_fid in the INSERT)
--      s.display_name            → coalesce(g.display_name_override, g.name)
--      INSERT fid                → INSERT gold_fid (fid stays NULL)
--      on conflict (fid, ...)    → on conflict (gold_fid, ...) where gold_fid is not null
--
-- Confidence ladder + ranking unchanged from the legacy version. The full
-- Tier 1 ranking (env-overlay demote, "Beach"-in-name win, trigram match,
-- smallest area) lands in phase 3.5 as a separate refactor — keeping this
-- commit focused on the namespace migration alone.
create or replace function public.populate_from_cpad_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select g.fid, c.unit_name, c.mng_ag_lev, c.mng_agncy, c.access_typ,
      st_contains(c.geom, g.geom::geometry)               as inside,
      st_distance(g.geom, c.geom::geography)              as dist_m,
      st_area(c.geom::geography)                          as area_m2,
      cardinality(public.shared_name_tokens(
        coalesce(g.display_name_override, g.name), c.unit_name)) > 0 as name_match
    from public.beaches_gold g
    join public.cpad_units c on st_dwithin(c.geom::geography, g.geom, 500)
    where (p_fid is null or g.fid = p_fid)
      and g.geom is not null
      and g.is_active
  ),
  with_conf as (
    select *,
      case
        when inside     and name_match                       then 0.95
        when not inside and dist_m <= 100 and name_match     then 0.85
        when inside     and not name_match                   then 0.75
        when not inside and dist_m <= 500 and name_match     then 0.65
        when not inside and dist_m <= 100 and not name_match then 0.50
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
      case mng_ag_lev
        when 'City'                    then 'city'
        when 'County'                  then 'county'
        when 'State'                   then 'state'
        when 'Federal'                 then 'federal'
        when 'Tribal'                  then 'tribal'
        when 'Special District'        then 'special_district'
        when 'Non Profit'              then 'nonprofit'
        when 'Joint'                   then 'joint'
        when 'Home Owners Association' then 'private'
        when 'Private'                 then 'private'
        else                                'unknown'
      end as gov_type,
      case access_typ
        when 'Open Access'       then 'public'
        when 'Restricted Access' then 'restricted'
        when 'No Public Access'  then 'private'
        when 'Unknown Access'    then 'unknown'
        else                          null
      end as access_status
    from best
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'cpad', confidence,
      jsonb_build_object(
        'type', gov_type,
        'name', public.canonical_agency_name(gov_type, mng_agncy)
      ),
      now()
    from with_type
    where mng_ag_lev is not null
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
    select fid, 'access', 'cpad', confidence,
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
$$;

comment on function public.populate_from_cpad_gold(bigint) is
  'Gold-spine sibling of populate_from_cpad. Reads beaches_gold, writes evidence keyed on gold_fid. Phase 3 of the harmony migration (docs/harmony.md). Coexists with legacy populate_from_cpad until phase 7 deprecation.';

commit;
