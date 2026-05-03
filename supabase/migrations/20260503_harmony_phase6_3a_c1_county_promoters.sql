-- 20260503_harmony_phase6_3a_c1_county_promoters.sql
--
-- Phase 6.3a of the harmony migration (see docs/harmony.md).
--
-- Adds typed FK columns on beaches_gold for c1_city + county containment
-- and extends _promote_polygon_containment_to_gold to write them from
-- canonical evidence. Mirrors the cpad_unit_id pattern from phase 5.
--
-- Per polygon-kinds spec (project_unified_dog_policy_pipeline.md):
--   polygon_kind='cpad_unit'      → beaches_gold.cpad_unit_id        (phase 5)
--   polygon_kind='c1_city'        → beaches_gold.c1_jurisdiction_id  (this phase)
--   polygon_kind='county'         → beaches_gold.county_geoid        (this phase)
--   polygon_kind='cdp'            → no promotion (informational fall-through)
--   polygon_kind='military_base'  → no promotion (review queue only)
--   polygon_kind='tribal_land'    → no promotion (review queue only)

begin;

-- 1. ALTER beaches_gold to add the new FK-style columns. Both nullable;
--    existing rows untouched.
alter table public.beaches_gold
  add column if not exists c1_jurisdiction_id bigint,
  add column if not exists county_geoid text;

create index if not exists beaches_gold_c1_jurisdiction_id_idx
  on public.beaches_gold (c1_jurisdiction_id) where c1_jurisdiction_id is not null;
create index if not exists beaches_gold_county_geoid_idx
  on public.beaches_gold (county_geoid) where county_geoid is not null;

comment on column public.beaches_gold.c1_jurisdiction_id is
  'TIGER C1 incorporated place id from canonical polygon_containment evidence (jurisdictions source). Set by _promote_polygon_containment_to_gold. Null when beach is not inside any incorporated city.';

comment on column public.beaches_gold.county_geoid is
  'TIGER county GEOID (5-char text, e.g. "06037" for LA County) from canonical polygon_containment evidence (counties source). Set by _promote_polygon_containment_to_gold.';

-- 2. Extend the promoter to write all three FK columns from canonical
--    polygon_containment evidence. One UPDATE per polygon_kind keeps the
--    logic readable; each is a no-op when no canonical row exists for
--    that kind on a given beach.
create or replace function public._promote_polygon_containment_to_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare touched int := 0; n_cpad int; n_c1 int; n_county int;
begin
  -- cpad_unit_id (phase 5 logic, unchanged)
  with canonical as (
    select gold_fid, (claimed_values->>'polygon_id')::bigint as polygon_id
      from public.beach_enrichment_provenance
     where field_group = 'polygon_containment'
       and is_canonical = true
       and gold_fid is not null
       and claimed_values->>'polygon_kind' = 'cpad_unit'
       and (p_fid is null or gold_fid = p_fid)
  ),
  upd as (
    update public.beaches_gold g
       set cpad_unit_id = c.polygon_id::integer
      from canonical c
     where g.fid = c.gold_fid
       and (g.cpad_unit_id is distinct from c.polygon_id::integer)
    returning 1
  )
  select count(*) into n_cpad from upd;

  -- c1_jurisdiction_id (new)
  with canonical as (
    select gold_fid, (claimed_values->>'polygon_id')::bigint as polygon_id
      from public.beach_enrichment_provenance
     where field_group = 'polygon_containment'
       and is_canonical = true
       and gold_fid is not null
       and claimed_values->>'polygon_kind' = 'c1_city'
       and (p_fid is null or gold_fid = p_fid)
  ),
  upd as (
    update public.beaches_gold g
       set c1_jurisdiction_id = c.polygon_id
      from canonical c
     where g.fid = c.gold_fid
       and (g.c1_jurisdiction_id is distinct from c.polygon_id)
    returning 1
  )
  select count(*) into n_c1 from upd;

  -- county_geoid (new) — text, not bigint
  with canonical as (
    select gold_fid, claimed_values->>'polygon_id' as polygon_id_text
      from public.beach_enrichment_provenance
     where field_group = 'polygon_containment'
       and is_canonical = true
       and gold_fid is not null
       and claimed_values->>'polygon_kind' = 'county'
       and (p_fid is null or gold_fid = p_fid)
  ),
  upd as (
    update public.beaches_gold g
       set county_geoid = c.polygon_id_text
      from canonical c
     where g.fid = c.gold_fid
       and (g.county_geoid is distinct from c.polygon_id_text)
    returning 1
  )
  select count(*) into n_county from upd;

  touched := coalesce(n_cpad, 0) + coalesce(n_c1, 0) + coalesce(n_county, 0);
  return touched;
end;
$$;

comment on function public._promote_polygon_containment_to_gold(bigint) is
  'Phase 6.3a of harmony migration. Writes typed FK columns on beaches_gold from canonical polygon_containment evidence: cpad_unit_id (cpad_unit kind), c1_jurisdiction_id (c1_city kind), county_geoid (county kind). cdp/military_base/tribal_land kinds intentionally do not promote — they are informational or route to review queue.';

commit;
