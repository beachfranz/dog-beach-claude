-- 20260511_polygon_containment_overrides.sql
--
-- Crowd-sourced polygon sources (CPAD, OSM, PAD-US) regularly contain
-- mis-tagged or mis-shaped polygons. When that happens, our spatial-
-- join populators (_promote_polygon_containment_to_gold) misattribute
-- beaches sitting inside the wrong polygon. The misattribution then
-- cascades through downstream populators (populate_from_park_url
-- inheriting amenities from the wrong state-beach page, governance
-- becoming "State" when it should be "City", etc).
--
-- Concrete case (Franz, 2026-05-11): CPAD unit 7223 "Bolsa Chica
-- State Beach" is a MultiPolygon with 3 disjoint parts. Part 1 sits
-- where HB Dog Beach (fid 6212) actually is — but HB Dog Beach is
-- city-managed, not state. The CPAD source data is wrong about the
-- southern boundary. Result: HB Dog Beach inherits Bolsa Chica's
-- amenities (showers, fire pits, food) which are wrong.
--
-- This migration adds a per-beach override mechanism:
--   beaches_gold.polygon_overrides JSONB — keys: cpad_unit_id,
--   c1_jurisdiction_id, county_geoid (any subset), plus _override_at
--   and _override_reason. When a key is present (even null), the
--   promote function uses the override INSTEAD of the canonical
--   spatial-join result.
--
-- The override survives re-population. To set an override, write a
-- value (or explicit null) into the JSONB for that field.

begin;

alter table public.beaches_gold
  add column if not exists polygon_overrides jsonb;

comment on column public.beaches_gold.polygon_overrides is
  'Manual overrides for spatial-containment attribution. Survives '
  're-population. Keys: cpad_unit_id | c1_jurisdiction_id | county_geoid '
  '(any subset present in the JSONB wins over the canonical BEP). '
  'Plus optional _override_at, _override_by, _override_reason for audit.';

-- Update the promote function to respect overrides.
-- For each polygon field: if polygon_overrides contains the key, use
-- that value (which may be SQL null = "force no attribution"); else
-- use the canonical BEP result as before.
create or replace function public._promote_polygon_containment_to_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare touched int := 0; n_cpad int; n_c1 int; n_county int;
begin
  -- cpad_unit_id — override-aware
  with canonical as (
    select gold_fid, (claimed_values->>'polygon_id')::bigint as polygon_id
      from public.beach_enrichment_provenance
     where field_group = 'polygon_containment'
       and is_canonical = true
       and gold_fid is not null
       and claimed_values->>'polygon_kind' = 'cpad_unit'
       and (p_fid is null or gold_fid = p_fid)
  ),
  -- Beaches we want to write to: either canonical (with override applied) OR
  -- override-only (the override exists but no canonical row).
  to_write as (
    select g.fid as gold_fid,
           case when g.polygon_overrides ? 'cpad_unit_id'
                then nullif(g.polygon_overrides->>'cpad_unit_id', '')::integer
                else c.polygon_id::integer
           end as target
      from public.beaches_gold g
      left join canonical c on c.gold_fid = g.fid
     where (p_fid is null or g.fid = p_fid)
       and (g.polygon_overrides ? 'cpad_unit_id' or c.gold_fid is not null)
  ),
  upd as (
    update public.beaches_gold g
       set cpad_unit_id = w.target
      from to_write w
     where g.fid = w.gold_fid
       and (g.cpad_unit_id is distinct from w.target)
    returning 1
  )
  select count(*) into n_cpad from upd;

  -- c1_jurisdiction_id — override-aware
  with canonical as (
    select gold_fid, (claimed_values->>'polygon_id')::bigint as polygon_id
      from public.beach_enrichment_provenance
     where field_group = 'polygon_containment'
       and is_canonical = true
       and gold_fid is not null
       and claimed_values->>'polygon_kind' = 'c1_city'
       and (p_fid is null or gold_fid = p_fid)
  ),
  to_write as (
    select g.fid as gold_fid,
           case when g.polygon_overrides ? 'c1_jurisdiction_id'
                then nullif(g.polygon_overrides->>'c1_jurisdiction_id', '')::bigint
                else c.polygon_id
           end as target
      from public.beaches_gold g
      left join canonical c on c.gold_fid = g.fid
     where (p_fid is null or g.fid = p_fid)
       and (g.polygon_overrides ? 'c1_jurisdiction_id' or c.gold_fid is not null)
  ),
  upd as (
    update public.beaches_gold g
       set c1_jurisdiction_id = w.target
      from to_write w
     where g.fid = w.gold_fid
       and (g.c1_jurisdiction_id is distinct from w.target)
    returning 1
  )
  select count(*) into n_c1 from upd;

  -- county_geoid — override-aware
  with canonical as (
    select gold_fid, claimed_values->>'polygon_id' as polygon_id_text
      from public.beach_enrichment_provenance
     where field_group = 'polygon_containment'
       and is_canonical = true
       and gold_fid is not null
       and claimed_values->>'polygon_kind' = 'county'
       and (p_fid is null or gold_fid = p_fid)
  ),
  to_write as (
    select g.fid as gold_fid,
           case when g.polygon_overrides ? 'county_geoid'
                then nullif(g.polygon_overrides->>'county_geoid', '')
                else c.polygon_id_text
           end as target
      from public.beaches_gold g
      left join canonical c on c.gold_fid = g.fid
     where (p_fid is null or g.fid = p_fid)
       and (g.polygon_overrides ? 'county_geoid' or c.gold_fid is not null)
  ),
  upd as (
    update public.beaches_gold g
       set county_geoid = w.target
      from to_write w
     where g.fid = w.gold_fid
       and (g.county_geoid is distinct from w.target)
    returning 1
  )
  select count(*) into n_county from upd;

  touched := coalesce(n_cpad,0) + coalesce(n_c1,0) + coalesce(n_county,0);
  return touched;
end;
$function$;

commit;
