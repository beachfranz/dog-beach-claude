-- 20260507_perimeter_injection.sql
--
-- Whole beach perimeter injection. Per Franz 2026-05-07: every beach
-- has at least one zone (the Whole beach), and that zone should
-- carry parking / restrooms / showers / picnic_area pills sourced
-- DETERMINISTICALLY from beach_amenities.has_* flags — not from the
-- LLM (closed_zones_v3 explicitly excludes these from the section
-- taxonomy).
--
-- Rules:
--   1. Ensure an unnamed Whole beach region exists in zone_rules. If
--      the LLM only emitted named sub-zones (Garrapata pattern),
--      synthesize one.
--   2. Inject sections into the Whole beach region only:
--        parking_lot   -> on_leash  (when has_parking)
--        restrooms     -> on_leash  (when has_restrooms)
--        showers       -> on_leash  (when has_showers)
--        picnic_area   -> on_leash  (when has_picnic_area)
--      All four are universally on-leash by admin convention. Don't
--      overwrite a section already present in the region (idempotent).
--   3. The renderer dedupes the Whole beach against named zones so
--      perimeter pills only appear in Whole beach when no named zone
--      claims them.
--
-- Splits the legacy `restrooms_showers` combined section into
-- separate `restrooms` and `showers` going forward. Old extractions
-- still carry the combined name; renderer keeps a legacy label.

begin;

-- ============================================================
-- Helper: ensure regions[] has at least one unnamed region (Whole beach)
-- ============================================================
create or replace function public._zr_ensure_unnamed_region(p_regions jsonb)
returns jsonb language sql immutable as $$
  select case
    when p_regions is null or jsonb_typeof(p_regions) <> 'array' then
      jsonb_build_array(jsonb_build_object('name', null, 'sections', '{}'::jsonb))
    when exists (
      select 1 from jsonb_array_elements(p_regions) r
      where (r->>'name') is null
    ) then p_regions
    else p_regions || jsonb_build_array(
      jsonb_build_object('name', null, 'sections', '{}'::jsonb))
  end
$$;

-- ============================================================
-- Helper: add a section to the unnamed region only (idempotent —
-- skips if the section is already present)
-- ============================================================
create or replace function public._zr_add_section_to_unnamed(
  p_regions jsonb, p_section_name text, p_rule text
) returns jsonb language sql immutable as $$
  select coalesce((
    select jsonb_agg(
      case
        when (r->>'name') is null
             and not (coalesce(r->'sections', '{}'::jsonb) ? p_section_name)
          then jsonb_set(
                 jsonb_set(r, '{sections}',
                   coalesce(r->'sections', '{}'::jsonb)),
                 array['sections', p_section_name],
                 jsonb_build_object('rule', p_rule))
        else r
      end
    )
    from jsonb_array_elements(p_regions) r
  ), '[]'::jsonb)
$$;

-- ============================================================
-- Top-level: inject perimeter sections sourced from beach_amenities
-- into the Whole beach region. Handles v1 (regions[]) and v2
-- (seasons[].regions[]).
-- ============================================================
create or replace function public._zr_inject_perimeter(p_zr jsonb, p_fid bigint)
returns jsonb language plpgsql stable as $$
declare
  v_zr jsonb := p_zr;
  v_has_parking    boolean;
  v_has_restrooms  boolean;
  v_has_showers    boolean;
  v_has_picnic     boolean;
  v_seasons        jsonb;
  v_new_seasons    jsonb := '[]'::jsonb;
  v_season         jsonb;
  v_regs           jsonb;
begin
  if v_zr is null or p_fid is null then return v_zr; end if;

  -- Pull amenity flags via the arena bridge.
  -- has_parking is derived: parking_type non-null and not 'none'.
  select (ba.parking_type is not null and ba.parking_type <> 'none'),
         coalesce(ba.has_restrooms, false),
         coalesce(ba.has_showers, false),
         coalesce(ba.has_picnic_area, false)
    into v_has_parking, v_has_restrooms, v_has_showers, v_has_picnic
    from public.beaches_gold g
    left join public.beach_amenities ba on ba.arena_group_id = g.group_id
    where g.fid = p_fid;

  -- v2: seasons[].regions[]
  if v_zr ? 'seasons' and jsonb_typeof(v_zr->'seasons') = 'array' then
    for v_season in select * from jsonb_array_elements(v_zr->'seasons')
    loop
      v_regs := public._zr_ensure_unnamed_region(v_season->'regions');
      if v_has_parking   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'parking_lot',  'on_leash'); end if;
      if v_has_restrooms then v_regs := public._zr_add_section_to_unnamed(v_regs, 'restrooms',    'on_leash'); end if;
      if v_has_showers   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'showers',      'on_leash'); end if;
      if v_has_picnic    then v_regs := public._zr_add_section_to_unnamed(v_regs, 'picnic_area',  'on_leash'); end if;
      v_new_seasons := v_new_seasons || jsonb_build_array(jsonb_set(v_season, '{regions}', v_regs));
    end loop;
    v_zr := jsonb_set(v_zr, '{seasons}', v_new_seasons);
  end if;

  -- v1: regions[] (only when no seasons)
  if v_zr ? 'regions' and not (v_zr ? 'seasons') and jsonb_typeof(v_zr->'regions') = 'array' then
    v_regs := public._zr_ensure_unnamed_region(v_zr->'regions');
    if v_has_parking   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'parking_lot',  'on_leash'); end if;
    if v_has_restrooms then v_regs := public._zr_add_section_to_unnamed(v_regs, 'restrooms',    'on_leash'); end if;
    if v_has_showers   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'showers',      'on_leash'); end if;
    if v_has_picnic    then v_regs := public._zr_add_section_to_unnamed(v_regs, 'picnic_area',  'on_leash'); end if;
    v_zr := jsonb_set(v_zr, '{regions}', v_regs);
  end if;

  -- Beach has bdp row but no zone_rules structure at all: synthesize
  -- a v1 shape with just the unnamed region carrying perimeter.
  if not (v_zr ? 'seasons') and not (v_zr ? 'regions') then
    v_regs := public._zr_ensure_unnamed_region(null);
    if v_has_parking   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'parking_lot',  'on_leash'); end if;
    if v_has_restrooms then v_regs := public._zr_add_section_to_unnamed(v_regs, 'restrooms',    'on_leash'); end if;
    if v_has_showers   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'showers',      'on_leash'); end if;
    if v_has_picnic    then v_regs := public._zr_add_section_to_unnamed(v_regs, 'picnic_area',  'on_leash'); end if;
    v_zr := jsonb_set(coalesce(v_zr, '{}'::jsonb), '{regions}', v_regs, true);
  end if;

  return v_zr;
end $$;

-- ============================================================
-- Update the zone_rules promoter to call the new perimeter injector
-- (replaces the older parking-only injection).
-- ============================================================
create or replace function public._promote_zone_rules_for_fid(p_fid bigint)
returns text
language plpgsql
security definer
as $$
declare
  v_zr jsonb;
  v_updated boolean;
begin
  select claimed_values->'zone_rules'
    into v_zr
    from public.beach_enrichment_provenance
   where source = 'text_repass_v1'
     and gold_fid = p_fid
     and claimed_values ? 'zone_rules'
   order by updated_at desc
   limit 1;

  if v_zr is null then
    -- No LLM zone_rules: still inject perimeter so amenity-driven
    -- pills appear on every beach.
    v_zr := public._zr_inject_perimeter('{}'::jsonb, p_fid);
    if not (v_zr ? 'regions') and not (v_zr ? 'seasons') then
      return 'no zone_rules for fid';
    end if;
  else
    -- Apply admin policy + perimeter injection
    v_zr := public._inject_parking_on_leash(v_zr);
    v_zr := public._zr_inject_perimeter(v_zr, p_fid);
  end if;

  update public.beach_dog_policy
     set zone_rules = v_zr,
         zone_rules_updated_at = now()
   where arena_group_id = p_fid
     and (zone_rules is distinct from v_zr);
  v_updated := found;

  if not v_updated and not exists (
    select 1 from public.beach_dog_policy where arena_group_id = p_fid
  ) then
    insert into public.beach_dog_policy (arena_group_id, zone_rules, zone_rules_updated_at)
    values (p_fid, v_zr, now())
    on conflict (arena_group_id) do update
      set zone_rules = excluded.zone_rules,
          zone_rules_updated_at = excluded.zone_rules_updated_at;
    v_updated := true;
  end if;

  return case when v_updated then 'updated' else 'no-op (already current)' end;
end $$;

revoke all on function public._promote_zone_rules_for_fid(bigint) from public, anon, authenticated;
grant  execute on function public._promote_zone_rules_for_fid(bigint) to service_role;

-- ============================================================
-- One-shot backfill across all bdp rows. Runs perimeter injection
-- in addition to the parking deterministic rule (no-op for rows
-- already correct).
-- ============================================================
update public.beach_dog_policy bdp
   set zone_rules = public._zr_inject_perimeter(
                      public._inject_parking_on_leash(coalesce(zone_rules, '{}'::jsonb)),
                      g.fid),
       zone_rules_updated_at = now()
  from public.beaches_gold g
 where g.group_id = bdp.arena_group_id
   and g.is_active
   and zone_rules is distinct from public._zr_inject_perimeter(
                                     public._inject_parking_on_leash(coalesce(zone_rules, '{}'::jsonb)),
                                     g.fid);

commit;
