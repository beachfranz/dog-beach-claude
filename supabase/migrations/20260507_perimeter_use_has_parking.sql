-- 20260507_perimeter_use_has_parking.sql
--
-- Switch the perimeter injector from `parking_type is not null` to
-- the proper `has_parking` boolean (just landed via the OSM
-- amenity emitter consensus chain). Re-run the backfill so all
-- 498 has_parking=true beaches get parking pills.

begin;

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

  select coalesce(ba.has_parking, false),
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
      v_regs := public._zr_strip_section_from_unnamed(v_regs, 'restrooms_showers');
      if v_has_parking   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'parking_lot',  'on_leash'); end if;
      if v_has_restrooms then v_regs := public._zr_add_section_to_unnamed(v_regs, 'restrooms',    'on_leash'); end if;
      if v_has_showers   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'showers',      'on_leash'); end if;
      if v_has_picnic    then v_regs := public._zr_add_section_to_unnamed(v_regs, 'picnic_area',  'on_leash'); end if;
      v_new_seasons := v_new_seasons || jsonb_build_array(jsonb_set(v_season, '{regions}', v_regs));
    end loop;
    v_zr := jsonb_set(v_zr, '{seasons}', v_new_seasons);
  end if;

  -- v1: regions[]
  if v_zr ? 'regions' and not (v_zr ? 'seasons') and jsonb_typeof(v_zr->'regions') = 'array' then
    v_regs := public._zr_ensure_unnamed_region(v_zr->'regions');
    v_regs := public._zr_strip_section_from_unnamed(v_regs, 'restrooms_showers');
    if v_has_parking   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'parking_lot',  'on_leash'); end if;
    if v_has_restrooms then v_regs := public._zr_add_section_to_unnamed(v_regs, 'restrooms',    'on_leash'); end if;
    if v_has_showers   then v_regs := public._zr_add_section_to_unnamed(v_regs, 'showers',      'on_leash'); end if;
    if v_has_picnic    then v_regs := public._zr_add_section_to_unnamed(v_regs, 'picnic_area',  'on_leash'); end if;
    v_zr := jsonb_set(v_zr, '{regions}', v_regs);
  end if;

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

-- Re-run backfill with new has_parking signal
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
