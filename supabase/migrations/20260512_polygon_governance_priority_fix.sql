-- 20260512_polygon_governance_priority_fix.sql
--
-- Bug discovered during audit: polygon_containment_governance_v1 at
-- priority 6 displaced tribal_lands at priority 9 for fid 2986
-- Thompson Beach (Chemehuevi Indian Reservation). Tribal_lands and
-- military_bases must rank ABOVE the polygon-derived fallback because
-- they represent real special-jurisdiction overlays that take precedence.
--
-- Fix: move polygon_containment_governance_v1 to priority 10. Below
-- every explicitly-listed governance source, above the 'else' bucket
-- (99). The Step 2 denylist demote pass in _resolve_governance_gold
-- still explicitly promotes this source for the 3 state-agency cases
-- — that's an override, not a priority-based pick — so the structural
-- fix for Carbon/Amarillo/Santa Claus still works.

begin;

create or replace function public._gold_field_group_source_priority(p_field_group text, p_source text)
returns integer
language sql
immutable
as $function$
  select case
    when p_source = 'manual' then 1
    when p_field_group = 'dogs' then case p_source
      when 'llm'              then 2
      when 'research'         then 3
      when 'park_url'         then 4
      when 'city_policy'      then 4
      when 'county_policy'    then 4
      when 'unified_v1'       then 5
      when 'json_explode'     then 5
      when 'old_school_llm'   then 5
      when 'park_operators'   then 6
      when 'governing_body'   then 7
      when 'cpad'             then 8
      when 'jurisdictions'    then 9
      when 'counties'         then 10
      when 'nps_places'       then 11
      when 'military_bases'   then 12
      when 'tribal_lands'     then 12
      else                          99
    end
    when p_field_group = 'practical' then case p_source
      when 'llm'              then 2
      when 'park_url'         then 3
      when 'park_operators'   then 4
      when 'unified_v1'       then 5
      when 'old_school_llm'   then 5
      when 'json_explode'     then 5
      when 'ccc'              then 6
      when 'research'         then 7
      else                          99
    end
    when p_field_group = 'governance' then case p_source
      when 'park_url'                          then 2
      when 'park_operators'                    then 2
      when 'park_url_buffer_attribution'       then 3
      when 'csp_parks'                         then 4
      when 'nps_places'                        then 4
      when 'cpad'                              then 5
      when 'tiger_places'                      then 6
      when 'governing_body'                    then 7
      when 'name'                              then 8
      when 'tribal_lands'                      then 9
      when 'military_bases'                    then 9
      when 'polygon_containment_governance_v1' then 10  -- gap-fill below all explicit sources
      else                                          99
    end
    when p_field_group = 'access' then case p_source
      when 'plz'           then 2
      when 'cpad'          then 3
      when 'json_explode'  then 4
      when 'csp_parks'     then 5
      when 'military_bases' then 6
      else                       99
    end
    when p_field_group = 'polygon_containment' then case p_source
      when 'cpad'           then 2
      when 'jurisdictions'  then 3
      when 'counties'       then 4
      when 'military_bases' then 5
      when 'tribal_lands'   then 5
      else                       99
    end
    else 99
  end;
$function$;

commit;
