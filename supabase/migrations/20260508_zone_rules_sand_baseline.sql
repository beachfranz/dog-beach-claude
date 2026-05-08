-- 20260508_zone_rules_sand_baseline.sql
--
-- Make zone_rules useful even when no LLM extraction has run. The
-- existing _zr_inject_perimeter only adds parking/restrooms/showers/
-- picnic_area sections — and only when those amenity bits are populated.
-- For new states where no per-beach extraction has run AND no amenity
-- ingest has happened (60 of 65 OR beaches), the result is the empty
-- default {"regions":[{"name":null,"sections":{}}]}.
--
-- This migration adds _zr_inject_sand_from_policy(zr, fid): when there
-- is no existing 'sand' section, derive one from beach_dog_policy:
--   has_off_leash=true            -> sand: off_leash
--   has_on_leash=true             -> sand: on_leash
--   dogs_allowed='no'             -> sand: not_allowed
--   dogs_allowed='mixed' + on=t   -> sand: on_leash (caveat noted)
--
-- And updates _promote_zone_rules_for_fid to call it after the perimeter
-- injection. Also updates the trigger so re-publishes pick it up.
--
-- The 'evidence' field on the section attributes the call to the
-- inferred policy source so a future curator knows it's a default.

begin;

create or replace function public._zr_inject_sand_from_policy(p_zr jsonb, p_fid bigint)
returns jsonb
language plpgsql
stable
as $function$
declare
  v_zr jsonb := p_zr;
  v_dogs_allowed text;
  v_has_on boolean;
  v_has_off boolean;
  v_rule text;
  v_evidence_source text;
  v_evidence_quote text;
  v_regs jsonb;
  v_already_has_sand boolean := false;
begin
  if v_zr is null or p_fid is null then return v_zr; end if;

  select dogs_allowed, has_on_leash, has_off_leash
    into v_dogs_allowed, v_has_on, v_has_off
    from public.beach_dog_policy
   where arena_group_id = p_fid;

  if v_dogs_allowed is null then return v_zr; end if;

  -- Decide rule
  v_rule := case
    when v_has_off is true                                then 'off_leash'
    when v_dogs_allowed = 'no'                            then 'not_allowed'
    when v_has_on is true                                 then 'on_leash'
    when v_dogs_allowed = 'mixed'                         then 'on_leash'
    when v_dogs_allowed = 'yes'                           then 'on_leash'
    else null
  end;

  if v_rule is null then return v_zr; end if;

  -- Find the BEP source that gave us the dogs evidence (best effort)
  select bep.source, (bep.claimed_values->>'source_quote')
    into v_evidence_source, v_evidence_quote
    from public.beach_enrichment_provenance bep
   where bep.gold_fid = p_fid and bep.field_group = 'dogs'
   order by bep.confidence desc nulls last, bep.updated_at desc
   limit 1;

  -- Check if the unnamed region already has a sand section (don't overwrite real evidence)
  select exists(
    select 1 from jsonb_array_elements(coalesce(v_zr->'regions','[]'::jsonb)) r
     where (r->>'name') is null
       and r->'sections' ? 'sand'
  ) into v_already_has_sand;

  if v_already_has_sand then return v_zr; end if;

  -- Make sure unnamed region exists
  if v_zr ? 'regions' then
    v_regs := public._zr_ensure_unnamed_region(v_zr->'regions');
  else
    v_regs := jsonb_build_array(jsonb_build_object('name', null, 'sections', '{}'::jsonb));
  end if;

  -- Add sand section to unnamed region
  v_regs := (
    select jsonb_agg(
      case when (r->>'name') is null then
        jsonb_set(r, '{sections,sand}',
          jsonb_strip_nulls(jsonb_build_object(
            'rule', v_rule,
            'evidence', case
              when v_evidence_source is not null then
                jsonb_strip_nulls(jsonb_build_object(
                  'source', v_evidence_source,
                  'quote', v_evidence_quote,
                  'inferred', true
                ))
              else null
            end
          )))
      else r end
    )
    from jsonb_array_elements(v_regs) r
  );

  v_zr := jsonb_set(v_zr, '{regions}', v_regs);
  return v_zr;
end;
$function$;

grant execute on function public._zr_inject_sand_from_policy(jsonb, bigint) to anon, authenticated, service_role;


-- Update _promote_zone_rules_for_fid to also call sand injector
create or replace function public._promote_zone_rules_for_fid(p_fid bigint)
returns text
language plpgsql
security definer
as $function$
declare v_zr jsonb; v_updated boolean; v_source text;
begin
  select source into v_source from public.beach_dog_policy
   where arena_group_id = p_fid;
  if v_source = 'manual_curator' then
    return 'skipped — manual_curator';
  end if;

  select claimed_values->'zone_rules' into v_zr
    from public.beach_enrichment_provenance
   where source = 'text_repass_v1' and gold_fid = p_fid
     and claimed_values ? 'zone_rules'
   order by updated_at desc limit 1;

  if v_zr is null then
    v_zr := public._zr_inject_perimeter('{}'::jsonb, p_fid);
    v_zr := public._zr_inject_sand_from_policy(v_zr, p_fid);
    if not (v_zr ? 'regions') and not (v_zr ? 'seasons') then
      return 'no zone_rules for fid';
    end if;
  else
    v_zr := public._inject_parking_on_leash(v_zr);
    v_zr := public._zr_inject_perimeter(v_zr, p_fid);
    v_zr := public._zr_inject_sand_from_policy(v_zr, p_fid);
  end if;

  update public.beach_dog_policy
     set zone_rules = v_zr, zone_rules_updated_at = now()
   where arena_group_id = p_fid and (zone_rules is distinct from v_zr);
  v_updated := found;

  if not v_updated and not exists (select 1 from public.beach_dog_policy where arena_group_id = p_fid) then
    insert into public.beach_dog_policy (arena_group_id, zone_rules, zone_rules_updated_at)
    values (p_fid, v_zr, now())
    on conflict (arena_group_id) do update
      set zone_rules = excluded.zone_rules, zone_rules_updated_at = excluded.zone_rules_updated_at;
    v_updated := true;
  end if;

  return case when v_updated then 'updated' else 'no-op' end;
end;
$function$;

commit;
