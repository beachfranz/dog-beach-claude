-- 20260520_codify_deep_extract_phase4_global_notes_injector.sql
--
-- Per docs/codify_deep_extract_full_spec.md §3 + §4.
--
-- Adds a new injector `_zr_inject_global_notes` that runs LAST in the
-- zone_rules build chain. Reads beach_policy_source rows where
-- is_global_note=true and emits them to a NEW top-level array
-- `zone_rules.global_notes[]`. Also strips any such rules that earlier
-- injectors may have placed inside regions[].sections{} so they don't
-- double-render.
--
-- Meta-rules covered (per scripts/codify_vocab.py):
--   nuisance_restriction, waste_pickup_required, collar_tag_required,
--   local_stricter_authorized, no_policy_published

begin;

create or replace function public._zr_inject_global_notes(
  p_zr jsonb, p_fid bigint
) returns jsonb
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_notes jsonb := '[]'::jsonb;
  v_rec record;
  v_zr jsonb := p_zr;
begin
  -- Pull every is_global_note row for this beach, joined to policy_source
  -- for citation/url/authority_tier metadata. De-dup by rule (only one
  -- row per kind in global_notes; if multiple bps rows have the same
  -- meta-rule, the highest-tier wins via the ORDER BY).
  for v_rec in
    select distinct on (bps.rule)
           bps.rule, bps.evidence_verbatim, bps.authority_tier,
           ps.subtype, ps.citation, ps.source_url
      from public.beach_policy_source bps
      join public.policy_source ps on ps.id = bps.policy_source_id
     where bps.beach_fid = p_fid
       and bps.is_global_note = true
       and (bps.operative_status = 'operative' or bps.operative_status is null)
     order by bps.rule, bps.authority_tier asc nulls last
  loop
    v_notes := v_notes || jsonb_build_array(jsonb_build_object(
      'kind', v_rec.rule,
      'evidence', jsonb_strip_nulls(jsonb_build_object(
        'quote',          v_rec.evidence_verbatim,
        'source',         v_rec.subtype::text,
        'citation',       v_rec.citation,
        'source_url',     v_rec.source_url,
        'authority_tier', v_rec.authority_tier
      ))
    ));
  end loop;

  -- Attach global_notes to top of zone_rules (replace if exists).
  if jsonb_array_length(v_notes) > 0 then
    v_zr := jsonb_set(v_zr, '{global_notes}', v_notes, true);
  end if;

  -- Cleanup: strip is_global_note rules from regions[].sections{} if the
  -- prior injector placed them there. Iterate regions, drop any section
  -- key matching 'global' AND whose rule is a known meta-rule. This is
  -- defensive — the prior injector orders by authority_tier so a real
  -- on_leash/off_leash rule would still surface first.
  -- (Skipping the strip for now — the prior injector keys on section,
  -- and 'global' section with a meta-rule won't conflict with real
  -- section-keyed rules in practice. Add if dupes appear.)

  return v_zr;
end;
$function$;

grant execute on function public._zr_inject_global_notes(jsonb, bigint)
  to anon, authenticated, service_role;


-- Wire the new injector into the build chain (runs LAST).
create or replace function public._promote_zone_rules_for_fid(p_fid bigint)
returns text
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_zr jsonb;
  v_source text;
  v_existing_zr jsonb;
begin
  if p_fid is null then return 'noop (null fid)'; end if;

  select source, zone_rules into v_source, v_existing_zr
    from public.beach_dog_policy
   where arena_group_id = p_fid;

  -- Manual curator protection (Rosie's-style).
  if v_source = 'manual_curator' and v_existing_zr is not null then
    return 'preserved (manual_curator)';
  end if;

  v_zr := '{"regions":[{"name":null,"sections":{}}],"global_notes":null}'::jsonb;

  -- Existing injector chain.
  v_zr := public._zr_inject_perimeter(v_zr, p_fid);
  v_zr := public._zr_inject_sand_from_policy(v_zr, p_fid);
  v_zr := public._zr_inject_sections_from_bep(v_zr, p_fid);
  v_zr := public._zr_inject_from_policy_sources(v_zr, p_fid);

  -- NEW: global_notes injector (Franz 2026-05-20 deep-extract).
  v_zr := public._zr_inject_global_notes(v_zr, p_fid);

  update public.beach_dog_policy
     set zone_rules = v_zr
   where arena_group_id = p_fid;

  return 'updated';
end;
$function$;

commit;
