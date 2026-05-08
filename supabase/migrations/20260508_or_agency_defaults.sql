-- 20260508_or_agency_defaults.sql
--
-- Extend the pad_us agency-class default playbook (already covers BLM,
-- USFS, NPS, USACE, USFWS) to the codes that show up on OR PAD-US data:
--   FWS  — 3-letter Fish & Wildlife code (USFWS-equivalent)
--   CITY — city-managed parks (Tier 1c on-leash by default)
--   CNTY — county-managed parks (Tier 1c on-leash by default)
--   SPR  — State Park (e.g. OPRD, Oregon Parks and Recreation Dept)
--   REG  — Regional (port districts, regional rec authorities) — on-leash
--   NGO  — Non-government org / land trust (varies, conservative on-leash)
--
-- These are agency-class defaults: rows with unit_id IS NULL match by
-- mng_name in the emitter. Per-unit overrides still win when present.
--
-- Confidence in the emitter is 0.55 for these (vs 0.70 per-unit).
--
-- Plus: a state-statute fallback for OR active beaches with no PAD-US
-- containment match. ORS 390.605–390.770 (Oregon Beach Bill) — the dry
-- sand from the line of vegetation seaward is public; OPRD allows dogs
-- on most ocean-shore beaches under leash unless locally posted otherwise.

begin;

-- ── Agency-class defaults (mng_name match, unit_id IS NULL) ─────────────
insert into public.pad_us_unit_dogs_policy
  (unit_id, mng_name, dogs_allowed, default_rule, leash_required,
   area_sand, area_water, area_trails, area_campground,
   source_quote, notes, scraped_at)
values
  (null, 'FWS',  'no',    'no',    null,
   'forbidden', 'forbidden', 'forbidden', 'forbidden',
   'USFWS general rule (3-letter code variant): pets prohibited on most National Wildlife Refuges except in vehicles; some refuges allow leashed dogs on designated trails only.',
   'Agency-class default. Matches FWS 3-letter manager code on PAD-US-OR.',
   now()),
  (null, 'CITY', 'yes',   'mixed', true,
   'on_leash', 'on_leash', 'on_leash', null,
   'City-managed parks generally permit dogs on leash. Per-jurisdiction overrides may apply (some cities ban dogs from beaches seasonally).',
   'Agency-class default. Tier 1c on-leash unless overridden.',
   now()),
  (null, 'CNTY', 'yes',   'mixed', true,
   'on_leash', 'on_leash', 'on_leash', null,
   'County-managed parks generally permit dogs on leash. Per-jurisdiction overrides may apply.',
   'Agency-class default. Tier 1c on-leash unless overridden.',
   now()),
  (null, 'SPR',  'yes',   'mixed', true,
   'on_leash', 'on_leash', 'on_leash', 'on_leash',
   'State Park default — Oregon Parks and Recreation (OPRD) and most state parks permit dogs on leash year-round, including ocean beaches under their management.',
   'Agency-class default. SPR = "State Park" code on PAD-US.',
   now()),
  (null, 'REG',  'yes',   'mixed', true,
   'on_leash', 'on_leash', 'on_leash', null,
   'Regional authority (port districts, regional park districts) — generally on-leash dogs permitted unless locally posted.',
   'Agency-class default. Per-unit posting overrides expected.',
   now()),
  (null, 'NGO',  'mixed', 'mixed', true,
   'on_leash', null, 'on_leash', null,
   'Non-government conservation orgs and land trusts — typically allow dogs on leash on trails, restrict on sensitive habitat. Per-unit overrides expected.',
   'Agency-class default. Conservative on-leash.',
   now())
on conflict do nothing;

-- ── State-statute fallback emitter for OR (no PAD-US containment) ───────
-- Writes a low-confidence (0.40) dogs BEP row for any active OR gold beach
-- that doesn't already have a dogs BEP row. Uses the Oregon Beach Bill +
-- OPRD ocean-shore rule as the basis. Idempotent — only fires when no
-- other dogs evidence exists.

create or replace function public._emit_or_state_statute_dogs_default(p_fid bigint default null)
returns table(rows_inserted bigint, rows_skipped_existing bigint)
language plpgsql
security definer
as $function$
declare v_ins bigint := 0; v_eligible bigint := 0;
begin
  -- Count eligible OR fids (no dogs BEP yet)
  select count(*) into v_eligible
    from public.beaches_gold g
   where g.state = 'OR' and g.is_active = true
     and (p_fid is null or g.fid = p_fid)
     and not exists (
       select 1 from public.beach_enrichment_provenance bep
        where bep.gold_fid = g.fid and bep.field_group = 'dogs'
     );

  with candidates as (
    select g.fid
      from public.beaches_gold g
     where g.state = 'OR' and g.is_active = true
       and (p_fid is null or g.fid = p_fid)
       and not exists (
         select 1 from public.beach_enrichment_provenance bep
          where bep.gold_fid = g.fid and bep.field_group = 'dogs'
       )
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values,
       confidence, is_canonical, extraction_type, cpad_role, updated_at)
    select c.fid, 'dogs', 'or_state_statute_v1', null,
      jsonb_build_object(
        'allowed', 'yes',
        'default_rule', 'on_leash',
        'leash_required', 'on_leash',
        'off_leash_exists', 'false',
        'source_quote',
          'Oregon Beach Bill (ORS 390.605-390.770) - dry sand seaward of vegetation line is public; OPRD permits dogs on leash on most ocean-shore beaches unless locally posted.'
      ),
      0.40, false, 'derived_url_crawl', 'beach_access', now()
    from candidates c
    on conflict (gold_fid, field_group, source) do nothing
    returning 1
  )
  select count(*) into v_ins from ins;
  return query select v_ins, greatest(v_eligible - v_ins, 0::bigint);
end;
$function$;

grant execute on function public._emit_or_state_statute_dogs_default(bigint) to anon, authenticated, service_role;

commit;
