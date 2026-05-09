-- 20260509_codify_corrective_actions.sql
--
-- Codifies four corrective/cleanup actions Franz and Claude ran inline
-- tonight (2026-05-08/09) into reusable SQL functions so they can be
-- invoked as gated phases of the canonical orchestrator.
--
-- Functions added:
--   purge_cross_state_extractions(state)  — flip pass_a_policy_found=false on
--     operator_policy_extractions whose source_url is a known wrong-state
--     domain. Idempotent.
--   strip_plus_codes_from_addresses(state) — clean leading Plus codes
--     (e.g. "XV44+XJ Florence, OR, USA" -> "Florence, OR, USA") on
--     beaches_gold.address. Idempotent.
--   align_is_scoreable_to_tier(state) — set is_scoreable=true for tier
--     1_off-leash and 2_on-leash beaches; false for 3+4 / unknown.
--     Replaces the inline is_scoreable flip we did for OR/WA.
--   purge_bogus_operator_bep(state) — delete operator_*/section_research_v1
--     BEP rows for active beaches in the state, prep for clean refire.

begin;

-- ── 1. Purge cross-state-pollution extractions ─────────────────────
create or replace function public.purge_cross_state_extractions(p_state text)
returns table(rows_purged int, examples_text text)
language plpgsql as $$
declare v_n int;
        v_examples text;
        -- Domain hints: URL substring -> the state it really belongs to.
        v_hints constant jsonb := jsonb_build_array(
          jsonb_build_object('needle','.ca.gov',          'state','CA'),
          jsonb_build_object('needle','.ca.us',           'state','CA'),
          jsonb_build_object('needle','parks.ca.gov',     'state','CA'),
          jsonb_build_object('needle','cityofpacifica',   'state','CA'),
          jsonb_build_object('needle','coronado.ca',      'state','CA'),
          jsonb_build_object('needle','lakeforestca',     'state','CA'),
          jsonb_build_object('needle','carpinteriaca',    'state','CA'),
          jsonb_build_object('needle','smcgov',           'state','CA'),
          jsonb_build_object('needle','countyofkingsca',  'state','CA'),
          jsonb_build_object('needle','cityofmyrtlebeach','state','SC'),
          jsonb_build_object('needle','blainemn',         'state','MN'),
          jsonb_build_object('needle','dsm.city',         'state','IA'),
          jsonb_build_object('needle','raymond.ca',       'state','XX'),
          jsonb_build_object('needle','.fl.us',           'state','FL'),
          jsonb_build_object('needle','.fl.gov',          'state','FL'),
          jsonb_build_object('needle','.ny.gov',          'state','NY'),
          jsonb_build_object('needle','.ny.us',           'state','NY'),
          jsonb_build_object('needle','.tx.us',           'state','TX')
        );
begin
  with bogus as (
    select ope.id
      from public.operator_policy_extractions ope
      join public.operators op on op.id = ope.operator_id
      cross join lateral jsonb_array_elements(v_hints) hint
     where op.state_code = p_state
       and op.is_active
       and ope.pass_a_policy_found = true
       and lower(coalesce(ope.source_url,'')) like '%' || (hint->>'needle') || '%'
       and (hint->>'state') <> p_state
  )
  update public.operator_policy_extractions
     set pass_a_policy_found = false,
         pass_a_status = 'cross_state_purged'
   where id in (select id from bogus);
  get diagnostics v_n = row_count;

  select string_agg(format('%s: %s', op.canonical_name, ope.source_url), ' | ' order by ope.id)
    into v_examples
    from public.operator_policy_extractions ope
    join public.operators op on op.id = ope.operator_id
   where op.state_code = p_state and ope.pass_a_status = 'cross_state_purged'
   limit 5;

  return query select v_n, coalesce(v_examples, '');
end $$;


-- ── 2. Strip Plus-code prefixes from addresses ─────────────────────
create or replace function public.strip_plus_codes_from_addresses(p_state text)
returns int
language plpgsql as $$
declare v_n int;
begin
  -- Plus code pattern: 4-8 chars (no I,L,O,U,0,1) + '+' + 2+ chars + space.
  update public.beaches_gold
     set address = regexp_replace(
                     address,
                     '^[2-9CFGHJMPQRVWX]{4,}\+[2-9CFGHJMPQRVWX]+\s+',
                     '',
                     'i'
                   )
   where state = p_state
     and is_active
     and address ~* '^[2-9CFGHJMPQRVWX]{4,}\+[2-9CFGHJMPQRVWX]+\s+';
  get diagnostics v_n = row_count;
  return v_n;
end $$;


-- ── 3. Align is_scoreable to Location Tier ─────────────────────────
-- Per canon (2026-05-09): scoring scope = Location Tiers 1 + 2.
-- Sets is_scoreable=true for tier 1_off-leash and 2_on-leash beaches.
-- Sets is_scoreable=false for tier 3_limited_access, 4_no_dogs, unknown.
create or replace function public.align_is_scoreable_to_tier(p_state text)
returns table(promoted int, demoted int)
language plpgsql as $$
declare v_promoted int := 0; v_demoted int := 0;
begin
  -- Promote: tier 1+2 active beaches not yet scoreable
  update public.beaches_gold g
     set is_scoreable = true
    from public.beach_dog_policy bdp
   where bdp.arena_group_id = g.fid
     and g.state = p_state
     and g.is_active
     and not g.is_scoreable
     and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text)
         in ('1_off-leash','2_on-leash');
  get diagnostics v_promoted = row_count;

  -- Demote: tier 3/4/unknown beaches that ARE scoreable
  update public.beaches_gold g
     set is_scoreable = false
    from public.beach_dog_policy bdp
   where bdp.arena_group_id = g.fid
     and g.state = p_state
     and g.is_active
     and g.is_scoreable
     and public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text)
         not in ('1_off-leash','2_on-leash');
  get diagnostics v_demoted = row_count;

  return query select v_promoted, v_demoted;
end $$;


-- ── 4. Purge BEP rows from operator path (prep for refire) ─────────
-- Deletes BEP rows from operator_* + section_research_v1 sources for
-- all active state beaches. Used after a cross-state purge to wipe
-- polluted evidence before refire_bep_cascade rebuilds it cleanly.
create or replace function public.purge_bogus_operator_bep(p_state text)
returns int
language plpgsql as $$
declare v_n int;
begin
  delete from public.beach_enrichment_provenance bep
   using public.beaches_gold g
   where bep.gold_fid = g.fid
     and g.is_active and g.state = p_state
     and bep.source in (
       'operator_city','operator_county','operator_pad_us',
       'section_research_v1',
       'operator_amenities_v1','operator_dogs_policy_v1',
       'operator_policy_exceptions_v1'
     );
  get diagnostics v_n = row_count;
  return v_n;
end $$;


grant execute on function public.purge_cross_state_extractions(text)         to anon, authenticated, service_role;
grant execute on function public.strip_plus_codes_from_addresses(text)       to anon, authenticated, service_role;
grant execute on function public.align_is_scoreable_to_tier(text)            to anon, authenticated, service_role;
grant execute on function public.purge_bogus_operator_bep(text)              to anon, authenticated, service_role;

commit;
