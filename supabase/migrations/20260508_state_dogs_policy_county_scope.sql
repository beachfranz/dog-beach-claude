-- 20260508_state_dogs_policy_county_scope.sql
--
-- state_dogs_policy v2: add county-scoping. Some state-statute defaults
-- apply only to specific counties (the OR Beach Bill applies to ocean-
-- shore beaches in 6 coastal counties, not inland river/lake beaches in
-- Multnomah/Hood River/etc.). The previous schema's single row per
-- state forced one rule for the whole state.
--
-- New shape:
--   state_dogs_policy(state_code, county_fips_filter text[])
--   Per-county rows take priority over state-wide (county_fips_filter
--   IS NULL) rows.
--
-- OR gets two rows after this migration:
--   1. coastal — Beach Bill, off-leash=true, conf 0.65 (beats CITY 0.55)
--   2. statewide fallback — on-leash, conf 0.40 (inland default)
--
-- WA + CA keep single state-wide rows for now.

begin;

-- ── 1. Schema update ───────────────────────────────────────────────────
alter table public.state_dogs_policy
  add column if not exists county_fips_filter text[];

-- Drop old PK on state_code, allow multiple rows per state
alter table public.state_dogs_policy
  drop constraint if exists state_dogs_policy_pkey;

-- (uniqueness on (state_code, county_fips_filter) not enforced at DB
-- level — text-array-to-text isn't immutable for index expressions, and
-- the table is small enough that app-level dedup via UPSERT is fine.)

-- ── 2. Update existing OR row to be the inland fallback ────────────────
update public.state_dogs_policy
   set county_fips_filter = null,
       has_off_leash = false,
       default_rule = 'mixed',
       has_on_leash = true,
       confidence = 0.40,
       scope_notes = 'Statewide OR fallback — inland river/lake beaches. Counties not under Beach Bill jurisdiction inherit on-leash defaults from city/county rules.',
       notes = 'Inland fallback. For OR ocean-shore beaches in coastal counties, see the higher-confidence coastal row.',
       scraped_at = now()
 where state_code = 'OR' and county_fips_filter is null;

-- ── 3. Insert OR coastal-counties row (Beach Bill, off-leash) ──────────
insert into public.state_dogs_policy
  (state_code, state_name, county_fips_filter,
   dogs_allowed, default_rule, has_on_leash, has_off_leash,
   source_quote, source_url, ordinance_ref, scope_notes, confidence, notes)
values
  ('OR', 'Oregon',
   array['41007','41057','41041','41039','41011','41015'],  -- Clatsop, Tillamook, Lincoln, Lane, Coos, Curry
   'yes', 'yes', true, true,
   'Oregon Beach Bill (ORS 390.605-390.770) and OPRD policy: "Must I leash my dog on the ocean shore? The short answer is no, but the handler of any domestic animal on the ocean shore does have responsibilities." Handlers must carry a leash and put it on at the request of a park employee.',
   'https://www.oregon.gov/oprd/RPS/Pages/Pets-Beaches.aspx',
   'ORS 390.605-390.770',
   'OR Pacific coastal counties: Clatsop, Tillamook, Lincoln, Lane, Coos, Curry. Off-leash permitted under handler control on ocean shore. 6 snowy plover protection beaches (Mar 15-Sep 15) require on-leash; per-beach evidence overrides where present.',
   0.65,
   'Beach Bill applies to ocean shore from extreme low tide to vegetation line. Higher confidence than CITY agency default (0.55) so OR coastal cities resolve to off-leash.')
on conflict do nothing;
-- ON CONFLICT skipped (no unique constraint to anchor); idempotent re-runs
-- will INSERT duplicates — caller should DELETE state-coastal-row before re-run.

-- Belt + suspenders: if a duplicate slipped in, keep only the newest
delete from public.state_dogs_policy a
 where state_code = 'OR'
   and county_fips_filter = array['41007','41057','41041','41039','41011','41015']
   and exists (
     select 1 from public.state_dogs_policy b
      where b.state_code = a.state_code
        and b.county_fips_filter = a.county_fips_filter
        and b.scraped_at > a.scraped_at
   );

-- ── 4. Update emitter to honor county_fips_filter ──────────────────────
create or replace function public._emit_evidence_from_state_dogs_policy(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
security definer
as $function$
declare
  v_ins int := 0; v_upd int := 0; v_skp int := 0;
begin
  with target as (
    select g.fid as gold_fid, g.state, a.county_fips
      from public.beaches_gold g
      join public.arena a on a.fid = g.fid
     where g.is_active = true
       and (p_fid is null or g.fid = p_fid)
  ),
  -- Per-fid pick the most-specific matching rule:
  -- 1. Row with non-null county_fips_filter where the filter contains the beach's county_fips
  -- 2. Otherwise state-wide (county_fips_filter is null)
  ranked as (
    select t.gold_fid, sdp.*,
           row_number() over (partition by t.gold_fid
             order by case when sdp.county_fips_filter is not null then 0 else 1 end,
                      sdp.confidence desc) as rk
      from target t
      join public.state_dogs_policy sdp on sdp.state_code = t.state
     where sdp.county_fips_filter is null
        or t.county_fips = any(sdp.county_fips_filter)
  ),
  picks as (select * from ranked where rk = 1),
  payload as (
    select gold_fid,
           source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', dogs_allowed,
             'default_rule', default_rule,
             'leash_required',
               case when has_on_leash is true and not coalesce(has_off_leash, false)
                    then 'on_leash'
                    when has_off_leash is true then 'off_leash'
                    else null end,
             'off_leash_exists', case when has_off_leash is not null
                                       then has_off_leash::text else null end,
             'source_quote', source_quote,
             'ordinance_ref', ordinance_ref,
             'scope_notes', scope_notes
           )) as claimed_values,
           coalesce(confidence, 0.40) as confidence
      from picks
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values,
       confidence, is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'state_dogs_policy_v1', source_url, claimed_values,
           confidence, false, 'derived_url_crawl', 'beach_access', now()
    from payload where claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set source_url = excluded.source_url,
          claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          updated_at = now()
      where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
         or beach_enrichment_provenance.confidence is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into v_ins, v_upd, v_skp from upserted;
  return query select v_ins::bigint, v_upd::bigint, v_skp::bigint;
end;
$function$;

alter function public._emit_evidence_from_state_dogs_policy(bigint)
  set statement_timeout = '120s';

commit;
