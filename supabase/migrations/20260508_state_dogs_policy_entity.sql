-- 20260508_state_dogs_policy_entity.sql
--
-- Promote "state" to a first-class entity in the dog-policy hierarchy,
-- parallel to operators and CPAD/PAD-US units. Each state gets a
-- single row of statute/doctrine-based default dog rules. Lowest-
-- priority fallback (0.40 confidence) — fires for any active gold
-- beach with no per-beach, per-operator, per-unit, or agency-class
-- evidence.
--
-- Replaces the OR-specific _emit_or_state_statute_dogs_default.
-- Seeded with OR (Beach Bill), WA (Public Trust + SMA), CA (default
-- on-leash where coastal access is public). Other states added as
-- needed.
--
-- Wired into populate_from_state_default_gold(p_fid) so promote_to_gold's
-- foreach loop picks it up on every state launch.

begin;

-- ── 1. The entity table ────────────────────────────────────────────────
create table if not exists public.state_dogs_policy (
  state_code     text primary key,
  state_name     text,
  dogs_allowed   text check (dogs_allowed in ('yes','no','mixed','unknown')),
  default_rule   text check (default_rule in ('yes','no','mixed','unknown')),
  has_on_leash   boolean,
  has_off_leash  boolean,
  source_quote   text,
  source_url     text,
  ordinance_ref  text,
  scope_notes    text,
  confidence     numeric default 0.40,
  scraped_at     timestamptz default now(),
  notes          text
);

grant select on public.state_dogs_policy to anon, authenticated, service_role;

-- ── 2. Seed rows ───────────────────────────────────────────────────────
-- OR — Oregon Beach Bill (ORS 390.605-390.770) + OPRD on-leash rule
insert into public.state_dogs_policy
  (state_code, state_name, dogs_allowed, default_rule, has_on_leash, has_off_leash,
   source_quote, source_url, ordinance_ref, scope_notes, notes)
values
  ('OR', 'Oregon', 'yes', 'mixed', true, false,
   'Oregon Beach Bill (ORS 390.605-390.770) — dry sand seaward of vegetation line is public; OPRD permits dogs on leash on most ocean-shore beaches unless locally posted.',
   'https://www.oregon.gov/oprd/RPS/Pages/Pets-Beaches.aspx',
   'ORS 390.605-390.770',
   'Ocean shore only. Inland river/lake beaches inherit county/city rules.',
   'Lowest-priority dogs fallback for OR beaches outside any PAD-US polygon.'),

  ('WA', 'Washington', 'yes', 'mixed', true, false,
   'Washington Public Trust Doctrine + Shoreline Management Act (RCW 90.58) — tidelands and shoreline below the ordinary high-water mark are public. Most state and local jurisdictions permit dogs on leash; off-leash requires designated zone.',
   'https://parks.wa.gov/about/policies-rules/pets',
   'RCW 90.58',
   'Tidelands and shoreline below ordinary high-water mark. Inland and uplands inherit county/city rules.',
   'Lowest-priority dogs fallback for WA beaches outside any PAD-US polygon.'),

  ('CA', 'California', 'mixed', 'mixed', true, false,
   'California Coastal Act (Public Resources Code §30000 et seq.) — California beaches between mean high tide and the toe of the bluff are public. Local jurisdictions vary widely; most municipal beaches permit leashed dogs in restricted hours, many state beaches prohibit.',
   'https://www.parks.ca.gov/?page_id=23973',
   'Public Resources Code §30000 et seq.',
   'Coastal beaches; per-jurisdiction rules dominate.',
   'CA usually has richer per-operator/unit signals; this is final fallback only.')
on conflict (state_code) do update
  set state_name = excluded.state_name,
      dogs_allowed = excluded.dogs_allowed,
      default_rule = excluded.default_rule,
      has_on_leash = excluded.has_on_leash,
      has_off_leash = excluded.has_off_leash,
      source_quote = excluded.source_quote,
      source_url = excluded.source_url,
      ordinance_ref = excluded.ordinance_ref,
      scope_notes = excluded.scope_notes,
      notes = excluded.notes,
      scraped_at = now();

-- ── 3. Emitter ─────────────────────────────────────────────────────────
create or replace function public._emit_evidence_from_state_dogs_policy(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
security definer
as $function$
declare
  v_ins int := 0;
  v_upd int := 0;
  v_skp int := 0;
begin
  with payload as (
    select g.fid as gold_fid,
           sdp.state_code,
           sdp.source_quote,
           sdp.source_url,
           sdp.ordinance_ref,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', sdp.dogs_allowed,
             'default_rule', sdp.default_rule,
             'leash_required',
               case when sdp.has_on_leash is true and not coalesce(sdp.has_off_leash, false)
                    then 'on_leash'
                    when sdp.has_off_leash is true then 'off_leash'
                    else null end,
             'off_leash_exists', case when sdp.has_off_leash is not null
                                       then sdp.has_off_leash::text else null end,
             'source_quote', sdp.source_quote,
             'ordinance_ref', sdp.ordinance_ref,
             'scope_notes', sdp.scope_notes
           )) as claimed_values,
           coalesce(sdp.confidence, 0.40) as confidence
      from public.beaches_gold g
      join public.state_dogs_policy sdp on sdp.state_code = g.state
     where g.is_active = true
       and (p_fid is null or g.fid = p_fid)
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values,
       confidence, is_canonical, extraction_type, cpad_role, updated_at)
    select p.gold_fid, 'dogs', 'state_dogs_policy_v1', p.source_url, p.claimed_values,
           p.confidence, false, 'derived_url_crawl', 'beach_access', now()
    from payload p
    where p.claimed_values <> '{}'::jsonb
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

grant execute on function public._emit_evidence_from_state_dogs_policy(bigint) to anon, authenticated, service_role;

alter function public._emit_evidence_from_state_dogs_policy(bigint)
  set statement_timeout = '120s';

-- ── 4. Wire into promote_to_gold's populator loop ──────────────────────
-- Just a wrapper named populate_from_state_default_gold for symmetry with
-- the other populate_from_X_gold functions.

create or replace function public.populate_from_state_default_gold(p_fid bigint)
returns void
language plpgsql
as $function$
begin
  perform public._emit_evidence_from_state_dogs_policy(p_fid);
end;
$function$;

grant execute on function public.populate_from_state_default_gold(bigint) to anon, authenticated, service_role;

alter function public.populate_from_state_default_gold(bigint)
  set statement_timeout = '120s';

-- Re-create promote_to_gold to add populate_from_state_default_gold to
-- the foreach loop (after pad_us, before research). Same body otherwise.

create or replace function public.promote_to_gold(
  p_fids bigint[], p_score boolean default false, p_publish boolean default true
)
returns table(
  rows_promoted bigint, rows_already_in_gold bigint,
  containment_evidence bigint, source_evidence bigint,
  beach_dog_policy_changed bigint, beach_amenities_changed bigint,
  noaa_assigned bigint, slugs_assigned bigint
)
language plpgsql
as $function$
declare promoted int := 0; existing int := 0; fid_iter bigint;
        contain_ev int := 0; source_ev int := 0;
        bdp_changed int := 0; ba_changed int := 0;
        noaa_count int := 0; slug_count int := 0;
        new_fids bigint[];
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint;
    return;
  end if;

  perform set_config('app.promote_to_gold_active', 'true', true);

  with to_promote as (
    select a.* from public.arena a
     where a.fid = any(p_fids) and a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
       and not exists (
         select 1 from public.beaches_gold g
         join public.arena a2 on a2.fid = g.fid
         where a2.group_id = coalesce(a.group_id, a.fid) and g.fid <> a.fid
       )
  ),
  to_promote_with_canonical as (
    select t.*,
           coalesce(osm.nav_lat, t.nav_lat, t.lat) as canonical_lat,
           coalesce(osm.nav_lon, t.nav_lon, t.lon) as canonical_lon
      from to_promote t
      left join lateral (
        select nav_lat, nav_lon from public.arena
         where group_id = coalesce(t.group_id, t.fid)
           and source_code = 'osm' and is_active = true
           and nav_lat is not null and nav_lon is not null
         limit 1
      ) osm on true
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name, source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source, park_name, state, promoted_from, promoted_at,
       is_active, noaa_station_id, timezone, open_time, close_time, is_scoreable, geom)
    select t.fid, public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.canonical_lat, t.canonical_lon, t.county_name, t.source_code, t.source_id,
      coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source, null,
      coalesce(
        public._infer_state_from_county_fips(t.county_fips),
        public._infer_state_from_county(t.county_name)
      ),
      'promote_to_gold_v3', now(), true,
      public._nearest_noaa_station(st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)::geography),
      'America/Los_Angeles', '05:00', '22:00', coalesce(p_score, false),
      st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)
    from to_promote_with_canonical t
    returning fid, location_id, noaa_station_id
  )
  select array_agg(fid), count(*), count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into new_fids, promoted, slug_count, noaa_count from ins;
  existing := array_length(p_fids, 1) - promoted;

  foreach fid_iter in array p_fids loop
    perform public.populate_polygon_containment_gold(fid_iter);
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_pad_us_gold(fid_iter);
    perform public.populate_from_park_operators_gold(fid_iter);
    perform public.populate_from_state_default_gold(fid_iter);    -- ★ new
    perform public.populate_from_research_gold(fid_iter);
    perform public.populate_from_park_url_gold(fid_iter);
    perform public.populate_from_park_url_governance_gold(fid_iter);
    perform public.populate_from_unified_v1_gold(fid_iter);
    perform public.populate_from_city_dog_policy_gold(fid_iter);
    perform public.populate_from_county_dog_policy_gold(fid_iter);
    perform public._resolve_polygon_containment(fid_iter);
    perform public._resolve_governance_gold(fid_iter);
    perform public._resolve_dogs_gold(fid_iter);
    perform public._resolve_practical_gold(fid_iter);
    perform public._resolve_field_group_gold('access', fid_iter);
  end loop;

  select count(*) into contain_ev from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  if p_publish then
    foreach fid_iter in array p_fids loop
      perform public.compute_beach_field_consensus(fid_iter);
      perform public.promote_canonical_to_consumer_tables(fid_iter);
    end loop;
    select count(*) into bdp_changed from public.beach_dog_policy
     where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
    select count(*) into ba_changed from public.beach_amenities
     where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
  end if;

  perform set_config('app.promote_to_gold_active', 'false', true);

  return query select promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
                      bdp_changed::bigint, ba_changed::bigint, noaa_count::bigint, slug_count::bigint;
end;
$function$;

alter function public.promote_to_gold(bigint[], boolean, boolean)
  set statement_timeout = '900s';

-- ── 5. Retire OR-specific function (keep as no-op for back-compat) ─────
create or replace function public._emit_or_state_statute_dogs_default(p_fid bigint default null)
returns table(rows_inserted bigint, rows_skipped_existing bigint)
language plpgsql
security definer
as $function$
begin
  -- Superseded by _emit_evidence_from_state_dogs_policy. Kept as a
  -- compatibility shim returning zero so any old callers don't error.
  return query select 0::bigint, 0::bigint;
end;
$function$;

commit;
