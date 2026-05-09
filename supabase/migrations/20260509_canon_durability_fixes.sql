-- 20260509_canon_durability_fixes.sql
--
-- Closes the three remaining durability gaps surfaced by RI launch:
--
--   GAP 1 — NOAA station coverage: nothing in the canon asserts that
--           promoted beaches have noaa_station_id set. Issue #21 (RI's
--           daily_refresh_fire halt) was caused by Pacific-only NOAA
--           data; if someone drops/truncates noaa_stations, or a new
--           state launches into a region without coverage, the canon
--           today silently produces NULL station IDs and fails 22 phases
--           later at daily_refresh_fire.
--
--   GAP 2 — Populator-chain regression vulnerability: issues #2, #18,
--           and #20 are all the same pattern — `promote_to_gold` (or
--           `refire_bep_cascade`) gets re-created in a later migration
--           and silently drops a column from the INSERT or a populator
--           call from the foreach loop. No automated guardrail catches
--           this. Each regression has cost a state-launch debugging
--           cycle. Fix: a stored function that asserts the EXPECTED
--           populator/column set is present, callable as a phase
--           criterion in the canon.
--
--   GAP 3 — NOAA stations not registered in external_source_status:
--           precheck validates 4 sources (pad_us, osm_landing,
--           osm_amenities, tiger_places) but NOAA isn't one. Add it.
--
-- Together these mean: the next state launch will halt at the EARLIEST
-- point any of these gaps surface, with a clear remediation message.

begin;

-- ── GAP 1A: register NOAA stations in external_source_status ────────────
--
-- noaa_stations is a global (not per-state) dataset, but
-- external_source_status is keyed by (source, state). Use state='*'
-- as the global marker for nationwide loaders.

insert into public.external_source_status
  (source, state, status, row_count, last_loaded_at, notes)
values
  ('noaa_stations', '*',
   case when (select count(*) from public.noaa_stations) >= 3000
        then 'ok' else 'partial' end,
   (select count(*)::int from public.noaa_stations),
   now(),
   'Loaded from api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json. '
   '3,449 stations expected nationwide. Use state=''*'' as the global marker.')
on conflict (source, state) do update set
  status = excluded.status,
  row_count = excluded.row_count,
  last_loaded_at = excluded.last_loaded_at,
  notes = excluded.notes;


-- ── GAP 1B: precheck now also requires noaa_stations is loaded ──────────

create or replace function public.assert_state_upstream_loaded(p_state text)
returns table(source text, status text, row_count int, last_loaded_at timestamptz)
language plpgsql
as $$
declare
  v_required text[] := array['pad_us', 'osm_landing', 'osm_amenities', 'tiger_places'];
  v_missing  text[] := array[]::text[];
  v_src text;
  v_noaa_status text;
  v_noaa_rows int;
begin
  -- Per-state required sources
  foreach v_src in array v_required loop
    if not exists (
      select 1 from public.external_source_status ess
       where ess.source = v_src and ess.state = p_state
         and ess.status in ('ok','skipped')
    ) then
      v_missing := v_missing || v_src;
    end if;
  end loop;

  -- Global noaa_stations check (state='*')
  select ess.status, ess.row_count into v_noaa_status, v_noaa_rows
    from public.external_source_status ess
   where ess.source = 'noaa_stations' and ess.state = '*';
  if v_noaa_status is null or v_noaa_status not in ('ok','skipped') or coalesce(v_noaa_rows, 0) < 3000 then
    v_missing := v_missing || 'noaa_stations(global)';
  end if;

  if array_length(v_missing, 1) > 0 then
    raise exception
      'state %: missing upstream sources: %. For noaa_stations(global): '
      'load via api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json'
      '?type=tidepredictions then call public.load_noaa_stations_batch(jsonb).',
      p_state, v_missing;
  end if;

  return query
    select s.source, s.status, s.row_count, s.last_loaded_at
      from public.external_source_status s
     where (s.state = p_state and s.source = any(v_required))
        or (s.state = '*' and s.source = 'noaa_stations')
     order by s.source;
end $$;


-- ── GAP 1C: noaa_station_id check at promote phase ──────────────────────
-- The canon's `promote` phase criterion is currently:
--   "every active beach in state has county_fips set"
-- We extend this to also require noaa_station_id IS NOT NULL on every
-- active beach. If station coverage gap exists for this state, halt
-- here (immediately after promote) instead of 18 phases downstream.

create or replace function public.assert_promote_complete_for_state(p_state text)
returns boolean
language plpgsql
stable
as $$
declare
  v_missing_cfips int;
  v_missing_noaa  int;
  v_total         int;
begin
  select count(*),
         count(*) filter (where county_fips is null),
         count(*) filter (where noaa_station_id is null)
    into v_total, v_missing_cfips, v_missing_noaa
    from public.beaches_gold
   where state = p_state and is_active;

  if v_missing_cfips > 0 then
    raise exception
      'state %: % of % active beaches have NULL county_fips after promote. '
      'See issue #2/#18 (county_fips populator drift).',
      p_state, v_missing_cfips, v_total;
  end if;

  if v_missing_noaa > 0 then
    raise exception
      'state %: % of % active beaches have NULL noaa_station_id after promote. '
      'See issue #21 (NOAA station coverage gap). Likely cause: '
      'noaa_stations table missing rows for this region. Backfill via: '
      'UPDATE public.beaches_gold g SET noaa_station_id = '
      'public._nearest_noaa_station(g.geom::geography, 100) '
      'WHERE g.is_active AND g.state = ''%'' AND g.noaa_station_id IS NULL;',
      p_state, v_missing_noaa, v_total, p_state;
  end if;

  return true;
end $$;


-- ── GAP 2: populator-chain integrity assertion ──────────────────────────
-- Asserts that promote_to_gold and refire_bep_cascade contain ALL the
-- expected populator calls. If a future migration silently drops one,
-- this function flags it. Used as a SQL phase criterion (see runbook).

create or replace function public.assert_populator_chains_intact()
returns boolean
language plpgsql
stable
as $$
declare
  v_promote_src text;
  v_refire_src  text;
  v_required text[] := array[
    'populate_polygon_containment_gold',
    'populate_from_cpad_gold',
    'populate_from_pad_us_gold',
    'populate_from_park_operators_gold',
    'populate_from_operators_gold',
    'populate_from_state_default_gold',  -- ★ issue #20 lesson
    'populate_from_research_gold',
    'populate_from_park_url_gold',
    'populate_from_park_url_governance_gold',
    'populate_from_unified_v1_gold',
    'populate_from_city_dog_policy_gold',
    'populate_from_county_dog_policy_gold',
    '_emit_evidence_from_osm_amenities'
  ];
  v_required_columns text[] := array[
    'county_fips',  -- ★ issue #2/#18 lesson
    'park_name',    -- ★ issue #2 lesson
    'state',
    'location_id',
    'name'
  ];
  v_fn text;
  v_col text;
  v_missing text[] := array[]::text[];
begin
  -- Fetch function source for both chains
  select pg_get_functiondef(p.oid) into v_promote_src
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'public' and p.proname = 'promote_to_gold'
   limit 1;

  select pg_get_functiondef(p.oid) into v_refire_src
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'public' and p.proname = 'refire_bep_cascade'
   limit 1;

  if v_promote_src is null then
    raise exception 'public.promote_to_gold not found';
  end if;
  if v_refire_src is null then
    raise exception 'public.refire_bep_cascade not found';
  end if;

  -- Each populator must appear in BOTH chains
  foreach v_fn in array v_required loop
    if v_promote_src not ilike '%' || v_fn || '%' then
      v_missing := v_missing || ('promote_to_gold missing: ' || v_fn);
    end if;
    if v_refire_src not ilike '%' || v_fn || '%' then
      v_missing := v_missing || ('refire_bep_cascade missing: ' || v_fn);
    end if;
  end loop;

  -- Each required INSERT column must appear in promote_to_gold
  foreach v_col in array v_required_columns loop
    if v_promote_src not ilike '%' || v_col || '%' then
      v_missing := v_missing || ('promote_to_gold INSERT missing column: ' || v_col);
    end if;
  end loop;

  if array_length(v_missing, 1) > 0 then
    raise exception
      'populator chain integrity FAIL — % drift(s) detected: %',
      array_length(v_missing, 1), v_missing;
  end if;

  return true;
end $$;


grant execute on function public.assert_state_upstream_loaded(text)        to anon, authenticated, service_role;
grant execute on function public.assert_promote_complete_for_state(text)   to anon, authenticated, service_role;
grant execute on function public.assert_populator_chains_intact()          to anon, authenticated, service_role;

commit;
