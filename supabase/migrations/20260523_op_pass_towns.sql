-- 20260523_op_pass_towns.sql
--
-- New operator promoter for town-strong states: _op_pass1b_towns lifts
-- active TIGER county-subdivision towns (CLASSFP='T1') into
-- public.operators with level='town'. Wired into
-- populate_operators_for_state right after _op_pass1_cities.
--
-- Per Franz directive 2026-05-23 LATE ("be sure to model mcds using
-- same process as we use for C1s"). This mirrors _op_pass1_cities
-- shape — same slug+aliases+conflict pattern, just sourced from
-- tiger_county_subdivisions instead of jurisdictions.
--
-- Gated by public.state_government_strength.dominant_municipal_tier =
-- 'town' (NH/CT/MA/ME/RI/VT/NJ/PA). In county-strong states the
-- function returns 0 without scanning. Forward-compatible: if a state
-- gets reclassified as town-strong later, the COUSUB data + this
-- function are ready to fire.

BEGIN;

-- ── 1. ALTER CHECK constraints to allow level='town' + new origin ───
ALTER TABLE public.operators
  DROP CONSTRAINT IF EXISTS operators_level_check;

ALTER TABLE public.operators
  ADD CONSTRAINT operators_level_check
  CHECK (level = ANY (ARRAY[
    'federal', 'state', 'tribal', 'county', 'city',
    'town',                          -- NEW: MCD towns/townships/boroughs
    'special-district', 'private', 'joint', 'unknown'
  ]));

ALTER TABLE public.operators
  DROP CONSTRAINT IF EXISTS operators_origin_source_check;

ALTER TABLE public.operators
  ADD CONSTRAINT operators_origin_source_check
  CHECK (origin_source = ANY (ARRAY[
    'cpad', 'tiger_places', 'tiger_counties',
    'tiger_county_subdivisions',     -- NEW: the towns promoter
    'seed_federal', 'seed_tribal', 'manual', 'seed_federal_widened',
    'seed_state_pad_us', 'seed_district_pad_us', 'seed_ngo_pad_us',
    'seed_pvt_pad_us', 'seed_tribal_pad_us', 'seed_joint_pad_us',
    'seed_tribal_bia', 'seed_osm_operator', 'seed_bep_operator_name'
  ]));

-- ── 2. New _op_pass1b_towns function ─────────────────────────────────
CREATE OR REPLACE FUNCTION public._op_pass1b_towns(p_state text)
RETURNS integer
LANGUAGE plpgsql
AS $function$
declare v_n int := 0;
begin
  -- Gate: only town-strong states.
  IF NOT EXISTS (
    SELECT 1 FROM public.state_government_strength
     WHERE state_code = p_state
       AND dominant_municipal_tier = 'town'
  ) THEN
    RETURN 0;
  END IF;

  with towns_with_dupe as (
    select tcs.id, tcs.name, tcs.fips_state, tcs.fips_county, tcs.fips_cousub,
           count(*) over (partition by tcs.state, tcs.name) as dupe_count
      from public.tiger_county_subdivisions tcs
     where tcs.state = p_state
       and tcs.classfp = 'T1'   -- active towns/townships only
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      fips_state, fips_place, state_code, origin_source
    )
    select
      case
        when t.dupe_count > 1
          then public.slugify_agency('Town of ' || t.name) || '-fc' || t.fips_county
        else public.slugify_agency('Town of ' || t.name)
      end as slug,
      'Town of ' || t.name, t.name,
      array['Town of ' || t.name, t.name, t.name || ', Town of', t.name || ' Town'],
      'town', 'town',
      t.fips_state, t.fips_cousub, p_state, 'tiger_county_subdivisions'
    from towns_with_dupe t
    on conflict (slug) do update set
      fips_state      = excluded.fips_state,
      fips_place      = excluded.fips_place,
      aliases         = (select array_agg(distinct a)
                         from unnest(public.operators.aliases || excluded.aliases) a),
      updated_at      = now()
    returning 1
  )
  select count(*) into v_n from ins;
  return v_n;
end $function$;

GRANT EXECUTE ON FUNCTION public._op_pass1b_towns(text) TO service_role;

-- ── 3. Update populate_operators_for_state to call the towns pass ────
-- Add `towns_added` as an OUT parameter, call _op_pass1b_towns right
-- after _op_pass1_cities.
DROP PROCEDURE IF EXISTS public.populate_operators_for_state(text);

CREATE OR REPLACE PROCEDURE public.populate_operators_for_state(
  IN p_state text,
  OUT cities_added integer,
  OUT towns_added integer,
  OUT counties_added integer,
  OUT federal_added integer,
  OUT state_added integer,
  OUT district_added integer,
  OUT ngo_added integer,
  OUT pvt_added integer,
  OUT tribal_added integer,
  OUT joint_added integer,
  OUT bia_added integer,
  OUT osm_added integer,
  OUT bep_added integer,
  OUT linkage_updates integer,
  OUT consolidated_count integer
)
LANGUAGE plpgsql
AS $procedure$
declare
  v_skip_non_ca boolean := (p_state = 'CA');
begin
  if public._op_state_fp(p_state) is null then
    raise exception 'unknown state code: %', p_state;
  end if;

  cities_added       := public._op_pass1_cities(p_state);    commit;
  towns_added        := public._op_pass1b_towns(p_state);    commit;
  counties_added     := public._op_pass2_counties(p_state);  commit;
  federal_added      := public._op_pass3_federal(p_state);   commit;

  if v_skip_non_ca then
    state_added := 0; district_added := 0;
    ngo_added := 0; pvt_added := 0;
    tribal_added := 0; joint_added := 0;
    bia_added := 0; osm_added := 0; bep_added := 0;
  else
    state_added    := public._op_pass4_state(p_state);     commit;
    district_added := public._op_pass5_district(p_state);  commit;
    ngo_added      := public._op_pass6a_ngo(p_state);      commit;
    pvt_added      := public._op_pass6b_pvt(p_state);      commit;
    tribal_added   := public._op_pass7_tribal(p_state);    commit;
    joint_added    := public._op_pass8_joint(p_state);     commit;
    bia_added      := public._op_pass9_bia(p_state);       commit;
    osm_added      := public._op_pass10_osm(p_state);      commit;
    bep_added      := public._op_pass11_bep(p_state);      commit;
  end if;

  linkage_updates    := public._op_linkage(p_state);          commit;
  consolidated_count := public._op_pass12_consolidation(p_state); commit;
end $procedure$;

GRANT EXECUTE ON PROCEDURE public.populate_operators_for_state(text) TO service_role;

COMMIT;
