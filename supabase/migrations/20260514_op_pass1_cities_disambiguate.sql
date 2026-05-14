-- 20260514_op_pass1_cities_disambiguate.sql
--
-- Fix `_op_pass1_cities` slug collision for multi-municipality names.
--
-- Problem (OH preflight 2026-05-14): Ohio has three Oakwoods (Cuyahoga,
-- Montgomery, Paulding counties) — all produce slug='city-of-oakwood'.
-- The ON CONFLICT DO UPDATE clause then fires twice for the same row
-- inside a single INSERT → CardinalityViolation.
--
-- Same risk exists for any state with municipal-name duplicates (NY has
-- multiple Wellingtons, OH has Oakwood/Newton/Bethel/etc., PA has
-- duplicates too).
--
-- Fix: when a name collides within a state, append the fips_place code
-- to the slug to disambiguate. Stable, deterministic, and only affects
-- the duplicates — unique-name cities keep their plain slugs.
--
-- canonical_name keeps the human-friendly form; aliases array includes
-- both the plain and disambiguated forms so fuzzy matching downstream
-- still resolves correctly.

begin;

create or replace function public._op_pass1_cities(p_state text)
returns integer
language plpgsql
as $function$
declare v_n int := 0;
begin
  with cities_with_dupe as (
    select j.id, j.name, j.fips_state, j.fips_place,
           count(*) over (partition by j.state, j.name) as dupe_count
      from public.jurisdictions j
     where j.state = p_state and j.place_type like 'C%'
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      jurisdiction_id, fips_state, fips_place, state_code, origin_source
    )
    select
      case
        when c.dupe_count > 1
          then public.slugify_agency('City of ' || c.name) || '-fp' || c.fips_place
        else public.slugify_agency('City of ' || c.name)
      end as slug,
      'City of ' || c.name, c.name,
      array['City of ' || c.name, c.name, c.name || ', City of'],
      'city', 'city',
      c.id, c.fips_state, c.fips_place, p_state, 'tiger_places'
    from cities_with_dupe c
    on conflict (slug) do update set
      jurisdiction_id = excluded.jurisdiction_id,
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

commit;
