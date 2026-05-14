-- 20260514_op_pass2_counties_use_name_full.sql
--
-- Fix `_op_pass2_counties` slug collision for Maryland.
--
-- Problem: MD TIGER counties has two rows with name='Baltimore' —
--   24005 = "Baltimore County"  (the county)
--   24510 = "Baltimore city"    (independent city, treated as county-equivalent)
--
-- The function used `c.name || ' County'` to build the canonical name and
-- slug, so both rows produced slug='baltimore-county' → ON CONFLICT
-- DO UPDATE cardinality violation.
--
-- Fix: use `c.name_full` (which TIGER populates with the correct suffix:
-- "Baltimore County" vs "Baltimore city") for canonical_name AND slug
-- derivation. Falls back to `name || ' County'` when name_full is null.
--
-- Same pattern applies to VA, MO, NV (independent cities) — future-proofs
-- those state launches too.

begin;

create or replace function public._op_pass2_counties(p_state text)
returns integer
language plpgsql
as $function$
declare v_n int := 0; v_state_fp text := public._op_state_fp(p_state);
begin
  with ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      county_geoid, fips_state, fips_county, state_code, origin_source
    )
    select
      public.slugify_agency(coalesce(c.name_full, c.name || ' County')),
      coalesce(c.name_full, c.name || ' County'),
      c.name,
      array[
        coalesce(c.name_full, c.name || ' County'),
        c.name,
        c.name || ' County',
        c.name || ' County, County of',
        'County of ' || c.name
      ],
      'county', 'county',
      c.geoid, v_state_fp, substring(c.geoid from 3 for 3),
      p_state, 'tiger_counties'
    from public.counties c
    where c.state_fp = v_state_fp
    on conflict (slug) do update set
      county_geoid = excluded.county_geoid,
      fips_state   = excluded.fips_state,
      fips_county  = excluded.fips_county,
      aliases      = (select array_agg(distinct a)
                      from unnest(public.operators.aliases || excluded.aliases) a),
      updated_at   = now()
    returning 1
  )
  select count(*) into v_n from ins;
  return v_n;
end $function$;

commit;
