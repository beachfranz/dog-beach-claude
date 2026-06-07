-- 20260607_op_pass1b_towns_unique_slug.sql
--
-- Fix _op_pass1b_towns slug collision on ME launch.
--
-- Symptom: phase `operators` failed with "ON CONFLICT DO UPDATE command
-- cannot affect row a second time" — INSERT had two proposed rows with
-- the same slug.
--
-- Root cause: dedup suffix was `-fc<fips_county>`, but ME has TWO TIGER
-- COUSUB rows for "Rangeley" both in fips_county='007' (Franklin) with
-- different fips_cousub values (61840 vs 61875). The county-level suffix
-- isn't unique when a town name appears twice within the same county.
--
-- Fix: use fips_cousub for the suffix. (fips_state, fips_cousub) is the
-- TIGER COUSUB primary identity, guaranteed unique per state. The slug
-- becomes town-of-rangeley-cs61840 / town-of-rangeley-cs61875 — clean.
--
-- The rest of the function is unchanged; only the suffix expression in
-- the slug CASE differs.

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
       and tcs.classfp = 'T1'
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      fips_state, fips_place, state_code, origin_source
    )
    select
      case
        when t.dupe_count > 1
          then public.slugify_agency('Town of ' || t.name) || '-cs' || t.fips_cousub
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
