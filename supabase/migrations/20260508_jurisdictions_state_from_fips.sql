-- 20260508_jurisdictions_state_from_fips.sql
--
-- Fix: public.load_jurisdictions_batch hardcoded state='CA' regardless
-- of input STATEFP. Loading OR (41) or WA (53) places stamped them as
-- 'CA'. Adds state_from_fips() helper, rewrites the loader to use it,
-- and backfills affected rows in public.jurisdictions.

begin;

create or replace function public.state_from_fips(p_fips text)
returns text
language sql
immutable
as $$
  select case lpad(p_fips, 2, '0')
    when '01' then 'AL' when '02' then 'AK' when '04' then 'AZ' when '05' then 'AR'
    when '06' then 'CA' when '08' then 'CO' when '09' then 'CT' when '10' then 'DE'
    when '11' then 'DC' when '12' then 'FL' when '13' then 'GA' when '15' then 'HI'
    when '16' then 'ID' when '17' then 'IL' when '18' then 'IN' when '19' then 'IA'
    when '20' then 'KS' when '21' then 'KY' when '22' then 'LA' when '23' then 'ME'
    when '24' then 'MD' when '25' then 'MA' when '26' then 'MI' when '27' then 'MN'
    when '28' then 'MS' when '29' then 'MO' when '30' then 'MT' when '31' then 'NE'
    when '32' then 'NV' when '33' then 'NH' when '34' then 'NJ' when '35' then 'NM'
    when '36' then 'NY' when '37' then 'NC' when '38' then 'ND' when '39' then 'OH'
    when '40' then 'OK' when '41' then 'OR' when '42' then 'PA' when '44' then 'RI'
    when '45' then 'SC' when '46' then 'SD' when '47' then 'TN' when '48' then 'TX'
    when '49' then 'UT' when '50' then 'VT' when '51' then 'VA' when '53' then 'WA'
    when '54' then 'WV' when '55' then 'WI' when '56' then 'WY'
    when '60' then 'AS' when '66' then 'GU' when '69' then 'MP' when '72' then 'PR'
    when '78' then 'VI'
    else null
  end
$$;

create or replace function public.load_jurisdictions_batch(p_batch jsonb)
returns int
language plpgsql
security definer
set search_path = public, pg_catalog
as $$
declare
  f jsonb;
  n int := 0;
begin
  for f in select * from jsonb_array_elements(p_batch)
  loop
    insert into public.jurisdictions (
      name, namelsad, place_type, funcstat,
      fips_state, fips_place, fips_county,
      state, county, geom, loaded_at
    ) values (
      f->'props'->>'NAME',
      f->'props'->>'NAMELSAD',
      f->'props'->>'CLASSFP',
      f->'props'->>'FUNCSTAT',
      f->'props'->>'STATEFP',
      f->'props'->>'PLACEFP',
      null,
      public.state_from_fips(f->'props'->>'STATEFP'),
      null,
      ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(f->>'geom'), 4326)),
      now()
    )
    on conflict (fips_state, fips_place) do update set
      name       = excluded.name,
      namelsad   = excluded.namelsad,
      place_type = excluded.place_type,
      funcstat   = excluded.funcstat,
      state      = excluded.state,
      geom       = excluded.geom,
      loaded_at  = now();
    n := n + 1;
  end loop;
  return n;
end;
$$;

-- Backfill: any rows whose stored state disagrees with their fips_state
update public.jurisdictions
   set state = public.state_from_fips(fips_state)
 where state is distinct from public.state_from_fips(fips_state)
   and public.state_from_fips(fips_state) is not null;

commit;
