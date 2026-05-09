-- 20260509_coastal_pad_us_filter_tighten.sql
--
-- Phase 4 (federal_policy_seed) advisory was reporting 576 unseeded
-- units for MA — too noisy. The previous filter caught any
-- mng_type='FED' polygon, including military bases, USACE flood
-- control, conservation easements, and inland forests. Of MA's 576,
-- only ~10-15 are coastal-recreational units worth curating dog policy.
--
-- New filter: canonical coastal-recreational unit names only, AND
-- managed by FED. Returns one row per polygon (Cape Cod NS has many);
-- the new helper coastal_pad_us_units_distinct_for_state(state) returns
-- one row per unit_name for cleaner reporting/curation tracking.
--
-- PAD-US doesn't tag des_tp consistently (e.g. Cape Cod NS appears
-- with des_tp ∈ {WPA, PCON, CONE, MPA} — never 'NS'). So we filter
-- by unit_name patterns instead. Six canonical name patterns cover
-- the relevant federal coastal types nationwide:
--   - National Seashore   (e.g. Cape Cod, Cape Hatteras, Padre Island)
--   - National Lakeshore  (Sleeping Bear, Pictured Rocks, Apostle Is.)
--   - National Recreation Area (Boston Harbor Is., Golden Gate, Gateway)
--   - National Wildlife Refuge (Monomoy, Parker River, Chincoteague…)
--   - National Park       (Acadia, Olympic, Channel Islands, Biscayne)
--   - National Monument   (when coastal — Cabrillo, Cesar Chavez, etc.)

begin;

create or replace function public.coastal_pad_us_units_for_state(p_state text)
returns table(
  unit_id    bigint,
  mng_name   text,
  unit_name  text,
  loc_mang   text,
  loc_own    text,
  geom_centroid_lat double precision,
  geom_centroid_lon double precision
)
language sql
stable
as $function$
  with state_bbox as (
    select st_collect(geom) g from public.counties
     where state_fp = (select case p_state
       when 'AL' then '01' when 'AK' then '02' when 'AZ' then '04' when 'AR' then '05'
       when 'CA' then '06' when 'CO' then '08' when 'CT' then '09' when 'DE' then '10'
       when 'FL' then '12' when 'GA' then '13' when 'HI' then '15' when 'ID' then '16'
       when 'IL' then '17' when 'IN' then '18' when 'IA' then '19' when 'KS' then '20'
       when 'KY' then '21' when 'LA' then '22' when 'ME' then '23' when 'MD' then '24'
       when 'MA' then '25' when 'MI' then '26' when 'MN' then '27' when 'MS' then '28'
       when 'MO' then '29' when 'MT' then '30' when 'NE' then '31' when 'NV' then '32'
       when 'NH' then '33' when 'NJ' then '34' when 'NM' then '35' when 'NY' then '36'
       when 'NC' then '37' when 'ND' then '38' when 'OH' then '39' when 'OK' then '40'
       when 'OR' then '41' when 'PA' then '42' when 'RI' then '44' when 'SC' then '45'
       when 'SD' then '46' when 'TN' then '47' when 'TX' then '48' when 'UT' then '49'
       when 'VT' then '50' when 'VA' then '51' when 'WA' then '53' when 'WV' then '54'
       when 'WI' then '55' when 'WY' then '56'
     end)
  )
  select pu.unit_id::bigint,
         pu.mng_name::text,
         pu.unit_name::text,
         pu.loc_mang::text,
         pu.loc_own::text,
         st_y(st_centroid(pu.geom))::double precision,
         st_x(st_centroid(pu.geom))::double precision
    from public.pad_us_units pu, state_bbox sb
   where st_intersects(pu.geom, sb.g)
     and pu.mng_type = 'FED'
     and (
       pu.unit_name ilike '%National Seashore%'
       or pu.unit_name ilike '%National Lakeshore%'
       or pu.unit_name ilike '%National Recreation Area%'
       or pu.unit_name ilike '%National Wildlife Refuge%'
       or pu.unit_name ilike '%National Park%'
       or pu.unit_name ilike '%National Monument%'
     );
$function$;


-- Distinct-by-unit_name helper. Useful when one curation effort covers
-- a multi-polygon unit (Cape Cod NS has 30+ polygons; we only need to
-- write the dog policy once).

create or replace function public.coastal_pad_us_units_distinct_for_state(p_state text)
returns table(
  unit_name  text,
  mng_name   text,
  polygon_count bigint
)
language sql
stable
as $function$
  select unit_name, mng_name, count(*)::bigint as polygon_count
    from public.coastal_pad_us_units_for_state(p_state)
   where unit_name is not null
   group by 1, 2
   order by 1;
$function$;


-- Update the unseeded helper to use the same tighter filter
create or replace function public.unseeded_pad_us_units_for_state(p_state text)
returns bigint
language sql
stable
as $function$
  -- Count unique unit_names, not polygons. One curation effort per unit.
  select count(distinct c.unit_name)::bigint
    from public.coastal_pad_us_units_for_state(p_state) c
   where c.unit_name is not null
     and not exists (
       select 1 from public.pad_us_unit_dogs_policy pdp
        where pdp.unit_id = c.unit_id
     );
$function$;


grant execute on function public.coastal_pad_us_units_for_state(text)           to anon, authenticated, service_role;
grant execute on function public.coastal_pad_us_units_distinct_for_state(text)  to anon, authenticated, service_role;
grant execute on function public.unseeded_pad_us_units_for_state(text)          to anon, authenticated, service_role;

commit;
