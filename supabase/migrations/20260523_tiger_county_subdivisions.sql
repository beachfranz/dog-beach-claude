-- 20260523_tiger_county_subdivisions.sql
--
-- TIGER County Subdivisions (MCDs) layer. Mirrors public.jurisdictions
-- (TIGER Places, the C1 cities layer) per Franz directive 2026-05-23
-- LATE: "be sure to model mcds using same process as we use for C1s".
--
-- Why this exists: NH virgin-state test 2026-05-23 LATE surfaced that
-- NH municipal authority (where 234 towns hold most dog-policy power)
-- lives in TIGER's COUSUB layer, NOT in TIGER PLACE. The cities
-- promoter (_op_pass1_cities) only sees PLACE rows, so all 234 NH
-- towns were invisible to operator extraction. Same applies to the
-- other town-strong states (CT/MA/ME/RI/VT/NJ/PA per
-- state_government_strength).
--
-- Data source: TIGER/Line 2024 COUSUB shapefiles, one per state, from
-- https://www2.census.gov/geo/tiger/TIGER2024/COUSUB/tl_2024_{fp}_cousub.zip
--
-- CLASSFP values seen in COUSUB (CT/MA/NH typical):
--   T1 = Active government providing primary general-purpose functions  ← the dominant town class
--   T5 = Active with area also part of an inactive entity
--   T9 = Active with area only nominally a separate gov
--   C5 = Active incorporated city/town (rare in COUSUB; mostly in PLACE)
--   Z3 = Unorganized territory
--   Z5 = Inactive
--
-- For NH the load is expected to be ~250 rows (234 towns + 13 cities +
-- a few unorganized territories).

BEGIN;

CREATE TABLE IF NOT EXISTS public.tiger_county_subdivisions (
  id          bigserial PRIMARY KEY,
  name        text NOT NULL,
  namelsad    text,
  classfp     text,
  mtfcc       text,
  funcstat    text,
  fips_state  text NOT NULL,
  fips_county text,
  fips_cousub text NOT NULL,
  geoid       text,    -- 10-digit composite (state+county+cousub)
  state       text NOT NULL,
  county      text,
  geom        geometry(MultiPolygon, 4326),
  geom_geog   geography(MultiPolygon, 4326),
  loaded_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (fips_state, fips_county, fips_cousub)
);

CREATE INDEX IF NOT EXISTS tiger_county_subdivisions_state_idx
  ON public.tiger_county_subdivisions (state);
CREATE INDEX IF NOT EXISTS tiger_county_subdivisions_classfp_idx
  ON public.tiger_county_subdivisions (classfp);
CREATE INDEX IF NOT EXISTS tiger_county_subdivisions_geom_gist
  ON public.tiger_county_subdivisions USING gist (geom);
CREATE INDEX IF NOT EXISTS tiger_county_subdivisions_geog_gist
  ON public.tiger_county_subdivisions USING gist (geom_geog);

COMMENT ON TABLE public.tiger_county_subdivisions IS
  'TIGER 2024 County Subdivisions (MCDs). Carries towns/townships in '
  'town-strong states (NH/VT/ME/MA/CT/RI/NJ/PA). Loaded per-state by '
  'scripts/load_county_subdivisions_shapefile.py. Mirrors '
  'public.jurisdictions (TIGER Places) per same-process directive.';

-- Batch loader function: mirror of load_jurisdictions_batch. Writes
-- BOTH geom and geom_geog (gap #43 lesson — never let geom_geog go
-- silently NULL).
CREATE OR REPLACE FUNCTION public.load_county_subdivisions_batch(p_batch jsonb)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
declare
  f jsonb;
  n int := 0;
  v_geom geometry;
begin
  for f in select * from jsonb_array_elements(p_batch)
  loop
    v_geom := ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(f->>'geom'), 4326));
    insert into public.tiger_county_subdivisions (
      name, namelsad, classfp, mtfcc, funcstat,
      fips_state, fips_county, fips_cousub, geoid,
      state, geom, geom_geog, loaded_at
    ) values (
      f->'props'->>'NAME',
      f->'props'->>'NAMELSAD',
      f->'props'->>'CLASSFP',
      f->'props'->>'MTFCC',
      f->'props'->>'FUNCSTAT',
      f->'props'->>'STATEFP',
      f->'props'->>'COUNTYFP',
      f->'props'->>'COUSUBFP',
      f->'props'->>'GEOID',
      public.state_from_fips(f->'props'->>'STATEFP'),
      v_geom,
      v_geom::geography,
      now()
    )
    on conflict (fips_state, fips_county, fips_cousub) do update set
      name       = excluded.name,
      namelsad   = excluded.namelsad,
      classfp    = excluded.classfp,
      mtfcc      = excluded.mtfcc,
      funcstat   = excluded.funcstat,
      geoid      = excluded.geoid,
      state      = excluded.state,
      geom       = excluded.geom,
      geom_geog  = excluded.geom_geog,
      loaded_at  = now();
    n := n + 1;
  end loop;
  return n;
end;
$function$;

GRANT SELECT ON public.tiger_county_subdivisions
  TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.load_county_subdivisions_batch(jsonb)
  TO service_role;

COMMIT;

-- Sanity (post-load):
--   SELECT state, classfp, COUNT(*) AS n
--     FROM public.tiger_county_subdivisions
--    GROUP BY state, classfp ORDER BY state, classfp;
--   Expected for NH: ~234 T1 (active towns) + 13 C5 (cities) + a few Z3.
