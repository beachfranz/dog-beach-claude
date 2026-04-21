// v2-geocode-context/index.ts
// Pipeline stage 4 — populate address context fields only.
//
// Not responsible for jurisdiction classification. Just runs Google reverse
// geocode + Census Incorporated Places and populates the context fields
// (city, county, state, census_incorporated_place) that later stages may
// use as tie-breakers or display.
//
// Only processes records with review_status IS NULL.
//
// POST { limit?: number }
// Returns { processed, succeeded, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GOOGLE_KEY           = Deno.env.get("GOOGLE_MAPS_API_KEY")!;

const GEOCODE_URL   = "https://maps.googleapis.com/maps/api/geocode/json";
const CENSUS_URL    = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates";
const CONCURRENCY   = 5;
const DEFAULT_LIMIT = 2000;

interface GeoComponent { long_name: string; short_name: string; types: string[]; }

function component(components: GeoComponent[], type: string): string | null {
  return components.find(c => c.types.includes(type))?.long_name ?? null;
}

function stripPlaceSuffix(name: string): string {
  return name.replace(/\s+(city|town|village|borough|municipality|township)$/i, "").trim();
}

async function fetchIncorporatedPlace(lat: number, lon: number): Promise<string | null> {
  const url = new URL(CENSUS_URL);
  url.searchParams.set("x", String(lon));
  url.searchParams.set("y", String(lat));
  url.searchParams.set("benchmark", "Public_AR_Current");
  url.searchParams.set("vintage", "Current_Current");
  url.searchParams.set("layers", "Incorporated Places");
  url.searchParams.set("format", "json");
  try {
    const resp = await fetch(url.toString());
    if (!resp.ok) return null;
    const data = await resp.json();
    const places: { NAME: string }[] = data?.result?.geographies?.["Incorporated Places"] ?? [];
    return places.length > 0 ? places[0].NAME : null;
  } catch { return null; }
}

async function reverseGeocode(lat: number, lon: number) {
  const url = new URL(GEOCODE_URL);
  url.searchParams.set("latlng", `${lat},${lon}`);
  url.searchParams.set("key", GOOGLE_KEY);

  let data: { status: string; results: { address_components: GeoComponent[] }[] };
  try {
    const resp = await fetch(url.toString());
    data = await resp.json();
  } catch { return { status: "ERROR", fields: {} }; }

  if (data.status !== "OK")      return { status: data.status ?? "ERROR", fields: {} };
  if (!data.results?.length)     return { status: "ZERO_RESULTS", fields: {} };

  const components = data.results[0].address_components;
  const census     = await fetchIncorporatedPlace(lat, lon);

  return {
    status: "OK",
    fields: {
      street_number:             component(components, "street_number"),
      route:                     component(components, "route"),
      city:                      component(components, "locality"),
      county:                    component(components, "administrative_area_level_2"),
      state:                     component(components, "administrative_area_level_1"),
      zip:                       component(components, "postal_code"),
      census_incorporated_place: census ?? "UNINCORPORATED",
      governing_city:            census ? stripPlaceSuffix(census) : null,
      geocode_status:            "OK",
    },
  };
}

async function pLimit<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let index = 0;
  async function worker() {
    while (index < tasks.length) {
      const i = index++;
      results[i] = await tasks[i]();
    }
  }
  await Promise.all(Array.from({ length: limit }, worker));
  return results;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { limit?: number } = {};
  try { body = await req.json(); } catch { /* empty */ }

  if (!GOOGLE_KEY) return json({ error: "GOOGLE_MAPS_API_KEY not set" }, 500);
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error: fetchErr } = await supabase
    .from("beaches_staging_new")
    .select("id, latitude, longitude")
    .is("review_status", null)
    .is("geocode_status", null)
    .not("latitude",  "is", null)
    .not("longitude", "is", null)
    .limit(body.limit ?? DEFAULT_LIMIT);

  if (fetchErr) return json({ error: fetchErr.message }, 500);
  if (!rows?.length) return json({ processed: 0, succeeded: 0, errors: 0 });

  const tasks = rows.map(r => async () => ({
    id:     r.id,
    result: await reverseGeocode(r.latitude, r.longitude),
  }));
  const results = await pLimit(tasks, CONCURRENCY);

  let succeeded = 0;
  let errors    = 0;
  const writeErrs: string[] = [];

  const writeTasks = results.map(({ id, result }) => async () => {
    const fields: Record<string, unknown> = result.status === "OK"
      ? result.fields
      : { geocode_status: result.status };
    const { error } = await supabase
      .from("beaches_staging_new").update(fields).eq("id", id);
    if (error) { errors++; writeErrs.push(`id ${id}: ${error.message}`); }
    else if (result.status === "OK") succeeded++;
    else errors++;
  });
  await pLimit(writeTasks, 10);

  return json({ processed: rows.length, succeeded, errors, errors_sample: writeErrs.slice(0, 10) });
});
