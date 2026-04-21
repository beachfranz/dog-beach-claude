// census-governing-city/index.ts
// Calls the US Census Bureau Geocoder (Incorporated Places layer) for each
// beach and writes the result to census_incorporated_place:
//   - Name of the incorporated city if the lat/lon falls inside one
//   - null if the point is in an unincorporated area
//
// Only processes rows where census_incorporated_place IS NULL (re-runnable).
//
// POST { state?: string, county?: string, limit?: number }
// Returns { processed, incorporated, unincorporated, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const CENSUS_URL   = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates";
const CONCURRENCY  = 5;   // Census API is free/public — be polite
const DEFAULT_LIMIT = 500;

// ── Census call ───────────────────────────────────────────────────────────────

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
    const places: { NAME: string }[] =
      data?.result?.geographies?.["Incorporated Places"] ?? [];
    return places.length > 0 ? places[0].NAME : null;
  } catch {
    return null;
  }
}

// ── Concurrency limiter ───────────────────────────────────────────────────────

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

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state?: string; county?: string; limit?: number } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  let query = supabase
    .from("beaches_staging_new")
    .select("id, latitude, longitude, governing_city, governing_jurisdiction")
    .is("census_incorporated_place", null)
    .not("latitude", "is", null)
    .limit(body.limit ?? DEFAULT_LIMIT);

  if (body.state)  query = query.eq("state", body.state);
  if (body.county) query = query.eq("county", body.county);

  const { data: rows, error: fetchError } = await query;
  if (fetchError) return json({ error: fetchError.message }, 500);
  if (!rows?.length) return json({ processed: 0, incorporated: 0, unincorporated: 0, errors: [] });

  // ── Look up each point ──────────────────────────────────────────────────────

  const tasks = rows.map(row => async () => {
    const place = await fetchIncorporatedPlace(row.latitude, row.longitude);
    return {
      id:                        row.id,
      governing_city:            row.governing_city,
      governing_jurisdiction:    row.governing_jurisdiction,
      census_incorporated_place: place ?? "UNINCORPORATED",
    };
  });

  const results = await pLimit(tasks, CONCURRENCY);

  // ── Write results ───────────────────────────────────────────────────────────

  const writeErrors: string[] = [];
  const writeTasks = results.map(({ id, census_incorporated_place }) => async () => {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({ census_incorporated_place })
      .eq("id", id);
    if (error) writeErrors.push(`id ${id}: ${error.message}`);
  });

  await pLimit(writeTasks, 10);

  const incorporated   = results.filter(r => r.census_incorporated_place !== "UNINCORPORATED").length;
  const unincorporated = results.filter(r => r.census_incorporated_place === "UNINCORPORATED").length;

  return json({
    processed:     rows.length,
    incorporated,
    unincorporated,
    errors:        writeErrors,
  });
});
