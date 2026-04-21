// set-governing-body/index.ts
// Enrichment pass that sets governing_body, governing_body_source, and governing_body_notes.
//
// Two steps per row:
//   1. Name-keyword override — if the beach name implies a jurisdiction type
//      (e.g. "State Beach", "National Park", "County Park"), override the
//      governing_jurisdiction set by geocoding.
//   2. Derive governing_body from the (possibly overridden) governing_jurisdiction.
//
// Only processes rows where governing_body IS NULL — safe to re-run.
//
// POST { state?: string, county?: string, limit?: number }
// Returns { processed, updated, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const DEFAULT_LIMIT        = 2000;
const WRITE_BATCH          = 50;
const CONCURRENCY          = 10;

// ── Keyword tables ────────────────────────────────────────────────────────────

const FEDERAL_KEYWORDS = [
  "NATIONAL SEASHORE", "NATIONAL PARK", "NATIONAL RECREATION AREA",
  "NATIONAL MONUMENT", "NATIONAL WILDLIFE", "ARMY CORPS",
  "CAMP PENDLETON", "NAVAL", "AIR FORCE", "MARINE CORPS BASE",
];

const STATE_KEYWORDS = [
  "STATE BEACH", "STATE PARK", "STATE RECREATION", "STATE RESERVE",
  "STATE MARINE", "STATE HISTORIC",
];

const COUNTY_KEYWORDS = [
  "COUNTY PARK", "COUNTY BEACH", "REGIONAL PARK", "REGIONAL BEACH",
];

function detectKeyword(
  name: string,
): { jurisdiction: string; keyword: string } | null {
  const upper = name.toUpperCase();
  for (const kw of FEDERAL_KEYWORDS) {
    if (upper.includes(kw)) return { jurisdiction: "governing federal", keyword: kw };
  }
  for (const kw of STATE_KEYWORDS) {
    if (upper.includes(kw)) return { jurisdiction: "governing state", keyword: kw };
  }
  for (const kw of COUNTY_KEYWORDS) {
    if (upper.includes(kw)) return { jurisdiction: "governing county", keyword: kw };
  }
  return null;
}

// ── Per-row enrichment ────────────────────────────────────────────────────────

interface Row {
  id:                     number;
  display_name:           string;
  governing_jurisdiction: string | null;
  governing_city:         string | null;
  governing_county:       string | null;
  governing_state:        string | null;
}

interface Enriched {
  id:                    number;
  governing_jurisdiction: string;
  governing_body:        string;
  governing_body_source: string;
  governing_body_notes:  string;
}

function enrich(row: Row): Enriched {
  const keywordMatch = detectKeyword(row.display_name);
  const geocodeJurisdiction = row.governing_jurisdiction ?? "governing city";

  let jurisdiction:  string;
  let source:        string;
  let notes:         string;

  if (keywordMatch && keywordMatch.jurisdiction !== geocodeJurisdiction) {
    // Name keyword overrides geocode result
    jurisdiction = keywordMatch.jurisdiction;
    source       = "name_keyword";

    const previous = geocodeJurisdiction === "governing city"
      ? `City of ${row.governing_city ?? "unknown"}`
      : geocodeJurisdiction === "governing county"
        ? (row.governing_county ?? "unknown county")
        : (row.governing_state ?? "unknown state");

    notes = `Name contains "${keywordMatch.keyword}", jurisdiction set to ${jurisdiction}; geocode had resolved to ${previous}.`;

  } else if (keywordMatch) {
    // Keyword matches what geocoding already found — confirm with source
    jurisdiction = keywordMatch.jurisdiction;
    source       = "name_keyword+geocode";
    notes        = `Name contains "${keywordMatch.keyword}" and geocode agrees: ${jurisdiction}.`;

  } else {
    // No keyword — use geocode result as-is
    jurisdiction = geocodeJurisdiction;
    source       = "geocode";

    if (jurisdiction === "governing city") {
      notes = `Coordinates resolve to incorporated city ${row.governing_city ?? "unknown"} (geocode).`;
    } else if (jurisdiction === "governing county") {
      notes = `No incorporated city found at coordinates, assigned to ${row.governing_county ?? "unknown"} (geocode).`;
    } else {
      notes = `Could not determine specific jurisdiction from coordinates or name, defaulted to state.`;
    }
  }

  // Derive governing_body from jurisdiction
  let body: string;
  if (jurisdiction === "governing city") {
    body = `City of ${row.governing_city ?? "Unknown"}`;
  } else if (jurisdiction === "governing county") {
    body = row.governing_county ?? "Unknown County";
  } else if (jurisdiction === "governing state") {
    body = row.governing_state ?? "Unknown State";
  } else {
    // governing federal
    body = "Federal";
  }

  return {
    id:                     row.id,
    governing_jurisdiction: jurisdiction,
    governing_body:         body,
    governing_body_source:  source,
    governing_body_notes:   notes,
  };
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

  // Fetch rows without governing_body yet
  let query = supabase
    .from("beaches_staging_new")
    .select("id, display_name, governing_jurisdiction, governing_city, governing_county, governing_state")
    .is("governing_body", null)
    .is("review_status", null)
    .limit(body.limit ?? DEFAULT_LIMIT);

  if (body.state)  query = query.eq("state", body.state);
  if (body.county) query = query.eq("county", body.county);

  const { data: rows, error: fetchError } = await query;
  if (fetchError) return json({ error: fetchError.message }, 500);
  if (!rows?.length) return json({ processed: 0, updated: 0, errors: [] });

  // Enrich each row (pure CPU — no async needed, but keep pLimit for write phase)
  const enriched = (rows as Row[]).map(enrich);

  // Write in batches
  const writeErrors: string[] = [];
  const writeTasks = enriched.map(({ id, ...fields }) => async () => {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update(fields)
      .eq("id", id);
    if (error) writeErrors.push(`id ${id}: ${error.message}`);
  });

  await pLimit(writeTasks, CONCURRENCY);

  return json({
    processed: rows.length,
    updated:   enriched.length - writeErrors.length,
    errors:    writeErrors,
  });
});
