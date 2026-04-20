// clean-beaches-staging/index.ts
// Pipeline stage 4: cleans bronze-tier records and promotes them to silver.
//
// Cleaning operations:
//   - Mojibake fix (UTF-8 bytes stored/read as Latin-1, e.g. â€™ → ')
//   - Whitespace normalization (collapse runs, trim)
//   - governing_jurisdiction resolved to canonical entity name
//   - governing_body synced to governing_jurisdiction when confidence is high
//
// POST { state?: string, dry_run?: boolean, quality_tier?: string }
// Returns { processed, promoted, unchanged, changes, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// ── Fields to apply text cleaning to ─────────────────────────────────────────

const TEXT_FIELDS = [
  "display_name", "formatted_address", "route",
  "city", "county", "state", "governing_body",
  "zone_description", "policy_notes", "review_notes",
] as const;

// ── Canonical state parks agencies ────────────────────────────────────────────

const STATE_PARKS_AGENCY: Record<string, string> = {
  AL: "Alabama State Parks",
  AK: "Alaska State Parks",
  AZ: "Arizona State Parks and Trails",
  AR: "Arkansas State Parks",
  CA: "California Department of Parks and Recreation",
  CO: "Colorado Parks and Wildlife",
  CT: "Connecticut DEEP State Parks",
  DE: "Delaware State Parks",
  FL: "Florida Department of Environmental Protection",
  GA: "Georgia Department of Natural Resources",
  HI: "Hawaii Division of State Parks",
  ID: "Idaho Parks and Recreation",
  IL: "Illinois Department of Natural Resources",
  IN: "Indiana Department of Natural Resources",
  LA: "Louisiana State Parks",
  MA: "Massachusetts Department of Conservation and Recreation",
  MD: "Maryland Department of Natural Resources",
  ME: "Maine Bureau of Parks and Lands",
  MI: "Michigan Department of Natural Resources",
  MN: "Minnesota Department of Natural Resources",
  MS: "Mississippi Department of Wildlife, Fisheries and Parks",
  NC: "North Carolina State Parks",
  NJ: "New Jersey Division of Parks and Forestry",
  NY: "New York State Parks",
  OR: "Oregon Parks and Recreation Department",
  RI: "Rhode Island Division of Parks and Recreation",
  SC: "South Carolina State Parks",
  TX: "Texas Parks and Wildlife Department",
  VA: "Virginia Department of Conservation and Recreation",
  WA: "Washington State Parks and Recreation Commission",
  WI: "Wisconsin Department of Natural Resources",
};

// ── Name keywords for jurisdiction inference ──────────────────────────────────

const FEDERAL_NPS = [
  "NATIONAL SEASHORE", "NATIONAL PARK", "NATIONAL RECREATION AREA",
  "NATIONAL MONUMENT", "NATIONAL LAKESHORE", "NATIONAL RIVER",
];
const FEDERAL_USFS  = ["NATIONAL FOREST", "NATIONAL GRASSLAND"];
const FEDERAL_USFWS = ["NATIONAL WILDLIFE REFUGE", " NWR "];
const FEDERAL_ACOE  = ["ARMY CORPS", "CORPS OF ENGINEERS"];
const FEDERAL_BLM   = ["BUREAU OF LAND MANAGEMENT"];

const STATE_PARK_KEYWORDS = [
  "STATE BEACH", "STATE PARK", "STATE RECREATION AREA", "STATE RESERVE",
  "STATE MARINE", "STATE HISTORIC", "STATE NATURAL AREA",
];
const COUNTY_KEYWORDS = [
  "COUNTY PARK", "COUNTY BEACH", "REGIONAL PARK", "REGIONAL BEACH",
  "COUNTY RECREATION",
];

// ── Canonical jurisdiction resolver ──────────────────────────────────────────

function canonicalJurisdiction(
  name: string,
  city: string | null,
  county: string | null,
  state: string | null,
): { jurisdiction: string; confidence: "high" | "low" } {
  const u = name.toUpperCase();

  // Federal — NPS
  if (FEDERAL_NPS.some(kw => u.includes(kw)))
    return { jurisdiction: "National Park Service", confidence: "high" };

  // Federal — USFS
  if (FEDERAL_USFS.some(kw => u.includes(kw)))
    return { jurisdiction: "US Forest Service", confidence: "high" };

  // Federal — USFWS
  if (FEDERAL_USFWS.some(kw => u.includes(kw)))
    return { jurisdiction: "US Fish and Wildlife Service", confidence: "high" };

  // Federal — Army Corps
  if (FEDERAL_ACOE.some(kw => u.includes(kw)))
    return { jurisdiction: "US Army Corps of Engineers", confidence: "high" };

  // Federal — BLM
  if (FEDERAL_BLM.some(kw => u.includes(kw)))
    return { jurisdiction: "Bureau of Land Management", confidence: "high" };

  // State parks agency
  if (STATE_PARK_KEYWORDS.some(kw => u.includes(kw))) {
    const agency = state ? STATE_PARKS_AGENCY[state] : null;
    if (agency) return { jurisdiction: agency, confidence: "high" };
    return { jurisdiction: `${state ?? "State"} State Parks`, confidence: "high" };
  }

  // County parks
  if (COUNTY_KEYWORDS.some(kw => u.includes(kw))) {
    if (county) return { jurisdiction: `${county} County`, confidence: "high" };
  }

  // City / municipal (default for most beaches)
  if (city) return { jurisdiction: `City of ${city}`, confidence: "high" };

  // County fallback
  if (county) return { jurisdiction: `${county} County`, confidence: "low" };

  return { jurisdiction: "Unknown", confidence: "low" };
}

// ── Text cleaners ─────────────────────────────────────────────────────────────

function fixMojibake(s: string): string {
  try {
    const bytes = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) {
      const code = s.charCodeAt(i);
      if (code > 255) return s;
      bytes[i] = code;
    }
    return new TextDecoder("utf-8").decode(bytes);
  } catch {
    return s;
  }
}

function cleanText(s: string | null): string | null {
  if (s == null) return null;
  return s.replace(/\s+/g, " ").trim().replace(/\s+/g, " ") === s
    ? fixMojibake(s).replace(/\s+/g, " ").trim()
    : fixMojibake(s.replace(/\s+/g, " ").trim());
}

// ── Per-record cleaning ───────────────────────────────────────────────────────

type Change = { field: string; before: string | null; after: string | null };

function cleanRecord(row: Record<string, unknown>): {
  updates: Record<string, unknown>;
  changes: Change[];
} {
  const updates: Record<string, unknown> = {};
  const changes: Change[] = [];

  // Text field cleaning
  for (const field of TEXT_FIELDS) {
    const before = row[field] as string | null;
    const after  = cleanText(before);
    if (after !== before) {
      updates[field] = after;
      changes.push({ field, before, after });
    }
  }

  // Canonical jurisdiction
  const name   = ((updates["display_name"]  ?? row["display_name"])  ?? "") as string;
  const city   = ((updates["city"]           ?? row["city"])           ?? null) as string | null;
  const county = ((updates["county"]         ?? row["county"])         ?? null) as string | null;
  const state  = ((updates["state"]          ?? row["state"])          ?? null) as string | null;

  const { jurisdiction, confidence } = canonicalJurisdiction(name, city, county, state);
  const currentJurisdiction = row["governing_jurisdiction"] as string | null;

  if (jurisdiction !== currentJurisdiction && jurisdiction !== "Unknown") {
    updates["governing_jurisdiction"] = jurisdiction;
    changes.push({ field: "governing_jurisdiction", before: currentJurisdiction, after: jurisdiction });

    // Sync governing_body to canonical jurisdiction (high confidence only)
    if (confidence === "high") {
      const currentBody = row["governing_body"] as string | null;
      if (jurisdiction !== currentBody) {
        updates["governing_body"] = jurisdiction;
        changes.push({ field: "governing_body", before: currentBody, after: jurisdiction });
      }
    }
  }

  // Flag low-confidence jurisdiction for manual review
  if (confidence === "low") {
    const existingNotes = (row["review_notes"] as string | null) ?? "";
    const note = "governing_jurisdiction could not be determined — manual review needed";
    if (!existingNotes.includes("governing_jurisdiction")) {
      updates["review_notes"]  = existingNotes ? `${existingNotes}; ${note}` : note;
      updates["review_status"] = "Needs Review";
      changes.push({ field: "review_notes", before: existingNotes || null, after: updates["review_notes"] as string });
    }
  }

  return { updates, changes };
}

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state?: string; dry_run?: boolean; quality_tier?: string } = {};
  try { body = await req.json(); } catch { /* empty body is fine */ }

  const { state, dry_run = false, quality_tier = "bronze" } = body;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  let query = supabase
    .from("beaches_staging")
    .select("*")
    .eq("quality_tier", quality_tier)
    .limit(10000);

  if (state) query = query.eq("state", state);

  const { data: records, error: fetchError } = await query;
  if (fetchError) return json({ error: fetchError.message }, 500);

  const results = {
    processed: records?.length ?? 0,
    promoted:  0,
    unchanged: 0,
    changes:   [] as { id: number; display_name: string; changes: Change[] }[],
    errors:    [] as string[],
  };

  const rowsToUpsert: Record<string, unknown>[] = [];

  for (const row of records ?? []) {
    const { updates, changes } = cleanRecord(row as Record<string, unknown>);

    if (changes.length > 0) {
      results.changes.push({ id: row.id, display_name: row.display_name, changes });
    } else {
      results.unchanged++;
    }

    // Promote bronze → silver; re-clean silver records stay silver
    const targetTier = quality_tier === "bronze" ? "silver" : quality_tier;
    rowsToUpsert.push({ ...row, ...updates, quality_tier: targetTier });
  }

  if (!dry_run && rowsToUpsert.length > 0) {
    const { error } = await supabase
      .from("beaches_staging")
      .upsert(rowsToUpsert, { onConflict: "id" });

    if (error) {
      results.errors.push(error.message);
    } else {
      results.promoted = rowsToUpsert.length;
    }
  } else {
    results.promoted = rowsToUpsert.length;
  }

  return json(results);
});
