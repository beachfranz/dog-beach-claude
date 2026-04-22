// load-beaches-staging/index.ts
// Loads beaches from US_beaches.csv in Supabase Storage into beaches_staging_new.
// Filters to a single state so the pipeline can be run state by state.
//
// POST { state: string }  — full name ("California") or abbreviation ("CA")
// Returns { state, matched, inserted, skipped, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { parse as parseCsv } from "https://deno.land/std@0.224.0/csv/mod.ts";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const STORAGE_BUCKET       = "pipeline";
const CSV_PATH             = "US_beaches.csv";
const INSERT_BATCH_SIZE    = 500;

// ── State name ↔ abbreviation ─────────────────────────────────────────────────

const STATE_ABBR: Record<string, string> = {
  alabama: "AL", alaska: "AK", arizona: "AZ", arkansas: "AR",
  california: "CA", colorado: "CO", connecticut: "CT", delaware: "DE",
  florida: "FL", georgia: "GA", hawaii: "HI", idaho: "ID",
  illinois: "IL", indiana: "IN", iowa: "IA", kansas: "KS",
  kentucky: "KY", louisiana: "LA", maine: "ME", maryland: "MD",
  massachusetts: "MA", michigan: "MI", minnesota: "MN", mississippi: "MS",
  missouri: "MO", montana: "MT", nebraska: "NE", nevada: "NV",
  "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
  "new york": "NY", "north carolina": "NC", "north dakota": "ND",
  ohio: "OH", oklahoma: "OK", oregon: "OR", pennsylvania: "PA",
  "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
  tennessee: "TN", texas: "TX", utah: "UT", vermont: "VT",
  virginia: "VA", washington: "WA", "west virginia": "WV",
  wisconsin: "WI", wyoming: "WY",
};

function resolveState(input: string): { abbr: string; name: string } | null {
  const trimmed = input.trim();
  // Already an abbreviation
  if (/^[A-Z]{2}$/.test(trimmed)) {
    const name = Object.entries(STATE_ABBR).find(([, v]) => v === trimmed)?.[0];
    return name ? { abbr: trimmed, name: name.replace(/\b\w/g, c => c.toUpperCase()) } : null;
  }
  const abbr = STATE_ABBR[trimmed.toLowerCase()];
  return abbr ? { abbr, name: trimmed.replace(/\b\w/g, c => c.toUpperCase()) } : null;
}

// ── WKT parser ────────────────────────────────────────────────────────────────

function parseWkt(wkt: string): { latitude: number; longitude: number } | null {
  const m = wkt.match(/POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)/i);
  if (!m) return null;
  return { longitude: parseFloat(m[1]), latitude: parseFloat(m[2]) };
}

// ── Address builder ───────────────────────────────────────────────────────────

function buildFormattedAddress(fields: string[]): string {
  return fields
    .map(f => f.trim())
    .filter(Boolean)
    .join(", ");
}

// ── State extraction from address fields ──────────────────────────────────────

// ADDR2 is typically "City, ST  ZIP" — extract the 2-letter state code.
function extractStateAbbr(addr2: string): string | null {
  const m = addr2.match(/,\s+([A-Z]{2})\s+\d/);
  return m ? m[1] : null;
}

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state?: string } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  if (!body.state) return json({ error: "state is required" }, 400);

  const resolved = resolveState(body.state);
  if (!resolved) return json({ error: `unrecognized state: ${body.state}` }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Download CSV from Storage ───────────────────────────────────────────────

  const { data: fileData, error: downloadError } = await supabase
    .storage
    .from(STORAGE_BUCKET)
    .download(CSV_PATH);

  if (downloadError) return json({ error: `storage: ${downloadError.message}` }, 500);

  const csvText = await fileData.text();

  // ── Parse and filter ────────────────────────────────────────────────────────

  const rows = parseCsv(csvText, { skipFirstRow: true, columns: [
    "WKT", "fid", "COUNTRY", "NAME", "ADDR1", "ADDR2", "ADDR3", "ADDR4", "ADDR5", "CAT_MOD",
  ]});

  const matched = rows.filter(r => {
    const abbr = extractStateAbbr(r.ADDR2 ?? "");
    return abbr === resolved.abbr;
  });

  // ── Transform ───────────────────────────────────────────────────────────────

  const records = matched.flatMap(r => {
    const coords = parseWkt(r.WKT ?? "");
    if (!coords) return [];                   // skip rows with unparseable geometry

    return [{
      src_fid:           parseInt(r.fid, 10),
      display_name:      r.NAME.trim(),
      latitude:          coords.latitude,
      longitude:         coords.longitude,
      raw_address:       buildFormattedAddress([r.ADDR1, r.ADDR2, r.ADDR3, r.ADDR4, r.ADDR5]),
      country:           r.COUNTRY?.trim() || null,
      quality_tier:      "bronze",
    }];
  });

  // ── Batch insert ─────────────────────────────────────────────────────────────

  let inserted = 0;
  let skipped  = 0;
  const errors: string[] = [];

  for (let i = 0; i < records.length; i += INSERT_BATCH_SIZE) {
    const batch = records.slice(i, i + INSERT_BATCH_SIZE);
    const { error, count } = await supabase
      .from("beaches_staging_new")
      .upsert(batch, { onConflict: "src_fid", ignoreDuplicates: true, count: "exact" });

    if (error) {
      errors.push(`batch ${i}–${i + batch.length}: ${error.message}`);
    } else {
      inserted += count ?? 0;
      skipped  += batch.length - (count ?? 0);
    }
  }

  return json({ state: resolved.name, matched: matched.length, inserted, skipped, errors });
});
