// load-beaches-staging/index.ts
// Loads beaches from US_beaches.csv in Supabase Storage into beaches_staging_new.
// Filters to a single state using PostGIS + the `states` table's polygons
// (replaces an earlier ADDR2 regex filter that silently dropped ~30% of
// landmark beaches whose source-CSV addresses were multi-line quoted or
// empty — including Torrey Pines, Malibu Surfrider, Huntington City Beach,
// Corona del Mar State Beach, etc.).
//
// POST { state: string }  — full name ("California") or abbreviation ("CA")
// Returns { state, parsed, matched, inserted, skipped, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { parse as parseCsv } from "https://deno.land/std@0.224.0/csv/mod.ts";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const STORAGE_BUCKET       = "pipeline";
const CSV_PATH             = "US_beaches.csv";
const RPC_BATCH_SIZE       = 500;

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
  wisconsin: "WI", wyoming: "WY", "district of columbia": "DC",
};

function resolveState(input: string): { abbr: string; name: string } | null {
  const trimmed = input.trim();
  if (/^[A-Z]{2}$/i.test(trimmed)) {
    const abbr = trimmed.toUpperCase();
    const name = Object.entries(STATE_ABBR).find(([, v]) => v === abbr)?.[0];
    return name ? { abbr, name: name.replace(/\b\w/g, c => c.toUpperCase()) } : null;
  }
  const abbr = STATE_ABBR[trimmed.toLowerCase()];
  return abbr ? { abbr, name: trimmed.replace(/\b\w/g, c => c.toUpperCase()) } : null;
}

function parseWkt(wkt: string): { latitude: number; longitude: number } | null {
  const m = wkt.match(/POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)/i);
  if (!m) return null;
  return { longitude: parseFloat(m[1]), latitude: parseFloat(m[2]) };
}

function buildFormattedAddress(fields: (string | undefined)[]): string {
  return fields.map(f => (f ?? "").trim()).filter(Boolean).join(", ");
}

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

  // Download the CSV from storage.
  const { data: fileData, error: downloadError } = await supabase
    .storage.from(STORAGE_BUCKET).download(CSV_PATH);
  if (downloadError) return json({ error: `storage: ${downloadError.message}` }, 500);

  const csvText = await fileData.text();

  // Parse all rows. No state filter here — that's done downstream by the
  // RPC using PostGIS. Rows without parseable coordinates are dropped
  // before we send them to the DB (those can't be placed anywhere).
  const rows = parseCsv(csvText, { skipFirstRow: true, columns: [
    "WKT", "fid", "COUNTRY", "NAME", "ADDR1", "ADDR2", "ADDR3", "ADDR4", "ADDR5", "CAT_MOD",
  ]}) as Record<string, string>[];

  const parsed: Array<{
    src_fid:      number;
    display_name: string;
    latitude:     number;
    longitude:    number;
    raw_address:  string | null;
    country:      string | null;
  }> = [];

  for (const r of rows) {
    const coords = parseWkt(r.WKT ?? "");
    if (!coords) continue;
    const fid = parseInt(r.fid ?? "", 10);
    if (!Number.isFinite(fid)) continue;
    const name = (r.NAME ?? "").trim();
    if (!name) continue;
    parsed.push({
      src_fid:      fid,
      display_name: name,
      latitude:     coords.latitude,
      longitude:    coords.longitude,
      raw_address:  buildFormattedAddress([r.ADDR1, r.ADDR2, r.ADDR3, r.ADDR4, r.ADDR5]) || null,
      country:      r.COUNTRY?.trim() || null,
    });
  }

  // Batch through the RPC so we don't blow the request size on Supabase's
  // JSON limits. 500 rows per call × ~150 bytes each = ~75KB payload.
  let totalMatched  = 0;
  let totalInserted = 0;
  const errors: string[] = [];

  for (let i = 0; i < parsed.length; i += RPC_BATCH_SIZE) {
    const batch = parsed.slice(i, i + RPC_BATCH_SIZE);
    const { data, error } = await supabase.rpc("ingest_beaches_batch_with_state_filter", {
      p_target_state_code: resolved.abbr,
      p_rows:              batch,
    });
    if (error) {
      errors.push(`batch ${i}–${i + batch.length}: ${error.message}`);
      continue;
    }
    const result = data as { matched?: number; inserted?: number; error?: string };
    if (result?.error) { errors.push(`batch ${i}: ${result.error}`); continue; }
    totalMatched  += result.matched  ?? 0;
    totalInserted += result.inserted ?? 0;
  }

  return json({
    state:    resolved.name,
    parsed:   parsed.length,
    matched:  totalMatched,
    inserted: totalInserted,
    skipped:  totalMatched - totalInserted,  // matched but already present (src_fid conflict)
    errors,
  });
});
