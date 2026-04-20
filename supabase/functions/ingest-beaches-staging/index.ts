// ingest-beaches-staging/index.ts
// Accepts a batch of geocoded beach records and upserts them into beaches_staging.
// Called by the local loader script — no service key needed on the client side.
//
// POST { records: BeachRecord[] }
// Returns { inserted: number, skipped: number, errors: string[] }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

interface BeachRecord {
  source_fid?:             number | null;
  display_name:            string;
  latitude:                number;
  longitude:               number;
  formatted_address?:      string | null;
  street_number?:          string | null;
  route?:                  string | null;
  city?:                   string | null;
  county?:                 string | null;
  state?:                  string | null;
  country?:                string | null;
  zip?:                    string | null;
  governing_jurisdiction?: string | null;
  governing_body?:         string | null;
  review_status?:          string | null;
  review_notes?:           string | null;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { records?: BeachRecord[] };
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON" }, 400);
  }

  const records = body.records;
  if (!Array.isArray(records) || records.length === 0) {
    return json({ error: "records array required" }, 400);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const rows = records
    .filter(r => r.latitude != null && r.longitude != null && r.display_name)
    .map(r => ({
      source_fid:             r.source_fid ?? null,
      display_name:           r.display_name.trim(),
      latitude:               r.latitude,
      longitude:              r.longitude,
      formatted_address:      r.formatted_address ?? null,
      street_number:          r.street_number     ?? null,
      route:                  r.route             ?? null,
      city:                   r.city              ?? null,
      county:                 r.county            ?? null,
      state:                  r.state             ?? null,
      country:                r.country           ?? null,
      zip:                    r.zip               ?? null,
      governing_jurisdiction: r.governing_jurisdiction ?? null,
      governing_body:         r.governing_body         ?? null,
      quality_tier:           "bronze",
      review_status:          r.review_status ?? "OK",
      review_notes:           r.review_notes  ?? null,
    }));

  const skipped = records.length - rows.length;

  const { error } = await supabase
    .from("beaches_staging")
    .upsert(rows, { onConflict: "source_fid" });

  if (error) return json({ error: error.message }, 500);

  return json({
    inserted: rows.length,
    skipped,
    errors: [],
  });
});
