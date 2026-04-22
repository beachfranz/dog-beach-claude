// v2-non-beach-late/index.ts
// Late-stage non-beach filter that runs on county_default records (which
// have review_status = 'ready'). The early v2-non-beach-filter only checks
// unclassified records; this one covers records that slipped through.
//
// Matches:
//   - Names ending in "River" (rivers, not beaches)
//   - Specific ambiguous region names ("Lost coast", "The Wall")
//   - Other geographic features that aren't beaches per se

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const NON_BEACH_LATE_PATTERNS: { pattern: RegExp; reason: string }[] = [
  { pattern: /\bRiver\s*$/i,       reason: "Name ends with 'River' — a river, not a beach." },
  { pattern: /^Lost\s+coast\b/i,   reason: "'Lost coast' is a broad coastal region (~80 miles), not a specific beach." },
  { pattern: /^The\s+Wall\s*$/i,   reason: "'The Wall' is an ambiguous surf feature, not a distinct beach." },
];

function checkNonBeach(name: string): { invalid: boolean; reason: string } {
  for (const { pattern, reason } of NON_BEACH_LATE_PATTERNS) {
    if (pattern.test(name)) return { invalid: true, reason };
  }
  return { invalid: false, reason: "" };
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state_code?: string; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const stateCode = body.state_code ?? "CA";
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, governing_body_source, state")
    .in("governing_body_source", ["county_default", "state_default"]);
  if (error) return json({ error: error.message }, 500);
  const filtered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (!filtered.length) return json({ state_code: stateCode, processed: 0, marked: 0 });

  const hits: Array<{ id: number; display_name: string; reason: string }> = [];
  for (const r of filtered) {
    const c = checkNonBeach(r.display_name);
    if (c.invalid) hits.push({ id: r.id, display_name: r.display_name, reason: c.reason });
  }

  if (body.dry_run) {
    return json({ dry_run: true, state_code: stateCode, processed: filtered.length, matched: hits.length, preview: hits });
  }

  let marked = 0;
  const writeErrors: string[] = [];
  for (const h of hits) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        review_status: "invalid",
        review_notes:  `Not a distinct beach: ${h.reason}`,
      })
      .eq("id", h.id);
    if (error) writeErrors.push(`id ${h.id}: ${error.message}`);
    else marked++;
  }

  return json({ state_code: stateCode, processed: filtered.length, matched: hits.length, marked, errors: writeErrors });
});
