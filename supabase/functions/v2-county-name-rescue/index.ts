// v2-county-name-rescue/index.ts
// Upgrades county_default records where the display_name or ccc_match_name
// contains an explicit "County Park" / "Regional Park" / "County Beach" /
// "Regional Beach" signal. These are records where CPAD county polygon match
// missed (possibly due to naming mismatch in CPAD's agency records) but the
// beach name itself makes the county-management relationship explicit.
//
// Upgrades source from county_default → county_name_rescue. Keeps
// governing_body = county name (populated by geocode).

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const COUNTY_NAME_PATTERNS: { pattern: RegExp; reason: string }[] = [
  { pattern: /\bCounty\s+Park\b/i,    reason: "name contains 'County Park'" },
  { pattern: /\bRegional\s+Park\b/i,  reason: "name contains 'Regional Park'" },
  { pattern: /\bCounty\s+Beach\b/i,   reason: "name contains 'County Beach'" },
  { pattern: /\bRegional\s+Beach\b/i, reason: "name contains 'Regional Beach'" },
];

function matchCounty(name: string): { match: boolean; reason: string } {
  for (const { pattern, reason } of COUNTY_NAME_PATTERNS) {
    if (pattern.test(name)) return { match: true, reason };
  }
  return { match: false, reason: "" };
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, ccc_match_name, county")
    .eq("governing_body_source", "county_default");
  if (error) return json({ error: error.message }, 500);
  if (!rows?.length) return json({ processed: 0, matched: 0 });

  const matches: Array<{
    id: number; display_name: string; ccc_name: string | null; source: string; reason: string; county: string;
  }> = [];
  for (const r of rows) {
    const displayMatch = matchCounty(r.display_name);
    if (displayMatch.match) {
      matches.push({
        id: r.id, display_name: r.display_name, ccc_name: r.ccc_match_name,
        source: "display_name", reason: displayMatch.reason, county: r.county,
      });
      continue;
    }
    if (r.ccc_match_name) {
      const cccMatch = matchCounty(r.ccc_match_name);
      if (cccMatch.match) {
        matches.push({
          id: r.id, display_name: r.display_name, ccc_name: r.ccc_match_name,
          source: "ccc_match_name", reason: cccMatch.reason, county: r.county,
        });
      }
    }
  }

  if (body.dry_run) {
    return json({ dry_run: true, processed: rows.length, matched: matches.length, preview: matches });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const m of matches) {
    const note = m.source === "ccc_match_name"
      ? `CCC-matched name "${m.ccc_name}" ${m.reason}. Classification upgraded from county default.`
      : `Display name ${m.reason}. Classification upgraded from county default.`;
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_body_source: "county_name_rescue",
        governing_body_notes:  note,
        review_notes:          "County classification corroborated by explicit name signal.",
      })
      .eq("id", m.id);
    if (error) writeErrors.push(`id ${m.id}: ${error.message}`);
    else updated++;
  }

  return json({ processed: rows.length, matched: matches.length, updated, errors: writeErrors });
});
