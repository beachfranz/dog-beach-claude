// v2-state-name-rescue/index.ts
// Rescue stage — catches state beaches our state polygon missed, identified
// by an explicit "State Beach/Park/Recreation Area/Reserve" signal in either
// the display_name OR the ccc_match_name (CCC's curated name).
//
// Only upgrades records where governing_body_source = 'county_default' —
// i.e. beaches that fell through all the polygon classifiers. This avoids
// fighting with correctly-classified records.
//
// Also avoids Corona del Mar / Tamarack-style false positives because those
// beaches wouldn't be county_default (they'd have hit city polygon).

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Patterns that unambiguously indicate state-managed land in a place name.
const STATE_NAME_PATTERNS: { pattern: RegExp; reason: string }[] = [
  { pattern: /\bState\s+Beach\b/i,            reason: "name contains 'State Beach'" },
  { pattern: /\bState\s+Park\b/i,             reason: "name contains 'State Park'" },
  { pattern: /\bState\s+Recreation\s+Area\b/i, reason: "name contains 'State Recreation Area'" },
  { pattern: /\bState\s+Reserve\b/i,          reason: "name contains 'State Reserve'" },
  { pattern: /\bState\s+Marine\b/i,           reason: "name contains 'State Marine' (reserve/conservation area)" },
  { pattern: /\bState\s+Historic\b/i,         reason: "name contains 'State Historic'" },
];

function matchState(name: string): { match: boolean; reason: string } {
  for (const { pattern, reason } of STATE_NAME_PATTERNS) {
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
    .select("id, display_name, ccc_match_name, governing_body_source")
    .eq("governing_body_source", "county_default");
  if (error) return json({ error: error.message }, 500);
  if (!rows?.length) return json({ processed: 0, matched: 0 });

  const matches: Array<{
    id: number; display_name: string; ccc_name: string | null; source: string; reason: string;
  }> = [];
  for (const r of rows) {
    const displayMatch = matchState(r.display_name);
    if (displayMatch.match) {
      matches.push({
        id: r.id, display_name: r.display_name, ccc_name: r.ccc_match_name,
        source: "display_name", reason: displayMatch.reason,
      });
      continue;
    }
    if (r.ccc_match_name) {
      const cccMatch = matchState(r.ccc_match_name);
      if (cccMatch.match) {
        matches.push({
          id: r.id, display_name: r.display_name, ccc_name: r.ccc_match_name,
          source: "ccc_match_name", reason: cccMatch.reason,
        });
      }
    }
  }

  if (body.dry_run) {
    return json({
      dry_run:   true,
      processed: rows.length,
      matched:   matches.length,
      preview:   matches.map(m => ({
        display_name: m.display_name,
        ccc_name:     m.ccc_name,
        signal:       `${m.source}: ${m.reason}`,
      })),
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const m of matches) {
    const note = m.source === "ccc_match_name"
      ? `CCC-matched name "${m.ccc_name}" ${m.reason}. Classification rescued from county default to state.`
      : `Display name ${m.reason}. Classification rescued from county default to state.`;
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_jurisdiction: "governing state",
        governing_body:         "California (State Parks)",
        governing_body_source:  "state_name_rescue",
        governing_body_notes:   note,
        review_notes:           "Rescued to state via explicit state-park name signal.",
      })
      .eq("id", m.id);
    if (error) writeErrors.push(`id ${m.id}: ${error.message}`);
    else updated++;
  }

  return json({
    processed: rows.length,
    matched:   matches.length,
    updated,
    errors:    writeErrors,
  });
});
