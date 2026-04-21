// v2-non-beach-filter/index.ts
// Pipeline stage 3 — rule-based regex filter for obvious non-beaches.
//
// Only matches on the display_name itself, not on LLM knowledge of whether
// the place exists. "I've never heard of it" is NOT evidence it isn't a beach.
//
// Flags: business names (deli, bar, psychic, garage, restaurant, cafe),
//        explicit surf schools/camps, HOA in name, street address suffixes.
//
// POST { dry_run?: boolean }
// Returns { marked, preview }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const NON_BEACH_PATTERNS: { pattern: RegExp; reason: string }[] = [
  { pattern: /\bdeli\b/i,                          reason: "Name contains 'deli' — a food business." },
  { pattern: /\bpsychic\b/i,                       reason: "Name contains 'psychic' — a business type." },
  { pattern: /\bgarage\b/i,                        reason: "Name contains 'garage' — a business type." },
  { pattern: /&\s*bar\b/i,                         reason: "Name contains '& Bar' — a commercial bar." },
  { pattern: /\brestaurant\b/i,                    reason: "Name contains 'restaurant' — a food business." },
  { pattern: /\bcafe\b/i,                          reason: "Name contains 'cafe' — a food business." },
  { pattern: /\bsurf\s+(school|camp|lesson)/i,     reason: "Name contains surf school/camp — a commercial operation." },
  { pattern: /\b(lessons|rentals|tours|charters)\b/i, reason: "Name contains commercial service term." },
  { pattern: /\bhoa\b/i,                           reason: "Name explicitly contains 'HOA' — private homeowner association." },
  { pattern: /\b(lane|drive|avenue|blvd|street|road|court)\s*$/i, reason: "Name ends with a street suffix — likely an address." },
];

function checkName(name: string): { invalid: boolean; reason: string } {
  for (const { pattern, reason } of NON_BEACH_PATTERNS) {
    if (pattern.test(name)) return { invalid: true, reason };
  }
  return { invalid: false, reason: "" };
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name")
    .is("review_status", null);
  if (error) return json({ error: error.message }, 500);

  const hits: Array<{ id: number; display_name: string; reason: string }> = [];
  for (const r of rows ?? []) {
    const check = checkName(r.display_name);
    if (check.invalid) hits.push({ id: r.id, display_name: r.display_name, reason: check.reason });
  }

  if (body.dry_run) {
    return json({ dry_run: true, would_mark: hits.length, preview: hits });
  }

  let marked = 0;
  for (const h of hits) {
    const { error: uErr } = await supabase
      .from("beaches_staging_new")
      .update({
        review_status: "invalid",
        review_notes:  `Not a beach: ${h.reason}`,
      })
      .eq("id", h.id);
    if (!uErr) marked++;
  }

  return json({ marked, preview: hits });
});
