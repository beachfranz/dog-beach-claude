// v2-private-land-filter/index.ts
// Marks known private-land beaches as invalid for public jurisdictional
// purposes. These are beaches on private property (corporate, developer-owned,
// private gated communities) where there is no public jurisdiction and where
// access is typically restricted or fee-based.
//
// Most prominent example: Del Monte Forest / 17-Mile Drive beaches. Owned
// and operated by Pebble Beach Company, a private corporation. Fanshell
// Beach, Point Joe, Bird Rock, Moss Beach, Stillwater Cove, Seal Rock beaches
// are tourist photo stops on the 17-Mile Drive scenic route but are all on
// private property.
//
// Only marks records that are currently in county_default. Private beaches
// that were (wrongly) caught by another polygon are left alone — that's a
// separate problem.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Known private land signatures in ccc_match_name.
const PRIVATE_CCC_PATTERNS: { pattern: RegExp; reason: string }[] = [
  { pattern: /\b17-Mile\s+Drive\b/i,
    reason: "17-Mile Drive tourist stop, Del Monte Forest — private Pebble Beach Company land." },
];

// Bounding-box private land zones. Catches the Del Monte Forest beaches whose
// CCC names don't explicitly say "17-Mile Drive" (Fanshell, Point Joe,
// Stillwater Cove, Seal Rock). Scoped tightly so as to not catch Sonoma's
// Stillwater Cove County Regional Park.
const PRIVATE_BBOXES: { minLat: number; maxLat: number; minLon: number; maxLon: number; reason: string }[] = [
  {
    minLat: 36.555, maxLat: 36.615,
    minLon: -121.985, maxLon: -121.935,
    reason: "Inside Del Monte Forest / 17-Mile Drive — private Pebble Beach Company land.",
  },
];

function checkPrivate(
  displayName: string,
  cccName: string | null,
  lat: number,
  lon: number,
): { private: boolean; reason: string } {
  if (cccName) {
    for (const { pattern, reason } of PRIVATE_CCC_PATTERNS) {
      if (pattern.test(cccName)) return { private: true, reason };
    }
  }
  for (const b of PRIVATE_BBOXES) {
    if (lat >= b.minLat && lat <= b.maxLat && lon >= b.minLon && lon <= b.maxLon) {
      return { private: true, reason: b.reason };
    }
  }
  return { private: false, reason: "" };
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
    .select("id, display_name, ccc_match_name, latitude, longitude, governing_body_source")
    .eq("governing_body_source", "county_default");
  if (error) return json({ error: error.message }, 500);
  if (!rows?.length) return json({ processed: 0, matched: 0 });

  const matches: Array<{
    id: number; display_name: string; ccc_name: string | null; reason: string;
  }> = [];
  for (const r of rows) {
    if (r.latitude === null || r.longitude === null) continue;
    const check = checkPrivate(r.display_name, r.ccc_match_name, r.latitude, r.longitude);
    if (check.private) {
      matches.push({
        id:           r.id,
        display_name: r.display_name,
        ccc_name:     r.ccc_match_name,
        reason:       check.reason,
      });
    }
  }

  if (body.dry_run) {
    return json({
      dry_run:   true,
      processed: rows.length,
      matched:   matches.length,
      preview:   matches,
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const m of matches) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        review_status: "invalid",
        review_notes:  `Private land: ${m.reason}`,
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
