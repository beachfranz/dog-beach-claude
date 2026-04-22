// v2-private-land-filter/index.ts
// Marks known private-land beaches as invalid. Config-driven: reads
// private_land_zones table for bbox definitions per state.
//
// Also keeps a narrow CCC-name regex for the few beaches whose lat/lon is
// outside the bbox but whose CCC-matched name explicitly cites private land
// (e.g. "(17-Mile Drive)"). This regex could later move to a config table
// column; for now it's small and state-independent.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Generic private-land signatures that can appear in CCC-matched names.
const PRIVATE_NAME_PATTERNS: { pattern: RegExp; reason: string }[] = [
  { pattern: /\b17-Mile\s+Drive\b/i,
    reason: "17-Mile Drive tourist stop, Del Monte Forest — private Pebble Beach Company land." },
];

interface Zone {
  name: string; min_lat: number; max_lat: number; min_lon: number; max_lon: number; reason: string;
}

function checkPrivate(
  cccName: string | null, lat: number, lon: number, zones: Zone[],
): { private: boolean; reason: string } {
  if (cccName) {
    for (const { pattern, reason } of PRIVATE_NAME_PATTERNS) {
      if (pattern.test(cccName)) return { private: true, reason };
    }
  }
  for (const z of zones) {
    if (lat >= z.min_lat && lat <= z.max_lat && lon >= z.min_lon && lon <= z.max_lon) {
      return { private: true, reason: z.reason || `Inside ${z.name}.` };
    }
  }
  return { private: false, reason: "" };
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

  const { data: zoneRows, error: zErr } = await supabase
    .from("private_land_zones")
    .select("name, min_lat, max_lat, min_lon, max_lon, reason")
    .eq("state_code", stateCode)
    .eq("active", true);
  if (zErr) return json({ error: zErr.message }, 500);

  const zones: Zone[] = (zoneRows ?? []).map((z: Record<string, unknown>) => ({
    name:    String(z.name),
    min_lat: Number(z.min_lat),
    max_lat: Number(z.max_lat),
    min_lon: Number(z.min_lon),
    max_lon: Number(z.max_lon),
    reason:  String(z.reason ?? ""),
  }));

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, ccc_match_name, latitude, longitude, state")
    .eq("governing_body_source", "county_default");
  if (error) return json({ error: error.message }, 500);
  const stateFiltered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (!stateFiltered.length) return json({ state_code: stateCode, processed: 0, matched: 0 });

  const matches: Array<{ id: number; display_name: string; reason: string }> = [];
  for (const r of stateFiltered) {
    if (r.latitude === null || r.longitude === null) continue;
    const c = checkPrivate(r.ccc_match_name, r.latitude, r.longitude, zones);
    if (c.private) matches.push({ id: r.id, display_name: r.display_name, reason: c.reason });
  }

  if (body.dry_run) return json({ dry_run: true, state_code: stateCode, zones: zones.length, processed: stateFiltered.length, matched: matches.length, preview: matches });

  let updated = 0;
  const writeErrors: string[] = [];
  for (const m of matches) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({ review_status: "invalid", review_notes: `Private land: ${m.reason}` })
      .eq("id", m.id);
    if (error) writeErrors.push(`id ${m.id}: ${error.message}`);
    else updated++;
  }

  return json({ state_code: stateCode, zones: zones.length, processed: stateFiltered.length, matched: matches.length, updated, errors: writeErrors });
});
