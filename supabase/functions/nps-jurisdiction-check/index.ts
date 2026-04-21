// nps-jurisdiction-check/index.ts
// Matches beaches against cached NPS places using two SQL RPCs:
//   1. match_beaches_nps_proximity — haversine ≤ 300m, requires name_sim ≥ 0.20
//   2. match_beaches_nps_name      — trgm LATERAL, name_sim ≥ 0.65, distance ≤ 20km
//
// Proximity beats name if both fire for the same beach.
// Writes nps_match_score, nps_match_name, nps_match_park to all beaches.
// With { apply: true } sets governing_jurisdiction = "governing federal" for matches.
//
// Run load-nps-places first.
//
// POST { apply?: boolean }
// Returns { total_beaches, confident, proximity_weak, unmatched, updated, preview }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const PROXIMITY_M         = 300;
const PROXIMITY_MIN_SIM   = 0.20;
const NAME_THRESHOLD      = 0.65;
const NAME_MAX_DIST_M     = 20_000;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { apply?: boolean } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Run both RPCs in parallel ───────────────────────────────────────────────
  const [proxResult, nameResult, beachCount] = await Promise.all([
    supabase.rpc("match_beaches_nps_proximity", { proximity_m: PROXIMITY_M }),
    supabase.rpc("match_beaches_nps_name",      { name_threshold: NAME_THRESHOLD }),
    supabase.from("beaches_staging_new").select("id, governing_jurisdiction", { count: "exact" }),
  ]);

  if (proxResult.error) return json({ error: proxResult.error.message }, 500);
  if (nameResult.error) return json({ error: nameResult.error.message }, 500);

  const currentJuris = new Map(
    (beachCount.data ?? []).map(r => [r.id, r.governing_jurisdiction as string])
  );

  // ── Merge: proximity wins; filter out weak/noisy matches ───────────────────
  interface Match {
    beach_id:     number;
    nps_title:    string;
    nps_park:     string;
    distance_m:   number | null;
    score:        number;
    signal:       "proximity" | "proximity_weak" | "name";
    is_confident: boolean;
  }

  const byId = new Map<number, Match>();

  // Proximity matches first
  for (const m of proxResult.data ?? []) {
    const confident = m.name_similarity >= PROXIMITY_MIN_SIM;
    byId.set(m.beach_id, {
      beach_id:     m.beach_id,
      nps_title:    m.nps_title,
      nps_park:     m.nps_park,
      distance_m:   Math.round(m.distance_m),
      score:        Math.round(m.name_similarity * 100) / 100,
      signal:       confident ? "proximity" : "proximity_weak",
      is_confident: confident,
    });
  }

  // Name matches for beaches not already in a confident proximity hit
  for (const m of nameResult.data ?? []) {
    if (byId.has(m.beach_id)) continue;              // proximity already covers this
    if (m.distance_m !== null && m.distance_m > NAME_MAX_DIST_M) continue;  // too far
    byId.set(m.beach_id, {
      beach_id:     m.beach_id,
      nps_title:    m.nps_title,
      nps_park:     m.nps_park,
      distance_m:   m.distance_m !== null ? Math.round(m.distance_m) : null,
      score:        Math.round(m.name_similarity * 100) / 100,
      signal:       "name",
      is_confident: true,
    });
  }

  // ── Reset all, then write matches ───────────────────────────────────────────
  await supabase
    .from("beaches_staging_new")
    .update({ nps_match_score: 0, nps_match_name: null, nps_match_park: null })
    .neq("id", 0);

  const writeErrors: string[] = [];
  let updated = 0;

  for (const m of byId.values()) {
    const fields: Record<string, unknown> = {
      nps_match_score: m.score,
      nps_match_name:  m.nps_title,
      nps_match_park:  m.nps_park,
    };

    if (body.apply && m.is_confident && currentJuris.get(m.beach_id) !== "governing federal") {
      fields.governing_jurisdiction = "governing federal";
      fields.governing_body         = m.nps_park;
      fields.governing_body_source  = "nps_api";
      fields.governing_body_notes   =
        `NPS match via ${m.signal}: "${m.nps_title}"` +
        (m.distance_m !== null ? ` (${m.distance_m}m)` : "") +
        ` in ${m.nps_park} (score ${m.score}).`;
      updated++;
    }

    const { error } = await supabase
      .from("beaches_staging_new").update(fields).eq("id", m.beach_id);
    if (error) writeErrors.push(`id ${m.beach_id}: ${error.message}`);
  }

  const confident    = [...byId.values()].filter(m => m.is_confident);
  const proxWeak     = [...byId.values()].filter(m => m.signal === "proximity_weak");
  const total        = beachCount.count ?? 0;

  return json({
    total_beaches:   total,
    confident:       confident.length,
    proximity_weak:  proxWeak.length,
    unmatched:       total - byId.size,
    updated,
    errors:          writeErrors,
    preview_confident: confident
      .map(m => ({
        display_name:   m.nps_title,   // will be overwritten below
        nps_match_name: m.nps_title,
        nps_match_park: m.nps_park,
        signal:         m.signal,
        distance_m:     m.distance_m,
        score:          m.score,
        was:            currentJuris.get(m.beach_id),
      }))
      .sort((a, b) => (a.distance_m ?? 99999) - (b.distance_m ?? 99999)),
    preview_weak: proxWeak
      .map(m => ({
        nps_match_name: m.nps_title,
        nps_match_park: m.nps_park,
        distance_m:     m.distance_m,
        score:          m.score,
      }))
      .sort((a, b) => (a.distance_m ?? 99999) - (b.distance_m ?? 99999)),
  });
});
