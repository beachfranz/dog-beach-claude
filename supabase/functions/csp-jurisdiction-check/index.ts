// csp-jurisdiction-check/index.ts
// Matches beaches against cached CSP places (csp_places table) using two SQL RPCs:
//   1. match_beaches_csp_proximity — haversine ≤ 300m, requires name_sim ≥ 0.20
//   2. match_beaches_csp_name      — trgm LATERAL, name_sim ≥ 0.65, distance ≤ 20km
//
// Proximity beats name if both fire for the same beach.
// Writes csp_match_score, csp_match_name to all beaches.
// With { apply: true } sets governing_jurisdiction = "governing state" and
// governing_body = the CSP park name for confident matches.
//
// Run load-csp-places first.
//
// POST { apply?: boolean }
// Returns { total_beaches, confident, proximity_weak, unmatched, updated, preview }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const PROXIMITY_M       = 300;
const PROXIMITY_MIN_SIM = 0.20;
const NAME_THRESHOLD    = 0.65;
const NAME_MAX_DIST_M   = 20_000;

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
  const [proxResult, nameResult, beachResult] = await Promise.all([
    supabase.rpc("match_beaches_csp_proximity", { proximity_m: PROXIMITY_M }),
    supabase.rpc("match_beaches_csp_name",      { name_threshold: NAME_THRESHOLD }),
    supabase.from("beaches_staging_new").select("id, governing_jurisdiction", { count: "exact" }),
  ]);

  if (proxResult.error) return json({ error: proxResult.error.message }, 500);
  if (nameResult.error) return json({ error: nameResult.error.message }, 500);

  const currentJuris = new Map(
    (beachResult.data ?? []).map(r => [r.id, r.governing_jurisdiction as string])
  );

  // ── Merge results ───────────────────────────────────────────────────────────
  interface Match {
    beach_id:     number;
    csp_name:     string;
    distance_m:   number | null;
    score:        number;
    signal:       "proximity" | "proximity_weak" | "name";
    is_confident: boolean;
  }

  const byId = new Map<number, Match>();

  for (const m of proxResult.data ?? []) {
    const confident = m.name_similarity >= PROXIMITY_MIN_SIM;
    byId.set(m.beach_id, {
      beach_id:     m.beach_id,
      csp_name:     m.csp_name,
      distance_m:   Math.round(m.distance_m),
      score:        Math.round(m.name_similarity * 100) / 100,
      signal:       confident ? "proximity" : "proximity_weak",
      is_confident: confident,
    });
  }

  for (const m of nameResult.data ?? []) {
    if (byId.has(m.beach_id)) continue;
    if (m.distance_m !== null && m.distance_m > NAME_MAX_DIST_M) continue;
    byId.set(m.beach_id, {
      beach_id:     m.beach_id,
      csp_name:     m.csp_name,
      distance_m:   m.distance_m !== null ? Math.round(m.distance_m) : null,
      score:        Math.round(m.name_similarity * 100) / 100,
      signal:       "name",
      is_confident: true,
    });
  }

  // ── Reset all, then write matches ───────────────────────────────────────────
  await supabase
    .from("beaches_staging_new")
    .update({ csp_match_score: 0, csp_match_name: null })
    .neq("id", 0);

  const writeErrors: string[] = [];
  let updated = 0;

  for (const m of byId.values()) {
    const fields: Record<string, unknown> = {
      csp_match_score: m.score,
      csp_match_name:  m.csp_name,
    };

    if (body.apply && m.is_confident && currentJuris.get(m.beach_id) !== "governing state") {
      fields.governing_jurisdiction = "governing state";
      fields.governing_body         = m.csp_name;
      fields.governing_body_source  = "csp_arcgis";
      fields.governing_body_notes   =
        `CSP match via ${m.signal}: "${m.csp_name}"` +
        (m.distance_m !== null ? ` (${m.distance_m}m)` : "") +
        ` (score ${m.score}).`;
      updated++;
    }

    const { error } = await supabase
      .from("beaches_staging_new").update(fields).eq("id", m.beach_id);
    if (error) writeErrors.push(`id ${m.beach_id}: ${error.message}`);
  }

  const confident  = [...byId.values()].filter(m => m.is_confident);
  const proxWeak   = [...byId.values()].filter(m => m.signal === "proximity_weak");
  const total      = beachResult.count ?? 0;

  // Fetch display names for the preview
  const matchedIds = [...byId.keys()];
  const { data: nameRows } = matchedIds.length
    ? await supabase.from("beaches_staging_new").select("id, display_name").in("id", matchedIds)
    : { data: [] };
  const nameMap = new Map((nameRows ?? []).map(r => [r.id, r.display_name]));

  return json({
    total_beaches:  total,
    confident:      confident.length,
    proximity_weak: proxWeak.length,
    unmatched:      total - byId.size,
    updated,
    errors:         writeErrors,
    preview_confident: confident
      .map(m => ({
        display_name: nameMap.get(m.beach_id),
        csp_name:     m.csp_name,
        signal:       m.signal,
        distance_m:   m.distance_m,
        score:        m.score,
        was:          currentJuris.get(m.beach_id),
      }))
      .sort((a, b) => (a.distance_m ?? 99999) - (b.distance_m ?? 99999)),
    preview_weak: proxWeak
      .map(m => ({
        display_name: nameMap.get(m.beach_id),
        csp_name:     m.csp_name,
        distance_m:   m.distance_m,
        score:        m.score,
      }))
      .sort((a, b) => (a.distance_m ?? 99999) - (b.distance_m ?? 99999)),
  });
});
