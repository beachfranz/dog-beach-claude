// v2-dedup/index.ts
// Pipeline stage 2 — spatial + name similarity dedup, run BEFORE any API calls.
//
// Finds pairs of active records within 50m and with name similarity >= 0.5
// (pg_trgm). Winner is chosen by (locked > unlocked) > (longer name) > (lower id).
// Loser gets review_status = 'duplicate' with a note pointing to the winner.
//
// Runs repeatedly until no more duplicate pairs are found (handles transitive
// cases where A ~ B ~ C all at 40m apart).
//
// POST { dry_run?: boolean, max_distance_m?: number, min_similarity?: number }
// Returns { marked, iterations, pairs }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const DEFAULT_MAX_DIST_M  = 50;
const DEFAULT_MIN_SIM     = 0.5;
const MAX_ITERATIONS      = 5;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { dry_run?: boolean; max_distance_m?: number; min_similarity?: number } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  const maxDist = body.max_distance_m ?? DEFAULT_MAX_DIST_M;
  const minSim  = body.min_similarity  ?? DEFAULT_MIN_SIM;
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  let totalMarked = 0;
  let iterations  = 0;
  const pairs: Array<{ winner: string; loser: string; dist_m: number; sim: number }> = [];

  while (iterations < MAX_ITERATIONS) {
    iterations++;

    const { data: rows, error } = await supabase.rpc("v2_find_dedup_pairs", {
      max_distance_m: maxDist,
      min_similarity: minSim,
    });
    if (error) return json({ error: error.message }, 500);

    const foundPairs = rows ?? [];
    if (foundPairs.length === 0) break;

    if (body.dry_run) {
      for (const p of foundPairs) {
        pairs.push({
          winner: `id=${p.winner_id} "${p.winner_name}"`,
          loser:  `id=${p.loser_id} "${p.loser_name}"`,
          dist_m: Math.round(p.dist_m),
          sim:    Math.round(p.name_sim * 100) / 100,
        });
      }
      break;
    }

    for (const p of foundPairs) {
      const note = `Duplicate of id ${p.winner_id} ("${p.winner_name}"); ${Math.round(p.dist_m)}m apart, name similarity ${Math.round(p.name_sim * 100) / 100}.`;
      const { error: uErr } = await supabase
        .from("beaches_staging_new")
        .update({ review_status: "duplicate", review_notes: note })
        .eq("id", p.loser_id);
      if (!uErr) totalMarked++;
      pairs.push({
        winner: `id=${p.winner_id} "${p.winner_name}"`,
        loser:  `id=${p.loser_id} "${p.loser_name}"`,
        dist_m: Math.round(p.dist_m),
        sim:    Math.round(p.name_sim * 100) / 100,
      });
    }
  }

  return json({
    dry_run:    !!body.dry_run,
    marked:     totalMarked,
    iterations,
    pairs,
  });
});
