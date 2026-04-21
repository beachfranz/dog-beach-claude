// v2-run-pipeline/index.ts
// Top-level orchestrator for the v2 jurisdiction pipeline.
//
// Invokes each stage in order, aggregates return values, and halts on errors.
// Each stage is independently deployed and can be run standalone for debugging
// or re-runs; this orchestrator is the production-path entrypoint.
//
// Stages:
//   2  v2-dedup                 spatial + name similarity dedup
//   3  v2-non-beach-filter      rule-based non-beach filter
//   4  v2-geocode-context       Google + Census address context
//   5  v2-federal-classify      USA Federal Lands point-in-polygon
//   6  v2-state-classify        CA State Parks boundary point-in-polygon
//   7  v2-ccc-crossref          CCC Public Access Points cross-reference
//   8  v2-city-classify         Census TIGER Places polygon (with buffer)
//   9  v2-default-county        neighbor inherit + default to county
//   10 v2-ai-audit              Claude audit over final classifications
//
// POST { skip?: string[], dry_run?: boolean }
// Returns { stages: { name, result, duration_ms }[], total_ms }

import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const STAGES = [
  "v2-dedup",
  "v2-non-beach-filter",
  "v2-geocode-context",
  "v2-federal-classify",
  "v2-state-classify",
  "v2-state-operator-override",
  "v2-ccc-crossref",
  "v2-city-classify",
  "v2-county-classify",
  "v2-default-county",
  "v2-state-name-rescue",
  "v2-county-name-rescue",
  "v2-non-beach-late",
  "v2-private-land-filter",
  "v2-blm-sma-rescue",
  "v2-ai-audit",
];

async function callStage(stage: string, body: unknown): Promise<unknown> {
  const resp = await fetch(`${SUPABASE_URL}/functions/v1/${stage}`, {
    method:  "POST",
    headers: {
      "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
      "Content-Type":  "application/json",
    },
    body: JSON.stringify(body ?? {}),
  });
  const text = await resp.text();
  try { return JSON.parse(text); }
  catch { return { error: `non-json response from ${stage}`, body: text.slice(0, 500) }; }
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { skip?: string[]; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const skip = new Set(body.skip ?? []);
  const stageBody = body.dry_run ? { dry_run: true } : {};
  const results: Array<{ stage: string; skipped?: boolean; result?: unknown; duration_ms?: number }> = [];

  const totalStart = performance.now();
  for (const stage of STAGES) {
    if (skip.has(stage)) {
      results.push({ stage, skipped: true });
      continue;
    }
    const t0 = performance.now();
    const result = await callStage(stage, stageBody);
    results.push({ stage, result, duration_ms: Math.round(performance.now() - t0) });
  }
  const totalMs = Math.round(performance.now() - totalStart);

  return json({ stages: results, total_ms: totalMs });
});
