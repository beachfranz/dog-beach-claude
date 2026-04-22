// v2-blm-sma-rescue/index.ts
// Rescue stage — catches federal and private lands the primary polygon
// classifiers missed, using the BLM Surface Management Agency service.
//
// Config-driven:
//   - source_key='blm_sma' (CA) or 'blm_sma_national' (other states, fallback)
//   - sma_code_mappings table gives agency name + type per SMA_ID

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { buildArcgisQueryUrl, extractField, getSource, getStateConfig, PipelineSource, stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const CONCURRENCY = 10;

interface SmaMapping { sma_id: number; agency_name: string; agency_type: string; is_public: boolean; }

async function findSMA(lat: number, lon: number, source: PipelineSource): Promise<{ sma_id: number; unit: string | null } | null> {
  const url = buildArcgisQueryUrl(source, {
    geometry:       `${lon},${lat}`,
    geometryType:   "esriGeometryPoint",
    spatialRel:     "esriSpatialRelIntersects",
    inSR:           "4326",
    returnGeometry: "false",
  });
  try {
    const resp = await fetch(url);
    if (!resp.ok) return null;
    const data = await resp.json();
    const features = data?.features ?? [];
    if (features.length === 0) return null;
    const attrs = features[0].attributes ?? {};
    const id = Number(extractField(source, attrs, "sma_id"));
    return { sma_id: id, unit: extractField(source, attrs, "unit") };
  } catch { return null; }
}

async function pLimit<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let i = 0;
  async function worker() { while (i < tasks.length) { const n = i++; results[n] = await tasks[n](); } }
  await Promise.all(Array.from({ length: limit }, worker));
  return results;
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

  const stateCfg       = await getStateConfig(supabase, stateCode);
  const beachBillState = stateCfg?.beach_bill_override === true;

  // Try state-specific BLM SMA source first (blm_sma), then fall back to
  // national (blm_sma_national).
  const source = (await getSource(supabase, "blm_sma", stateCode))
              ?? (await getSource(supabase, "blm_sma_national"));
  if (!source) return json({ error: "No blm_sma or blm_sma_national source configured" }, 500);

  const { data: smaRows, error: sErr } = await supabase
    .from("sma_code_mappings").select("*");
  if (sErr) return json({ error: sErr.message }, 500);
  const smaById = new Map<number, SmaMapping>();
  for (const r of smaRows ?? []) smaById.set(Number(r.sma_id), r as SmaMapping);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude, state")
    .in("governing_body_source", ["county_default", "state_default"])
    .not("latitude", "is", null)
    .not("longitude", "is", null);
  if (error) return json({ error: error.message }, 500);
  const stateFiltered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (!stateFiltered.length) return json({ state_code: stateCode, processed: 0, matched: 0 });

  const tasks = stateFiltered.map(r => async () => ({
    id: r.id, display_name: r.display_name,
    hit: await findSMA(r.latitude, r.longitude, source),
  }));
  const results = await pLimit(tasks, CONCURRENCY);

  const federal:     typeof results = [];
  const privateLand: typeof results = [];
  const other:       typeof results = [];
  for (const r of results) {
    if (!r.hit) continue;
    const m = smaById.get(r.hit.sma_id);
    if (!m) { other.push(r); continue; }
    if (m.agency_type === "federal" || m.agency_type === "tribal") federal.push(r);
    else if (m.agency_type === "private")                           privateLand.push(r);
    else                                                            other.push(r);
  }

  if (body.dry_run) {
    return json({
      dry_run: true, state_code: stateCode,
      source: { url: source.url, priority: source.priority, state_code: source.state_code },
      beach_bill_override: beachBillState,
      processed: stateFiltered.length, federal: federal.length, private: privateLand.length, other: other.length,
      private_action: beachBillState ? "skip (beach_bill_override=true)" : "invalidate",
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];

  for (const r of federal) {
    const mapping = smaById.get(r.hit!.sma_id)!;
    const agency = mapping.agency_name;
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_jurisdiction: "governing federal",
        governing_body:         r.hit!.unit ? `${agency} (${r.hit!.unit})` : agency,
        governing_body_source:  "blm_sma_federal",
        governing_body_notes:   `Beach falls within federal land per BLM SMA (SMA_ID=${r.hit!.sma_id} = ${agency}${r.hit!.unit ? `, ${r.hit!.unit}` : ""}).`,
        review_status:          "ready",
        review_notes:           "Rescued to federal via BLM SMA service — caught gap in primary federal polygon.",
      }).eq("id", r.id);
    if (error) writeErrors.push(`id ${r.id}: ${error.message}`);
    else updated++;
  }

  let privateSkipped = 0;
  if (beachBillState) {
    privateSkipped = privateLand.length;
  } else {
    for (const r of privateLand) {
      const { error } = await supabase
        .from("beaches_staging_new")
        .update({
          review_status: "invalid",
          review_notes:  "Private land per BLM SMA (SMA_ID=2388).",
        }).eq("id", r.id);
      if (error) writeErrors.push(`id ${r.id}: ${error.message}`);
      else updated++;
    }
  }

  return json({
    state_code: stateCode,
    beach_bill_override: beachBillState,
    processed: stateFiltered.length,
    federal: federal.length, private: privateLand.length, other: other.length,
    updated, private_skipped: privateSkipped, errors: writeErrors,
  });
});
