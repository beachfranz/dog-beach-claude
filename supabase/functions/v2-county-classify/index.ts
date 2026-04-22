// v2-county-classify/index.ts
// Pipeline stage 8b — CPAD-based classifier. Config-driven:
// source_key='cpad_polygon'. CA has the CA-specific CPAD (priority 10);
// other states fall back to the national PAD-US (priority 200).
//
// Priority when multiple CPAD hits exist: Federal > State > County.
// Non-govt levels (Non Profit / Special District / HOA) → invalid.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { buildArcgisQueryUrl, extractField, getSource, PipelineSource, stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const CONCURRENCY   = 10;
const DEFAULT_LIMIT = 5000;

interface CpadHit { agency_level: string; unit: string; agency: string; county: string; }

async function findCpadUnits(lat: number, lon: number, source: PipelineSource): Promise<CpadHit[]> {
  const url = buildArcgisQueryUrl(source, {
    geometry:       `${lon},${lat}`,
    geometryType:   "esriGeometryPoint",
    spatialRel:     "esriSpatialRelIntersects",
    inSR:           "4326",
    returnGeometry: "false",
  });
  try {
    const resp = await fetch(url);
    if (!resp.ok) return [];
    const data = await resp.json();
    const features = data?.features ?? [];
    return features.map((f: { attributes: Record<string, unknown> }) => {
      const a = f.attributes ?? {};
      return {
        agency_level: extractField(source, a, "agency_level") ?? "",
        unit:         extractField(source, a, "unit")         ?? "",
        agency:       extractField(source, a, "agency")       ?? "",
        county:       extractField(source, a, "county")       ?? "",
      };
    });
  } catch { return []; }
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

  let body: { state_code?: string; limit?: number; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const stateCode = body.state_code ?? "CA";
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const source = await getSource(supabase, "cpad_polygon", stateCode);
  if (!source) return json({ error: `No pipeline_sources row for cpad_polygon (state=${stateCode})` }, 500);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude, state")
    .is("review_status", null)
    .not("latitude", "is", null)
    .not("longitude", "is", null)
    .limit(body.limit ?? DEFAULT_LIMIT);
  if (error) return json({ error: error.message }, 500);
  const filtered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (!filtered.length) return json({ state_code: stateCode, processed: 0, matched: 0, updated: 0 });

  const tasks = filtered.map(r => async () => ({
    id: r.id, display_name: r.display_name,
    hits: await findCpadUnits(r.latitude, r.longitude, source),
  }));
  const results = await pLimit(tasks, CONCURRENCY);

  type Classification =
    | { kind: "federal"|"state"|"county"|"invalid"; id: number; display_name: string; hit: CpadHit };
  const classified: Classification[] = [];
  for (const r of results) {
    if (r.hits.length === 0) continue;
    const fed     = r.hits.find(h => h.agency_level === "Federal");
    const state   = r.hits.find(h => h.agency_level === "State");
    const county  = r.hits.find(h => h.agency_level === "County");
    const nonProf = r.hits.find(h => h.agency_level === "Non Profit");
    const special = r.hits.find(h => h.agency_level === "Special District");
    const hoa     = r.hits.find(h => h.agency_level === "Home Owners Association");

    if (fed)         classified.push({ kind: "federal", id: r.id, display_name: r.display_name, hit: fed });
    else if (state)  classified.push({ kind: "state",   id: r.id, display_name: r.display_name, hit: state });
    else if (county) classified.push({ kind: "county",  id: r.id, display_name: r.display_name, hit: county });
    else if (nonProf || special || hoa) {
      const hit = nonProf ?? special ?? hoa!;
      classified.push({ kind: "invalid", id: r.id, display_name: r.display_name, hit });
    }
  }

  const countByKind = (k: string) => classified.filter(c => c.kind === k).length;

  if (body.dry_run) {
    return json({
      dry_run: true, state_code: stateCode,
      processed: filtered.length,
      federal: countByKind("federal"), state: countByKind("state"),
      county: countByKind("county"),  invalid: countByKind("invalid"),
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const c of classified) {
    let update: Record<string, unknown>;
    if (c.kind === "county") {
      update = {
        governing_jurisdiction: "governing county",
        governing_body:         c.hit.agency || c.hit.unit,
        governing_body_source:  "county_polygon",
        governing_body_notes:   `Beach falls within ${c.hit.unit} (${c.hit.agency}) boundary polygon (CPAD/PAD-US).`,
        review_status:          "ready",
        review_notes:           "Confirmed county via CPAD county-agency polygon match.",
      };
    } else if (c.kind === "federal") {
      update = {
        governing_jurisdiction: "governing federal",
        governing_body:         c.hit.agency || c.hit.unit,
        governing_body_source:  "cpad_federal",
        governing_body_notes:   `Beach falls within ${c.hit.unit} (${c.hit.agency}) boundary polygon (CPAD/PAD-US).`,
        review_status:          "ready",
        review_notes:           "Confirmed federal via CPAD federal-agency polygon match (gap our primary federal polygon missed).",
      };
    } else if (c.kind === "state") {
      update = {
        governing_jurisdiction: "governing state",
        governing_body:         c.hit.agency || c.hit.unit,
        governing_body_source:  "cpad_state",
        governing_body_notes:   `Beach falls within ${c.hit.unit} (${c.hit.agency}) boundary polygon (CPAD/PAD-US).`,
        review_status:          "ready",
        review_notes:           "Confirmed state via CPAD state-agency polygon match (gap our primary state polygon missed).",
      };
    } else {
      update = {
        review_status: "invalid",
        review_notes:  `CPAD classifies this as ${c.hit.agency_level}: ${c.hit.unit} (${c.hit.agency}). Outside federal/state/county/city jurisdiction tiers — requires special handling.`,
      };
    }
    const { error } = await supabase.from("beaches_staging_new").update(update).eq("id", c.id);
    if (error) writeErrors.push(`id ${c.id}: ${error.message}`);
    else updated++;
  }

  return json({
    state_code: stateCode,
    processed: filtered.length,
    federal: countByKind("federal"), state: countByKind("state"),
    county: countByKind("county"),  invalid: countByKind("invalid"),
    updated, errors: writeErrors,
  });
});
