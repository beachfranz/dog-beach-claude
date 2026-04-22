// v2-default-county/index.ts
// Final classifier for records that escaped every polygon stage.
//
// Config-driven:
//   - state_code filters staging rows
//   - state_config.coastal_default_tier decides whether the residual default
//     is 'county' (CA behavior) or 'state' (Oregon Beach Bill — Ocean Shore
//     is OPRD-managed statewide)
//   - state_config.coastal_default_body names the fallback state authority
//     (e.g., "Oregon Parks and Recreation Department (Ocean Shore)")
//
// Two passes:
//   Pass A: neighbor inheritance — if an unlocked record is within 200m of a
//           locked neighbor with a ground-truth source, inherit its juris.
//   Pass B: residual default. For coastal_default_tier='county': use the
//           county geocode. For 'state': use coastal_default_body.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { getStateConfig, stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const INHERIT_DISTANCE_M = 200;
const GROUND_TRUTH_SOURCES = [
  "federal_polygon", "state_polygon", "city_polygon", "city_polygon_buffer",
];

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state_code?: string; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const stateCode = body.state_code ?? "CA";
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const cfg = await getStateConfig(supabase, stateCode);
  if (!cfg) return json({ error: `No state_config row for ${stateCode}` }, 500);

  // Pass A — neighbor inheritance (state-agnostic RPC, filter results)
  const { data: inheritHits, error: inheritErr } = await supabase
    .rpc("v2_find_neighbor_inheritance", {
      max_distance_m:  INHERIT_DISTANCE_M,
      trusted_sources: GROUND_TRUTH_SOURCES,
    });
  if (inheritErr) return json({ error: inheritErr.message }, 500);

  // Pass B — residual default candidates
  const { data: defaultRows, error: defaultErr } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, county, state")
    .is("review_status", null);
  if (defaultErr) return json({ error: defaultErr.message }, 500);
  const stateFiltered = (defaultRows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);

  const inheritIds = new Set((inheritHits ?? []).map((r: { u_id: number }) => r.u_id));
  const defaults   = stateFiltered.filter(r => !inheritIds.has(r.id));

  if (body.dry_run) {
    return json({
      dry_run:              true,
      state_code:           stateCode,
      coastal_default_tier: cfg.coastal_default_tier,
      coastal_default_body: cfg.coastal_default_body,
      would_inherit:        inheritHits?.length ?? 0,
      would_default:        defaults.length,
      inherit_preview:      (inheritHits ?? []).slice(0, 30),
      default_preview:      defaults.slice(0, 30),
    });
  }

  // Apply Pass A
  let inherited = 0;
  const writeErrors: string[] = [];
  for (const h of inheritHits ?? []) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_jurisdiction: h.l_juris,
        governing_body:         h.l_body,
        governing_body_source:  "neighbor_inherit",
        governing_body_notes:   `Inherited from id=${h.l_id} ("${h.l_name}") at ${Math.round(h.dist_m)}m via ${h.l_source}.`,
        review_status:          "ready",
        review_notes:           `Inherited jurisdiction from nearby locked neighbor (${h.l_source}).`,
      })
      .eq("id", h.u_id);
    if (error) writeErrors.push(`inherit id ${h.u_id}: ${error.message}`);
    else inherited++;
  }

  // Apply Pass B — residual default based on state's coastal_default_tier
  let defaulted = 0;
  for (const r of defaults) {
    let update: Record<string, unknown>;
    if (cfg.coastal_default_tier === "state" && cfg.coastal_default_body) {
      update = {
        governing_jurisdiction: "governing state",
        governing_body:         cfg.coastal_default_body,
        governing_body_source:  "state_default",
        governing_body_notes:   `No federal, county, city polygon match. Defaulted to state (${cfg.state_name} coastal_default_tier='state').`,
        review_status:          "ready",
        review_notes:           `Defaulted to state per ${cfg.state_name} coastal-default rules.`,
      };
    } else if (r.county) {
      update = {
        governing_jurisdiction: "governing county",
        governing_body:         r.county,
        governing_body_source:  "county_default",
        governing_body_notes:   `No federal, state, or city polygon match. Defaulted to ${r.county}.`,
        review_status:          "ready",
        review_notes:           "Defaulted to county. May need human review.",
      };
    } else {
      continue; // no county, no state default — leave for review
    }
    const { error } = await supabase
      .from("beaches_staging_new").update(update).eq("id", r.id);
    if (error) writeErrors.push(`default id ${r.id}: ${error.message}`);
    else defaulted++;
  }

  return json({
    state_code:           stateCode,
    coastal_default_tier: cfg.coastal_default_tier,
    inherited,
    defaulted,
    errors:               writeErrors,
  });
});
