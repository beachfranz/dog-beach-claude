// v2-default-county/index.ts
// Pipeline stage 9 — final classifier for records that none of the polygon
// classifiers (federal, state, city) matched.
//
// Two passes:
//   Pass A: neighbor inheritance. If any locked neighbor within 200m has a
//           ground-truth source (federal_polygon, state_polygon, city_polygon,
//           city_polygon_buffer), inherit their jurisdiction and governing body.
//
//   Pass B: default to governing county. Uses the county field populated by
//           geocode-context.
//
// Both passes set review_status = 'ready'. Pass A is authoritative; pass B
// is a reasonable default that human review may override.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const INHERIT_DISTANCE_M = 200;
const GROUND_TRUTH_SOURCES = [
  "federal_polygon",
  "state_polygon",
  "city_polygon",
  "city_polygon_buffer",
];

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Pass A: neighbor inheritance ────────────────────────────────────────────
  const { data: inheritHits, error: inheritErr } = await supabase
    .rpc("v2_find_neighbor_inheritance", {
      max_distance_m:     INHERIT_DISTANCE_M,
      trusted_sources:    GROUND_TRUTH_SOURCES,
    });
  if (inheritErr) return json({ error: inheritErr.message }, 500);

  // ── Pass B: default to county ───────────────────────────────────────────────
  const { data: defaultRows, error: defaultErr } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, county")
    .is("review_status", null);
  if (defaultErr) return json({ error: defaultErr.message }, 500);

  // Exclude rows that will be handled by Pass A
  const inheritIds = new Set((inheritHits ?? []).map((r: { u_id: number }) => r.u_id));
  const countyDefaults = (defaultRows ?? []).filter(r => !inheritIds.has(r.id) && r.county);

  if (body.dry_run) {
    return json({
      dry_run:           true,
      would_inherit:     inheritHits?.length ?? 0,
      would_default:     countyDefaults.length,
      no_county_field:   (defaultRows ?? []).filter(r => !inheritIds.has(r.id) && !r.county).length,
      inherit_preview:   (inheritHits ?? []).slice(0, 30),
      county_preview:    countyDefaults.slice(0, 30),
    });
  }

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

  let defaulted = 0;
  for (const r of countyDefaults) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_jurisdiction: "governing county",
        governing_body:         r.county,
        governing_body_source:  "county_default",
        governing_body_notes:   `No federal, state, or city polygon match. Defaulted to ${r.county}.`,
        review_status:          "ready",
        review_notes:           "Defaulted to county. May need human review.",
      })
      .eq("id", r.id);
    if (error) writeErrors.push(`default id ${r.id}: ${error.message}`);
    else defaulted++;
  }

  return json({
    inherited,
    defaulted,
    errors: writeErrors,
  });
});
