// v2-ccc-enrich/index.ts
// Phase 2 tier 1 — pulls the full set of California Coastal Commission
// Public Access Points amenity fields into our structured schema.
//
// v2-ccc-crossref already attached ccc_match_name/distance/dog_friendly
// for beaches with a CCC match within 200m. This function re-uses those
// matches and adds: parking, restrooms, showers, lifeguards, food,
// drinking water, fire pits, picnic area, disabled access.
//
// CCC fields use 'Yes' / 'No' / ' ' (blank) as values. We translate:
//   'Yes' → true
//   'No'  → false
//   ''    → null (unknown)
//
// Uses the existing ccc_match_name to find the corresponding CCC record —
// no re-running of the proximity match.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { getSource, getStateConfig, stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// CCC attribute → our column mapping
const AMENITY_FIELDS = [
  "DOG_FRIEND", "PARKING", "RESTROOMS", "SHOWERS", "LIFEGUARD",
  "FOOD", "DRINKWTR", "FIREPITS", "PCNC_AREA", "DSABLDACSS",
];

interface CccPoint {
  name:         string;
  lat:          number;
  lon:          number;
  dog:          string | null;
  parking:      string | null;
  restrooms:    string | null;
  showers:      string | null;
  lifeguard:    string | null;
  food:         string | null;
  drinkwtr:     string | null;
  firepits:     string | null;
  picnic:       string | null;
  disabled:     string | null;
}

function yn(v: string | null): boolean | null {
  if (!v) return null;
  const t = v.trim();
  if (t === "Yes") return true;
  if (t === "No")  return false;
  return null;
}

function dogYN(v: string | null): "yes" | "no" | "unknown" {
  if (!v) return "unknown";
  const t = v.trim();
  if (t === "Yes") return "yes";
  if (t === "No")  return "no";
  return "unknown";
}

async function loadCcc(url: string): Promise<Map<string, CccPoint>> {
  const params = new URLSearchParams({
    where:             "1=1",
    outFields:         `Name,LATITUDE,LONGITUDE,${AMENITY_FIELDS.join(",")}`,
    returnGeometry:    "false",
    f:                 "json",
    resultRecordCount: "5000",
  });
  const resp = await fetch(`${url}?${params}`);
  const data = await resp.json();
  const features = data?.features ?? [];
  const byName = new Map<string, CccPoint>();
  for (const f of features) {
    const a = f.attributes as Record<string, unknown>;
    const name = String(a.Name ?? "");
    if (!name) continue;
    byName.set(name, {
      name,
      lat:        Number(a.LATITUDE),
      lon:        Number(a.LONGITUDE),
      dog:        a.DOG_FRIEND ? String(a.DOG_FRIEND) : null,
      parking:    a.PARKING    ? String(a.PARKING)    : null,
      restrooms:  a.RESTROOMS  ? String(a.RESTROOMS)  : null,
      showers:    a.SHOWERS    ? String(a.SHOWERS)    : null,
      lifeguard:  a.LIFEGUARD  ? String(a.LIFEGUARD)  : null,
      food:       a.FOOD       ? String(a.FOOD)       : null,
      drinkwtr:   a.DRINKWTR   ? String(a.DRINKWTR)   : null,
      firepits:   a.FIREPITS   ? String(a.FIREPITS)   : null,
      picnic:     a.PCNC_AREA  ? String(a.PCNC_AREA)  : null,
      disabled:   a.DSABLDACSS ? String(a.DSABLDACSS) : null,
    });
  }
  return byName;
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

  const cfg = await getStateConfig(supabase, stateCode);
  if (!cfg?.has_coastal_access_source) {
    return json({ state_code: stateCode, skipped: true, reason: "state has no coastal_access_points source" });
  }

  const source = await getSource(supabase, "coastal_access_points", stateCode);
  if (!source) return json({ error: `No pipeline_sources row for coastal_access_points (state=${stateCode})` }, 500);

  const cccByName = await loadCcc(source.url);
  if (cccByName.size === 0) return json({ error: "CCC load returned no features" }, 500);

  // Only records that already have a CCC match (from v2-ccc-crossref)
  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, ccc_match_name, state")
    .not("ccc_match_name", "is", null);
  if (error) return json({ error: error.message }, 500);
  const filtered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (!filtered.length) return json({ state_code: stateCode, processed: 0, updated: 0 });

  const updates: Array<{
    id: number;
    fields: Record<string, unknown>;
  }> = [];

  const now = new Date().toISOString();

  for (const r of filtered) {
    const ccc = cccByName.get(r.ccc_match_name);
    if (!ccc) continue;

    const fields: Record<string, unknown> = {
      // Dog policy (high confidence when Yes/No; null stays null)
      dogs_allowed:          dogYN(ccc.dog),
      has_parking:           yn(ccc.parking),
      has_restrooms:         yn(ccc.restrooms),
      has_showers:           yn(ccc.showers),
      has_lifeguards:        yn(ccc.lifeguard),
      has_food:              yn(ccc.food),
      has_drinking_water:    yn(ccc.drinkwtr),
      has_fire_pits:         yn(ccc.firepits),
      has_picnic_area:       yn(ccc.picnic),
      has_disabled_access:   yn(ccc.disabled),
    };

    // Attach dog policy source only when the CCC record actually has a
    // value for DOG_FRIEND.
    if (ccc.dog && ccc.dog.trim()) {
      fields.dogs_policy_source     = "ccc";
      fields.dogs_policy_source_url = "https://www.coastal.ca.gov/access/";
      fields.dogs_policy_notes      = `CCC Public Access Point "${ccc.name}" lists dogs: ${ccc.dog}.`;
      fields.dogs_policy_updated_at = now;
    }

    fields.enrichment_source     = "ccc";
    fields.enrichment_updated_at = now;
    fields.enrichment_confidence = "high";

    updates.push({ id: r.id, fields });
  }

  if (body.dry_run) {
    return json({
      dry_run:     true,
      state_code:  stateCode,
      ccc_loaded:  cccByName.size,
      eligible:    filtered.length,
      would_write: updates.length,
      preview:     updates.slice(0, 5),
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const u of updates) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update(u.fields)
      .eq("id", u.id);
    if (error) writeErrors.push(`id ${u.id}: ${error.message}`);
    else updated++;
  }

  return json({
    state_code: stateCode,
    ccc_loaded: cccByName.size,
    eligible:   filtered.length,
    updated,
    errors:     writeErrors,
  });
});
