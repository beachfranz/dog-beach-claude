// load-csp-places/index.ts
// Fetches all California State Parks park entry points from the CSP ArcGIS
// FeatureServer and caches them in the csp_places table. Truncates and reloads.
//
// No API key required — public ArcGIS service.
//
// POST {}
// Returns { loaded, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const CSP_URL =
  "https://services2.arcgis.com/AhxrK3F6WM8ECvDi/arcgis/rest/services/ParkEntryPoints/FeatureServer/2/query";

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  // ── Fetch all park entry points ─────────────────────────────────────────────
  const url = new URL(CSP_URL);
  url.searchParams.set("where", "1=1");
  url.searchParams.set("outFields", "PARK_NAME,County,STREET_ADDRESS,City,Zip,LAT,LON,UNIT_NBR,MGMT_STATUS");
  url.searchParams.set("resultRecordCount", "2000");
  url.searchParams.set("f", "json");

  let data: { features?: { attributes: Record<string, unknown> }[] };
  try {
    const resp = await fetch(url.toString());
    data = await resp.json();
  } catch (e) {
    return json({ error: `Fetch failed: ${e}` }, 500);
  }

  if (!data.features?.length) return json({ error: "No features returned from CSP ArcGIS" }, 500);

  const rows = data.features.map(f => ({
    park_name:      String(f.attributes.PARK_NAME  ?? ""),
    latitude:       f.attributes.LAT  ? Number(f.attributes.LAT)  : null,
    longitude:      f.attributes.LON  ? Number(f.attributes.LON)  : null,
    county:         f.attributes.County         ? String(f.attributes.County)         : null,
    street_address: f.attributes.STREET_ADDRESS ? String(f.attributes.STREET_ADDRESS) : null,
    city:           f.attributes.City           ? String(f.attributes.City)           : null,
    zip:            f.attributes.Zip            ? String(f.attributes.Zip)            : null,
    unit_nbr:       f.attributes.UNIT_NBR       ? String(f.attributes.UNIT_NBR)       : null,
    mgmt_status:    f.attributes.MGMT_STATUS    ? String(f.attributes.MGMT_STATUS)    : null,
  })).filter(r => r.park_name);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Truncate and reload ─────────────────────────────────────────────────────
  await supabase.from("csp_places").delete().neq("id", 0);

  const CHUNK = 200;
  const errors: string[] = [];
  for (let i = 0; i < rows.length; i += CHUNK) {
    const { error } = await supabase.from("csp_places").insert(rows.slice(i, i + CHUNK));
    if (error) errors.push(error.message);
  }

  return json({ loaded: rows.length, errors });
});
