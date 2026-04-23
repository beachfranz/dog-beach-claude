// admin-load-us-states/index.ts
// One-shot loader for the `states` table. Pulls US state polygons from
// a GitHub-hosted GeoJSON (Census TIGERweb's REST endpoint is behind a
// WAF that rejects our fetches) and upserts them as PostGIS MultiPolygons.
// Used to back the coordinate-based state filter in load-beaches-staging.
//
// Security model: same as other admin-* functions (requireAdmin gate).
// Idempotent — safe to re-run.
//
// POST {}
// Returns { inserted, updated, total_features, fetch_chars, errors }

import { createClient }   from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }    from "../_shared/cors.ts";
import { requireAdmin }   from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// PublicaMundi's GeoJSON of US state boundaries. 52 features: 50 states +
// DC + Puerto Rico. `properties.name` has the full state name; no code —
// we map to the 2-letter USPS abbreviation via NAME_TO_CODE below.
const STATES_URL =
  "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json";

const NAME_TO_CODE: Record<string, string> = {
  "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
  "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
  "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
  "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
  "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
  "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
  "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
  "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
  "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
  "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Puerto Rico": "PR",
  "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
  "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
  "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
  "Wisconsin": "WI", "Wyoming": "WY",
};

interface StateFeature {
  type: "Feature";
  properties: { name: string };
  geometry:   unknown;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  // Fetch GeoJSON.
  let collection: { features: StateFeature[] };
  let fetchChars = 0;
  try {
    const resp = await fetch(STATES_URL);
    if (!resp.ok) return json({ error: `States GeoJSON HTTP ${resp.status}` }, 502);
    const text = await resp.text();
    fetchChars = text.length;
    collection = JSON.parse(text);
  } catch (err) {
    return json({ error: `States GeoJSON fetch failed: ${(err as Error).message}` }, 502);
  }

  if (!Array.isArray(collection?.features) || collection.features.length === 0) {
    return json({ error: "States GeoJSON response had no features" }, 502);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Upsert each feature via a raw SQL RPC-like path — PostgREST's upsert
  // doesn't let us call ST_GeomFromGeoJSON inline, so use supabase.rpc on
  // a helper function defined in the migration. Simpler: send a single
  // SQL statement via the "query" RPC. We don't have that here, so drop
  // down to inserts one at a time with a small SQL wrapper.
  let inserted = 0, updated = 0;
  const errors: string[] = [];

  for (const f of collection.features) {
    const name = f.properties?.name?.trim();
    const code = name ? NAME_TO_CODE[name] : undefined;
    if (!code || !name || !f.geometry) {
      errors.push(`Skipped feature with missing/unknown name: ${JSON.stringify(f.properties)}`);
      continue;
    }

    // ST_Multi(ST_GeomFromGeoJSON(...)) normalises Polygon → MultiPolygon
    // so the column type constraint is satisfied uniformly.
    const sql = `
      insert into public.states (state_code, state_name, geom)
      values ($1, $2, ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON($3), 4326)))
      on conflict (state_code) do update
        set state_name = excluded.state_name,
            geom      = excluded.geom,
            loaded_at = now()
      returning (xmax = 0) as was_insert;
    `;

    // supabase-js doesn't expose raw SQL. Use the "pg-meta" style: we'll
    // call a dedicated RPC. Define it inline via exec_sql if available.
    // Fallback: use the REST endpoint's pattern with rpc('exec', ...).
    const { data, error } = await supabase.rpc("load_state_feature", {
      p_state_code: code,
      p_state_name: name,
      p_geojson:    JSON.stringify(f.geometry),
    });
    if (error) {
      errors.push(`${code}: ${error.message}`);
      continue;
    }
    if (data === true) inserted++;
    else               updated++;
  }

  return json({
    inserted,
    updated,
    total_features: collection.features.length,
    fetch_chars:    fetchChars,
    errors,
  });
});
