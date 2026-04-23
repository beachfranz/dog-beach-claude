// admin-load-noaa-stations/index.ts
// Fetches the NOAA CO-OPS tide-predictions station list and upserts
// CA stations (~192) into public.noaa_stations. One-shot admin-gated
// loader — idempotent, safe to re-run.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const NOAA_URL =
  "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=tidepredictions";

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { state?: string } = {};
  try { body = await req.json(); } catch { /* empty */ }
  const stateFilter = body.state ?? "CA";

  // ── Fetch full station list ─────────────────────────────────────────────
  let text: string;
  try {
    const resp = await fetch(NOAA_URL);
    if (!resp.ok) return json({ error: `NOAA HTTP ${resp.status}` }, 502);
    text = await resp.text();
  } catch (err) {
    return json({ error: `NOAA fetch failed: ${(err as Error).message}` }, 502);
  }

  const payload = JSON.parse(text);
  const allStations = payload?.stations ?? [];
  const caStations = allStations.filter((s: { state?: string }) => s.state === stateFilter);

  if (caStations.length === 0) {
    return json({ error: `No stations found for state=${stateFilter}` }, 404);
  }

  // ── Upsert in a single batch (small dataset, fits easily) ───────────────
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const { data, error } = await supabase.rpc("load_noaa_stations_batch", {
    p_stations: caStations,
  });
  if (error) return json({ error: `RPC failed: ${error.message}` }, 500);

  return json({
    state_filter: stateFilter,
    nationwide:   allStations.length,
    ...(data as Record<string, unknown>),
  });
});
