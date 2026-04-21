// load-nps-places/index.ts
// Fetches all NPS places in California from the NPS API and caches them in
// the nps_places table. Run once (or to refresh). Truncates and reloads.
//
// POST {}
// Returns { loaded, parks_fetched, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const NPS_API_KEY          = Deno.env.get("NPS_API_KEY")!;
const NPS_BASE             = "https://developer.nps.gov/api/v1";
const PAGE_SIZE            = 50;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Fetch parks map: parkCode → fullName ────────────────────────────────────
  const parksResp = await fetch(
    `${NPS_BASE}/parks?stateCode=CA&limit=100&api_key=${NPS_API_KEY}`
  );
  const parksData = await parksResp.json();
  const parkMap: Record<string, string> = {};
  for (const p of parksData.data ?? []) {
    parkMap[p.parkCode] = p.fullName;
  }

  // ── Paginate all CA places ──────────────────────────────────────────────────
  const rows: {
    id: string; title: string; latitude: number | null; longitude: number | null;
    park_code: string; park_full_name: string;
  }[] = [];

  let start = 0;
  let total = Infinity;

  while (start < total) {
    const resp = await fetch(
      `${NPS_BASE}/places?stateCode=CA&limit=${PAGE_SIZE}&start=${start}&api_key=${NPS_API_KEY}`
    );
    if (!resp.ok) break;
    const data = await resp.json();
    total = Number(data.total ?? 0);

    for (const p of data.data ?? []) {
      const parkCode = p.relatedParks?.[0]?.parkCode ?? "";
      rows.push({
        id:             p.id,
        title:          p.title,
        latitude:       p.latitude  ? parseFloat(p.latitude)  : null,
        longitude:      p.longitude ? parseFloat(p.longitude) : null,
        park_code:      parkCode,
        park_full_name: parkMap[parkCode] ?? p.relatedParks?.[0]?.fullName ?? parkCode,
      });
    }
    start += PAGE_SIZE;
  }

  if (!rows.length) return json({ error: "No NPS places returned" }, 500);

  // ── Truncate and reload ─────────────────────────────────────────────────────
  await supabase.from("nps_places").delete().neq("id", "");   // truncate

  const CHUNK = 200;
  const errors: string[] = [];
  for (let i = 0; i < rows.length; i += CHUNK) {
    const { error } = await supabase.from("nps_places").insert(rows.slice(i, i + CHUNK));
    if (error) errors.push(error.message);
  }

  return json({
    loaded:        rows.length,
    parks_fetched: Object.keys(parkMap).length,
    errors,
  });
});
