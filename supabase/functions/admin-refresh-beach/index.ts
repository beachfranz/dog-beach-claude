// admin-refresh-beach/index.ts
// Runs the full daily pipeline (weather + tides + crowds + scoring) for a
// single beach. Used by the admin editor when a just-saved beach has no
// rows in beach_day_recommendations yet — i.e., it was never scored.
//
// Security model: same as other admin-* functions. requireAdmin() gates
// the call. Internally we re-invoke the existing daily-beach-refresh
// endpoint with the service role key so we don't duplicate the pipeline
// logic or its dependencies (weather/tide/crowd clients, scoring engine).
//
// POST { location_id: string }
// Returns whatever daily-beach-refresh returns, plus { ok, status }.

import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
// daily-beach-refresh is deployed with --no-verify-jwt so the gateway
// doesn't care about the bearer format. Real auth lives inside the
// function via x-admin-secret below.
const ADMIN_SECRET         = Deno.env.get("ADMIN_SECRET")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { location_id?: string };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { location_id } = body;
  if (!location_id) return json({ error: "location_id required" }, 400);

  // Internal fetch of daily-beach-refresh with service role.
  // Timeout bound loosely — scoring a single beach including crowd fetch
  // is typically 5–15s; give it 60s ceiling before we give up.
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 60_000);

  let resp: Response;
  try {
    resp = await fetch(`${SUPABASE_URL}/functions/v1/daily-beach-refresh`, {
      method:  "POST",
      headers: {
        "Authorization":   `Bearer ${SUPABASE_SERVICE_KEY}`,
        "Content-Type":    "application/json",
        "x-admin-secret":  ADMIN_SECRET,
      },
      body:    JSON.stringify({ location_ids: [location_id] }),
      signal:  controller.signal,
    });
  } catch (err) {
    clearTimeout(timer);
    return json({ error: `daily-beach-refresh invocation failed: ${(err as Error).message}` }, 502);
  }
  clearTimeout(timer);

  let result: unknown;
  try { result = await resp.json(); }
  catch { result = { error: "non-JSON response" }; }

  return json({ ok: resp.ok, status: resp.status, result }, resp.ok ? 200 : 502);
});
