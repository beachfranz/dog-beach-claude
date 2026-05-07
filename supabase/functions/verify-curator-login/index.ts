// verify-curator-login/index.ts
//
// POST { username, password }
//   -> 200 { token: <ADMIN_SECRET> } on match
//   -> 401 { error: "Invalid credentials" } otherwise
//
// CURATOR_USERNAME / CURATOR_PASSWORD env vars hold the credentials.
// On success we return the existing ADMIN_SECRET as the session token —
// the curator's subsequent calls send `x-admin-secret` to other admin
// endpoints. This reuses the existing admin-auth machinery without
// adding a separate scope.
//
// Constant-time comparison to avoid timing attacks.

import { corsHeaders } from "../_shared/cors.ts";

const USERNAME     = Deno.env.get("CURATOR_USERNAME") ?? "";
const PASSWORD     = Deno.env.get("CURATOR_PASSWORD") ?? "";
const ADMIN_SECRET = Deno.env.get("ADMIN_SECRET")     ?? "";

function constantTimeEq(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")
    return new Response(JSON.stringify({ error: "POST only" }), { status: 405, headers: cors });

  if (!USERNAME || !PASSWORD || !ADMIN_SECRET) {
    return new Response(JSON.stringify({
      error: "Curator credentials not configured. Set CURATOR_USERNAME / CURATOR_PASSWORD / ADMIN_SECRET in env.",
    }), { status: 500, headers: cors });
  }

  let body: { username?: string; password?: string };
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers: cors });
  }

  const userOK = constantTimeEq(body.username || "", USERNAME);
  const passOK = constantTimeEq(body.password || "", PASSWORD);
  if (!userOK || !passOK) {
    // Brief delay to mitigate brute force
    await new Promise(r => setTimeout(r, 500));
    return new Response(JSON.stringify({ error: "Invalid credentials" }),
      { status: 401, headers: cors });
  }

  return new Response(JSON.stringify({ token: ADMIN_SECRET }),
    { status: 200, headers: cors });
});
