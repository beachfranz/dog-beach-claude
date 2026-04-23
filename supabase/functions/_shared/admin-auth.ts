// _shared/admin-auth.ts
// Gate for every admin-* edge function. Two layers:
//
//   1. Shared-secret check — the browser sends `x-admin-secret: <value>`,
//      we compare constant-time against the ADMIN_SECRET env var. This is
//      the only thing stopping an attacker who finds the public function
//      URLs (the repo is public, URLs are discoverable). Compare is
//      constant-time so the secret can't be brute-forced via timing.
//
//   2. Per-IP hourly rate limit — mirrors chat_rate_limits. Caps damage
//      from a compromised/brute-forced secret and bounds Anthropic/Google
//      costs from someone hammering admin-re-extract-policy.
//
// Usage in a handler (after CORS preflight):
//
//   const fail = await requireAdmin(req, corsHeaders);
//   if (fail) return fail;   // 401 / 429 / 500 already built
//
// Returns null on success, or a fully-formed Response on failure.

// deno-lint-ignore-file no-explicit-any
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ADMIN_SECRET         = Deno.env.get("ADMIN_SECRET") ?? "";

const RATE_LIMIT_PER_HOUR = 300;  // per IP. Plenty for real admin work,
                                  // blocks scraping / brute force.

function constantTimeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

function clientIp(req: Request): string {
  // Supabase proxies set x-forwarded-for. Take the left-most (original client).
  const xff = req.headers.get("x-forwarded-for") ?? "";
  return xff.split(",")[0]?.trim() || "unknown";
}

export async function requireAdmin(
  req: Request,
  cors: Record<string, string>,
): Promise<Response | null> {
  const json = (body: unknown, status: number) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (!ADMIN_SECRET) {
    // Fail closed when the env var isn't configured — we don't want the
    // functions running unauthenticated just because a secret is missing.
    return json({ error: "Admin auth not configured (ADMIN_SECRET missing)" }, 500);
  }

  const provided = req.headers.get("x-admin-secret") ?? "";
  if (!constantTimeEqual(provided, ADMIN_SECRET)) {
    return json({ error: "Unauthorized" }, 401);
  }

  // Secret OK → check + increment rate limit.
  const ip   = clientIp(req);
  const hour = new Date(Math.floor(Date.now() / 3_600_000) * 3_600_000).toISOString();
  const supabase: any = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const { data: count, error } = await supabase.rpc(
    "increment_admin_rate",
    { p_ip: ip, p_hour: hour },
  );
  if (error) {
    // Fail closed on rate-limit RPC errors — don't let auth succeed blind.
    return json({ error: `Rate limit check failed: ${error.message}` }, 500);
  }
  if ((count ?? 0) > RATE_LIMIT_PER_HOUR) {
    return json({ error: `Rate limit exceeded (${RATE_LIMIT_PER_HOUR}/hour per IP)` }, 429);
  }

  // Opportunistic cleanup of rows older than 24h (10% of requests).
  if (Math.random() < 0.1) {
    await supabase.from("admin_rate_limits")
      .delete()
      .lt("hour", new Date(Date.now() - 86_400_000).toISOString());
  }

  return null;
}
