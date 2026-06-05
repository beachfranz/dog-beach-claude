---
name: edge-function
description: Use this skill when creating, modifying, or deploying a Supabase Edge Function in this repo (anything under supabase/functions/). Triggers include adding a new consumer read (e.g. get-*), a new admin tool (admin-*), a new catalog ingest step (v2-*), or modifying CORS, auth, or deploy commands for an existing function. Do NOT use for non-edge-function code (Python scripts, SQL migrations, frontend HTML/JS).
---

# Edge Function Authoring

Three flavors live in `supabase/functions/`. Pick the right template, then walk the checklist.

## Step 1 — Pick the flavor and find a template

| Flavor | Auth | Where to find a template |
|---|---|---|
| Consumer read (browser → API) | Anon key (`sb_publishable_...`) | `ls supabase/functions/get-*` — pick the one closest in shape |
| Admin tool (curator only) | `x-admin-secret` header via `requireAdmin()` | `ls supabase/functions/admin-*` — pick the one closest in shape |
| Catalog ingest step | Service role (server-to-server) | `ls supabase/functions/v2-*` — pick the one closest in shape |
| Writer cron (daily-refresh-style) | `x-admin-secret` AND service role | `daily-beach-refresh/` (canonical) or `get-beach-now/` (lighter) |

Copy the closest match into a new directory under `supabase/functions/<new-name>/` and modify. **Do not start from scratch** — the boilerplate is too easy to get subtly wrong.

If unsure which existing function is closest, ask Franz before picking — a wrong template costs more than a clarifying question.

## Step 2 — Wire the shared imports

Every function imports from `_shared/`. At minimum:

```ts
import { corsHeaders } from "../_shared/cors.ts";
```

Plus, depending on flavor:
- Admin / writer: `import { requireAdmin } from "../_shared/admin-auth.ts";`
- Anything that scores: `import { scoreHours } from "../_shared/scoring.ts";`
- Anything reading config: `import { loadConfig } from "../_shared/config.ts";`

If you find yourself reimplementing CORS, auth, or scoring inline, stop — pull from `_shared/` instead.

## Step 3 — CORS (every response, including OPTIONS)

`_shared/cors.ts` enforces the origin allowlist (`https://beachfranz.github.io` + `null` for file://). Every function follows this shape:

```ts
Deno.serve(async (req) => {
  const headers = corsHeaders(req, ["GET", "POST", "OPTIONS"]);

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers });
  }

  // ... handler logic ...

  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { ...headers, "Content-Type": "application/json" },
  });
});
```

**The OPTIONS preflight must return the same `headers`.** Forgetting this is the #1 cause of "works in curl, broken in browser."

## Step 4 — Auth gating

- **Consumer read**: no gating in the function itself. RLS on `beaches_gold` / `beach_dog_policy` / `beach_day_*` is the security layer.
- **Admin / writer**: first line of the handler:
  ```ts
  const authError = await requireAdmin(req);
  if (authError) return authError;
  ```
  Returns a 401/403 with CORS headers already applied. Do not reimplement.
- **Service role inside function**: use `Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")` — never expose this to the client.

The repo is public. Obscure URLs are not security. Always gate writers.

## Step 5 — Deploy

```bash
supabase functions deploy <fn-name> --no-verify-jwt
```

`--no-verify-jwt` is required because the `sb_publishable_` anon key format does not pass Supabase JWT verification. **Always include this flag.** A deploy without it appears to succeed but the function will reject browser calls.

## Step 6 — Pre-flight checklist

Before considering the function done, verify:

- [ ] Copied from a real template, not generated from scratch
- [ ] Imports `corsHeaders` from `../_shared/cors.ts`
- [ ] OPTIONS branch returns 204 with `headers`
- [ ] Every other response includes `...headers`
- [ ] Auth gate matches flavor (none / `requireAdmin` / service role)
- [ ] No service role key in any frontend code path
- [ ] Deployed with `--no-verify-jwt`
- [ ] Tested from `file://` (local HTML) AND `https://beachfranz.github.io` if consumer-facing
- [ ] If it's a writer, it accepts `{ location_ids: string[] }` body when a scoped run makes sense (see `daily-beach-refresh` for the pattern)
- [ ] If it touches scoring, it imports from `_shared/scoring.ts` — does NOT reimplement thresholds

## Common gotchas

- **`fid` vs `location_id`**: consumer endpoints accept both (`?fid=` preferred, `?location_id=` fallback). Templates handle this; preserve the pattern.
- **`is_scoreable` gate**: writers that fan out over beaches must filter `WHERE is_scoreable=true` or you'll explode from ~309 to 764 calls.
- **`arena_group_id` is the scoring PK**: post-path-3b, `beach_day_hourly_scores` and `beach_day_recommendations` key on `arena_group_id`, not `location_id`. Joins against `beaches_gold.fid` go through `arena_group_id`.
- **Rate limits**: `beach-chat` uses `increment_chat_rate` RPC (20/IP/hour). Any new public POST endpoint that calls Anthropic should follow the same pattern.
- **`admin_audit` + `admin_rate_limits`**: admin endpoints log to these. `requireAdmin` handles audit; rate-limit logic is per-function if needed.

## When the template doesn't fit

If none of the four flavors match (rare — usually you're missing context, not facing a genuinely new shape), stop and surface the mismatch to Franz before inventing a fifth pattern. New flavors should be deliberate, not accidental.
