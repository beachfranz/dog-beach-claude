// dispatch-github-workflow/index.ts
//
// Generic GitHub Actions dispatcher for orch_jobs. Lets the catalog point
// at heavy jobs that need real Python (e.g., monthly_dog_park_coverage with
// Playwright + Anthropic LLM calls + psycopg2) without provisioning a VM.
//
// orch_jobs catalog row pattern:
//   worker_kind    = 'edge_function'
//   worker_target  = 'dispatch-github-workflow'
//   worker_payload = { "workflow": "dp_coverage.yml",
//                      "ref": "main",
//                      "inputs": { "states": "CA,OR" } }
//
// The actual heavy work runs in GitHub Actions ubuntu-latest, which has
// up to 6h job runtime, ~14 GB RAM, 4 vCPUs, and ~14 GB SSD.
//
// Auth: GH_DISPATCH_TOKEN env var must be set in Supabase Edge Function
// secrets. PAT or fine-grained token with `actions:write` scope on this
// repo only.
//
// Repo: hardcoded to beachfranz/dog-beach-claude. If you fork, update
// GH_OWNER and GH_REPO below.

// deno-lint-ignore-file no-explicit-any
import { corsHeaders } from "../_shared/cors.ts";

const GH_OWNER = "beachfranz";
const GH_REPO  = "dog-beach-claude";

const ADMIN_SECRET     = Deno.env.get("ADMIN_SECRET") ?? "";
const GH_DISPATCH_TOKEN = Deno.env.get("GH_DISPATCH_TOKEN") ?? "";

Deno.serve(async (req: Request): Promise<Response> => {
  const headers = corsHeaders(req, "POST, OPTIONS");

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers });
  }

  // Admin-secret gate (orch_dispatch passes this header)
  const provided = req.headers.get("x-admin-secret") ?? "";
  if (!ADMIN_SECRET || provided !== ADMIN_SECRET) {
    return new Response(
      JSON.stringify({ error: "unauthorized" }),
      { status: 401, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  if (!GH_DISPATCH_TOKEN) {
    return new Response(
      JSON.stringify({ error: "GH_DISPATCH_TOKEN not configured" }),
      { status: 500, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  let body: any = {};
  try {
    body = await req.json();
  } catch (_e) {
    body = {};
  }

  const workflow: string = body.workflow ?? "";
  const ref: string      = body.ref ?? "main";
  const inputs: Record<string, string> = body.inputs ?? {};

  if (!workflow || !workflow.endsWith(".yml")) {
    return new Response(
      JSON.stringify({ error: "missing or invalid 'workflow' field (expect a .yml filename)" }),
      { status: 400, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  // GitHub workflow_dispatch API
  // https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event
  // Inputs values must be strings per the API contract; coerce.
  const stringInputs: Record<string, string> = {};
  for (const [k, v] of Object.entries(inputs)) {
    stringInputs[k] = typeof v === "string" ? v : JSON.stringify(v);
  }

  const apiUrl = `https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/actions/workflows/${workflow}/dispatches`;
  const t0 = Date.now();
  let ghResp: Response;
  try {
    ghResp = await fetch(apiUrl, {
      method: "POST",
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${GH_DISPATCH_TOKEN}`,
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
        "User-Agent": "dog-beach-orch",
      },
      body: JSON.stringify({ ref, inputs: stringInputs }),
      signal: AbortSignal.timeout(20_000),
    });
  } catch (e) {
    return new Response(
      JSON.stringify({ error: "fetch_failed", detail: (e as Error).message }),
      { status: 502, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  const elapsedMs = Date.now() - t0;

  // GitHub returns 204 No Content on success
  if (ghResp.status === 204) {
    return new Response(
      JSON.stringify({
        dispatched: true,
        workflow, ref, inputs: stringInputs,
        elapsed_ms: elapsedMs,
      }),
      { status: 200, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  const errText = await ghResp.text();
  return new Response(
    JSON.stringify({
      dispatched: false,
      github_status: ghResp.status,
      github_body: errText.slice(0, 500),
      workflow, ref, inputs: stringInputs,
      elapsed_ms: elapsedMs,
    }),
    { status: 502, headers: { ...headers, "Content-Type": "application/json" } },
  );
});
