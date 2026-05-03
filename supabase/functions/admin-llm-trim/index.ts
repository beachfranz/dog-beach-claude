// admin-llm-trim/index.ts
// Extract the relevant text from a verbose LLM response.
//
// The gold-set curator surfaces source variants that are often verbose
// ("Based on the webpage provided, dogs are allowed off-leash on this
// beach during the summer months. Note that the source also mentions
// parking is limited..."). The curator wants to pick a clean truth value
// quickly. This endpoint sends the verbose text + the field label to
// Haiku and asks it to extract just the answer.
//
// POST { text, field_name, field_label }
// Returns { trimmed: string }
//
// Cost: ~$0.001/call. Admin-gated (x-admin-secret + per-IP rate limit).

import Anthropic                from "https://esm.sh/@anthropic-ai/sdk@0.65.0";
import { corsHeaders }          from "../_shared/cors.ts";
import { requireAdmin }         from "../_shared/admin-auth.ts";

const ANTHROPIC_API_KEY = Deno.env.get("anthropic_api_key")!;

Deno.serve(async (req) => {
  const cors = corsHeaders(req, ["POST", "OPTIONS"]);
  if (req.method === "OPTIONS") return new Response(null, { headers: cors });

  const fail = await requireAdmin(req, cors);
  if (fail) return fail;

  let body: any;
  try { body = await req.json(); }
  catch { return new Response(JSON.stringify({ error: "invalid_json" }), { status: 400, headers: cors }); }

  const text       = String(body?.text ?? "").slice(0, 6000);
  const fieldLabel = String(body?.field_label ?? body?.field_name ?? "").slice(0, 200);
  if (!text || !fieldLabel) {
    return new Response(JSON.stringify({
      error: "missing_required_fields",
      required: ["text (string)", "field_label or field_name (string)"],
    }), { status: 400, headers: cors });
  }

  if (!ANTHROPIC_API_KEY) {
    return new Response(JSON.stringify({ error: "ANTHROPIC_API_KEY not configured" }),
      { status: 500, headers: cors });
  }

  const client = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

  const prompt = `The following is a verbose LLM response answering this question about a beach: "${fieldLabel}".

Extract ONLY the part that directly answers the question. Strip boilerplate ("Based on the source", "According to the webpage", "The page mentions that"). Strip any unrelated context. Do not paraphrase — keep the original wording for the relevant portion. If the response says it can't determine the answer or no information is available, return exactly: not specified

VERBOSE RESPONSE:
${text}

Return ONLY the relevant text. No preamble, no quotes wrapping the answer, no commentary.`;

  try {
    const resp = await client.messages.create({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 500,
      messages: [{ role: "user", content: prompt }],
    });
    // Anthropic SDK content is an array; first text block holds the answer
    const block = resp.content?.[0];
    const out = (block && "text" in block ? block.text : "").trim() || text;
    return new Response(JSON.stringify({
      trimmed: out,
      input_chars: text.length,
      output_chars: out.length,
      input_tokens:  resp.usage?.input_tokens  ?? 0,
      output_tokens: resp.usage?.output_tokens ?? 0,
    }), { headers: cors });
  } catch (e) {
    return new Response(JSON.stringify({ error: String((e as Error).message ?? e) }),
      { status: 500, headers: cors });
  }
});
