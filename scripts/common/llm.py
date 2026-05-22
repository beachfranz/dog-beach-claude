"""Canonical Anthropic helpers — model registry + SDK client + urllib caller.

Replaces 21 scattered `anthropic.Anthropic()` constructions with drifting
model strings identified by the 2026-05-22 audit. Today's strings live
across files as `claude-sonnet-4-5-20250929`, `claude-sonnet-4-6`,
`claude-haiku-4-5-20251001` — no script on Sonnet 4.7 or Opus 4.7. One
model bump here migrates the codebase.

Two integration styles supported:
  - SDK: `from scripts.common.llm import sdk_client; c = sdk_client()`
  - urllib (no SDK dep): `from scripts.common.llm import call; r = call(prompt, user_msg)`

Most pipeline scripts use urllib directly (see extract_temporal_from_policy_source,
generate_beach_descriptions). The `call()` helper centralizes the ~30 LOC
of identical HTTP boilerplate they each carry.
"""
from __future__ import annotations

import json
import os
import urllib.request
from typing import Any

# ─── Model registry ──────────────────────────────────────────────────
#
# Update when bumping. Every consumer that imports SONNET/HAIKU/OPUS
# moves with one edit.

SONNET = "claude-sonnet-4-7"             # canonical Sonnet (Sonnet 4.7)
HAIKU  = "claude-haiku-4-5-20251001"     # canonical Haiku
OPUS   = "claude-opus-4-7"               # canonical Opus

# ─── SDK client (lazy) ───────────────────────────────────────────────

_sdk_client = None


def sdk_client():
    """Get a process-wide cached Anthropic SDK client.

    Lazy import so scripts that only use `call()` don't pull the SDK.
    """
    global _sdk_client
    if _sdk_client is None:
        from anthropic import Anthropic
        _sdk_client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _sdk_client


# ─── urllib-based call (no SDK dep) ──────────────────────────────────

ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"


def call(
    system_prompt: str,
    user_message: str | dict,
    *,
    model: str = SONNET,
    max_tokens: int = 2048,
    cache_system: bool = True,
    timeout: int = 120,
) -> dict[str, Any]:
    """Call Anthropic with a system prompt + user message via urllib.

    Returns a parsed dict with keys:
      - text: assistant's text content (joined)
      - input_tokens / output_tokens / cache_read_input_tokens: usage
      - raw: full response JSON

    `cache_system=True` (default) adds ephemeral cache_control on the
    system block — saves cost when iterating on the same prompt.

    `user_message` can be a string OR a dict (JSON-serialized for you).
    """
    system_block = [{"type": "text", "text": system_prompt}]
    if cache_system:
        system_block[0]["cache_control"] = {"type": "ephemeral"}

    if isinstance(user_message, (dict, list)):
        user_content = json.dumps(user_message, ensure_ascii=False)
    else:
        user_content = user_message

    body = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system_block,
        "messages": [{"role": "user", "content": user_content}],
    }
    req = urllib.request.Request(
        ANTHROPIC_API,
        data=json.dumps(body).encode("utf-8"),
        method="POST",
    )
    req.add_header("x-api-key", os.environ["ANTHROPIC_API_KEY"])
    req.add_header("anthropic-version", ANTHROPIC_VERSION)
    req.add_header("content-type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.loads(r.read().decode("utf-8"))
    text = "".join(b.get("text", "") for b in resp.get("content", []))
    usage = resp.get("usage", {})
    return {
        "text": text,
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "cache_read_input_tokens": usage.get("cache_read_input_tokens", 0),
        "raw": resp,
    }


# ─── JSON-output helper ──────────────────────────────────────────────

def call_json(
    system_prompt: str,
    user_message: str | dict,
    **kwargs,
) -> dict[str, Any]:
    """Like `call()` but parses the assistant's text as JSON.

    Tolerates markdown code fences (```json ... ```), strips them
    transparently. Raises ValueError on non-JSON response.

    Returns the call() dict with one extra key: `parsed` (the parsed JSON).
    """
    r = call(system_prompt, user_message, **kwargs)
    text = r["text"].strip()
    if text.startswith("```"):
        parts = text.split("```", 2)
        if len(parts) >= 2:
            inner = parts[1]
            if inner.lower().startswith("json"):
                inner = inner[4:].lstrip()
            text = inner.rsplit("```", 1)[0].strip()
    try:
        r["parsed"] = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM returned non-JSON: {r['text'][:400]!r}") from e
    return r
