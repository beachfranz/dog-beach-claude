"""
bland_call_beach.py
-------------------
Place a single outbound voice call via Bland.ai to a city/agency
contact and ask about a specific beach's dog policy. Returns a
structured answer matching the geo_entity_response.leash_policy shape.

This is a template — sign up at https://bland.ai, get an API key,
fund the account ($10 minimum), and set BLAND_API_KEY in env.

Cost: ~$0.20-$0.40 per 2-3 minute call.

Compliance:
  - Identifies as an AI assistant in the opening line (CA SB 1001).
  - Calls government offices only — never consumers (FCC TCPA rule).
  - Polite, concise, easily redirectable to a human if the staffer
    doesn't have answers.

Usage:
  export BLAND_API_KEY=org_xxxxxxxxxx
  python scripts/one_off/bland_call_beach.py --dry-run \\
    --beach "Huntington Dog Beach" \\
    --phone "+17145364119" \\
    --agency "Huntington Beach Parks & Rec"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

import httpx

BLAND_API_KEY = os.environ.get("BLAND_API_KEY", "")
BLAND_BASE    = "https://api.bland.ai/v1"

# Voice script. Designed for ~90-120 seconds of conversation.
TASK_TEMPLATE = """\
You are an AI research assistant calling on behalf of Dog Beach Scout, \
a free website that helps dog owners find dog-friendly beaches in California.

You are calling {agency} to ask about dog policy at {beach}.

Opening line (REQUIRED — speak this verbatim before any other content):
  "Hi, this is an AI assistant calling on behalf of Dog Beach Scout, a free \
  website for dog owners. I have three quick questions about {beach}, do you \
  have a moment?"

If the person says they're busy or it's not a good time:
  Apologize, ask if there's a better time or a department that handles beach \
  policy questions, thank them, and end the call politely.

If they agree to talk, ask these THREE questions in order, one at a time, \
waiting for an answer before moving on:

  1. "Are dogs allowed at {beach}?"
  2. "If dogs are allowed, do they need to be on a leash, and is there a \
     specific leash length required?"
  3. "Are there any times of day, days of the week, or seasons when dogs \
     are not allowed, or when leash rules change?"

If you don't understand the answer, ask one polite follow-up. Don't argue.

If they say "I don't know" or transfer you, accept gracefully and either \
ask the next person or end the call.

End with: "Thank you so much for your time. Have a great day."

Important rules:
  - Never claim you're a human.
  - Never ask for the staffer's personal opinion — only official policy.
  - Don't argue or push back if they say something that contradicts \
    other sources.
  - If the person sounds annoyed at any point, apologize and end the call.
"""

# Structured output schema — Bland will run this against the transcript
# after the call completes and return the parsed values in the result.
ANALYSIS_PROMPT = """\
Extract the following from the call transcript:

  dogs_allowed:        "yes" | "no" | "restricted" | "seasonal" | "unknown"
  leash_required:      "always" | "sometimes" | "never" | "unknown"
  leash_length_ft:     number | null
  daily_restrictions:  string describing any time-of-day rules, or null
  seasonal_restrictions: string describing any season/date-bounded rules, or null
  staffer_unsure:      true | false   (true if the staffer didn't know or transferred)
  call_completed:      true | false   (true if all three questions were answered)
  notes:               string (one short clause summarizing anything unusual)

Use "unknown" / null when the call didn't yield a clear answer.
"""


def make_call(beach: str, phone: str, agency: str, dry_run: bool) -> dict[str, Any]:
    """Trigger a single outbound call. Returns the API response (incl. call_id)."""
    payload = {
        "phone_number":   phone,
        "task":           TASK_TEMPLATE.format(beach=beach, agency=agency),
        "voice":          "maya",          # natural female voice; pick from Bland catalog
        "model":          "enhanced",      # better instruction-following
        "language":       "en",
        "max_duration":   5,               # minutes — hard cap
        "wait_for_greeting":   True,       # let staffer say "Hello, parks dept"
        "answered_by_enabled": True,       # detect voicemail vs human
        "voicemail_message":   None,       # don't leave voicemail; just hang up
        "record":         True,            # keep audio for QA
        "summary_prompt": ANALYSIS_PROMPT,
        "metadata": {                      # echoed back in webhook payload
            "beach":   beach,
            "agency":  agency,
        },
    }

    if dry_run:
        print("=== DRY RUN — would POST to /v1/calls ===")
        print(json.dumps(payload, indent=2))
        return {"dry_run": True}

    if not BLAND_API_KEY:
        print("BLAND_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    r = httpx.post(
        f"{BLAND_BASE}/calls",
        headers={"Authorization": BLAND_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=30.0,
    )
    r.raise_for_status()
    return r.json()


def poll_call(call_id: str, timeout_s: int = 600) -> dict[str, Any]:
    """Poll until the call completes (or timeout). Returns full call detail
    including transcript + summary_prompt parsed result."""
    start = time.time()
    while True:
        if time.time() - start > timeout_s:
            raise TimeoutError(f"call {call_id} did not complete in {timeout_s}s")
        r = httpx.get(
            f"{BLAND_BASE}/calls/{call_id}",
            headers={"Authorization": BLAND_API_KEY},
            timeout=15.0,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("completed"):
            return data
        time.sleep(8)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--beach",   required=True, help='e.g. "Huntington Dog Beach"')
    p.add_argument("--phone",   required=True, help="E.164 format, e.g. +17145364119")
    p.add_argument("--agency",  required=True, help='e.g. "Huntington Beach Parks & Rec"')
    p.add_argument("--dry-run", action="store_true",
                   help="Print the request payload without placing the call")
    p.add_argument("--no-poll", action="store_true",
                   help="Return immediately after triggering, don't poll for result")
    args = p.parse_args()

    response = make_call(args.beach, args.phone, args.agency, args.dry_run)

    if args.dry_run:
        return 0

    print(json.dumps(response, indent=2))
    call_id = response.get("call_id")
    if not call_id or args.no_poll:
        return 0

    print(f"\nPolling call {call_id}…")
    result = poll_call(call_id)
    print("\n=== TRANSCRIPT ===")
    for turn in result.get("transcripts", []):
        speaker = turn.get("user", "agent")
        print(f"  [{speaker}] {turn.get('text', '')}")
    print("\n=== STRUCTURED ANSWER ===")
    print(json.dumps(result.get("summary"), indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
