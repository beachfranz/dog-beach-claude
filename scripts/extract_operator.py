"""
extract_operator.py — LLM signal for the day-to-day operator of a beach.

Standard pattern: BS4-strip a source page, send to Sonnet 4.6 with a
closed-shape prompt (city/county/state/federal/private/...), parse the
JSON, optionally write to public.beach_policy_extractions.

Usage:
  python scripts/extract_operator.py --fid 9716 --url <URL>            # preview
  python scripts/extract_operator.py --fid 9716 --url <URL> --apply    # write
  python scripts/extract_operator.py --fid 9716 --url <URL> --register # also insert variant row

The first run with --register inserts an extraction_prompt_variants row
named (operator, llm_v1) so future bulk runs can pick it up via the
existing extract_for_orphans flow.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import psycopg2
import psycopg2.extras
from anthropic import Anthropic
from bs4 import BeautifulSoup
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
MAX_CONTENT_CHARS = 12000

PROMPT = """Identify the entity that operates this beach (the agency or organization \
responsible for day-to-day management — not the owner if they differ, and not \
the broader park district unless that IS the operator).

Output ONLY a JSON object of this exact shape (no prose, no markdown):

{
  "operator": {
    "name":       "<full name as stated, e.g. 'City of San Diego', 'California Department of Parks and Recreation', 'Marin Municipal Water District'>",
    "level":      "<one of: city | county | state | federal | private | special_district | tribal | unclear>",
    "evidence":   "<verbatim quote from the source supporting this, or null>",
    "confidence": "<high | medium | low>"
  }
}

Be conservative:
- 'high' confidence requires an official agency page or an explicit statement of operator.
- 'medium' for indirect signals (e.g., the page is on the agency's domain).
- 'low' for tourism listings, blogs, or weak inference.
- 'unclear' level when the source doesn't clearly say."""


def fetch_html(url: str, timeout: int = 20) -> str | None:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "html" not in ct.lower():
                return None
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except Exception as e:
        print(f"fetch failed: {e}", file=sys.stderr)
        return None


def bs4_strip(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()
    text = (soup.body or soup).get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    return "\n".join(lines)[:MAX_CONTENT_CHARS]


def call_llm(client: Anthropic, page: str) -> dict:
    t0 = time.time()
    resp = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=600,
        system="You are extracting a single structured fact from beach webpages. "
               "Always respond with the JSON shape requested, never prose.",
        messages=[{
            "role": "user",
            "content": [
                {"type": "text", "text": f"Beach webpage content:\n\n{page}",
                 "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": PROMPT},
            ],
        }],
    )
    raw = resp.content[0].text.strip() if resp.content else ""
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return {
        "raw":           raw,
        "input_tokens":  resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
        "latency_ms":    int((time.time() - t0) * 1000),
    }


def register_variant(cur) -> int:
    """Insert (or update) the extraction_prompt_variants row."""
    cur.execute("""
        INSERT INTO public.extraction_prompt_variants
            (field_name, variant_key, prompt_template, expected_shape,
             target_model, active, is_canon, notes)
        VALUES
            ('operator', 'llm_v1', %s, 'structured_json',
             'claude-sonnet-4-6', true, true,
             'Day-to-day operator extraction. Closed level enum: city/county/state/federal/private/special_district/tribal/unclear. Added 2026-05-02.')
        ON CONFLICT (field_name, variant_key) DO UPDATE SET
            prompt_template = EXCLUDED.prompt_template,
            active          = true,
            is_canon        = true
        RETURNING id;
    """, (PROMPT,))
    return cur.fetchone()[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fid", type=int, required=True, help="arena_group_id")
    ap.add_argument("--url", required=True)
    ap.add_argument("--apply",    action="store_true",
                    help="Write to beach_policy_extractions (default: preview)")
    ap.add_argument("--register", action="store_true",
                    help="Insert/update the operator variant row first")
    args = ap.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        sys.exit("ANTHROPIC_API_KEY missing")
    client = Anthropic(api_key=api_key)

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor()

    variant_id = None
    if args.register:
        variant_id = register_variant(cur)
        conn.commit()
        print(f"  [variant] operator/llm_v1 → id={variant_id}")
    else:
        cur.execute("""
            SELECT id FROM public.extraction_prompt_variants
             WHERE field_name='operator' AND variant_key='llm_v1' AND active=true
             LIMIT 1
        """)
        row = cur.fetchone()
        variant_id = row[0] if row else None

    print(f"  Fetching {args.url} ...")
    html = fetch_html(args.url)
    if not html:
        sys.exit("fetch failed")
    page = bs4_strip(html)
    print(f"  page content: {len(page)} chars")

    print(f"  Calling Sonnet 4.6 ...")
    resp = call_llm(client, page)
    raw = resp["raw"]

    # Parse JSON
    try:
        obj = json.loads(raw)
        op = obj.get("operator", {})
        print()
        print("  ── Extracted operator ─────────────────────────")
        print(f"    name:       {op.get('name')}")
        print(f"    level:      {op.get('level')}")
        print(f"    confidence: {op.get('confidence')}")
        ev = op.get("evidence") or ""
        print(f"    evidence:   {ev[:200]}")
        print()
        print(f"  tokens: in={resp['input_tokens']} out={resp['output_tokens']} latency={resp['latency_ms']}ms")
        parse_ok = True
    except Exception as e:
        print(f"  PARSE FAILED: {e}")
        print(f"  raw: {raw[:400]}")
        parse_ok = False
        obj = {}

    if not args.apply:
        print()
        print("  (preview only; rerun with --apply to insert into beach_policy_extractions)")
        return 0

    if not variant_id:
        sys.exit("ERROR: no active operator variant found; rerun with --register first.")

    # Insert into beach_policy_extractions. Need a source_id from city_policy_sources.
    cur.execute("""
        SELECT id FROM public.city_policy_sources WHERE url = %s LIMIT 1
    """, (args.url,))
    row = cur.fetchone()
    if row:
        source_id = row[0]
    else:
        cur.execute("""
            INSERT INTO public.city_policy_sources
                (place_fips, source_type, url, title, notes, curated_by)
            VALUES ('06ORPH', 'other', %s, %s, %s, 'extract_operator.py')
            RETURNING id
        """, (args.url, "operator extraction source",
              f"Auto-added by extract_operator.py for fid={args.fid}"))
        source_id = cur.fetchone()[0]

    # Look up legacy fid (us_beach_points.fid) if available, else use arena fid
    cur.execute("""
        SELECT location_id FROM public.beaches WHERE arena_group_id = %s LIMIT 1
    """, (args.fid,))
    legacy_loc = cur.fetchone()
    legacy_fid = args.fid  # since we re-keyed extractions to arena keyspace in 3a

    parsed_value = json.dumps(obj.get("operator")) if parse_ok else None

    run_id = f"operator-{uuid.uuid4().hex[:8]}"
    cur.execute("""
        INSERT INTO public.beach_policy_extractions
            (fid, arena_group_id, source_id, variant_id, field_name,
             source_type, variant_key, raw_response, parsed_value,
             evidence_quote, parse_succeeded,
             extraction_method, run_id, model_name,
             input_tokens, output_tokens, latency_ms)
        VALUES (%s, %s, %s, %s, 'operator',
                'llm_extraction', 'llm_v1', %s, %s,
                %s, %s,
                'llm_hybrid', %s, 'claude-sonnet-4-6',
                %s, %s, %s)
        RETURNING id
    """, (
        legacy_fid, args.fid, source_id, variant_id,
        raw[:6000], parsed_value,
        (obj.get("operator") or {}).get("evidence"),
        parse_ok,
        run_id,
        resp["input_tokens"], resp["output_tokens"], resp["latency_ms"],
    ))
    new_id = cur.fetchone()[0]
    conn.commit()
    print(f"  inserted beach_policy_extractions.id = {new_id}, run_id = {run_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
