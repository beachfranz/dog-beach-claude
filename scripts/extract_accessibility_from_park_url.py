"""Phase B Slice 2 — extract accessibility signals from park_url_extractions
via Haiku, write to BEP accessibility field_group with source='park_url'.

Strategy: only run on fids whose cached park URL text contains at least
one ADA-relevant keyword (wheelchair, ADA, accessible, mobi-mat,
handicap, disabled access). Saves cost vs hitting every park URL.

Output shape per fid (matches beach_amenities.accessibility_features):
  {
    accessible_parking:   true/false/null,
    accessible_restrooms: true/false/null,
    path_to_sand:         "boardwalk"/"ramp"/"mat"/"stairs_only"/"none"/null,
    beach_mat:            true/false/null,
    wheelchair_rental:    {available, provider, url} or null,
    accessible_viewing:   true/false/null,
    notes:                short paraphrase or null
  }

Strict: extract only what's explicitly stated; null when not mentioned.

Usage:
   python scripts/extract_accessibility_from_park_url.py            # all ADA-keyword fids
   python scripts/extract_accessibility_from_park_url.py --fids 6212,8325
   python scripts/extract_accessibility_from_park_url.py --state CA
   python scripts/extract_accessibility_from_park_url.py --dry-run  # show targets, no LLM
"""
from __future__ import annotations
import argparse, json, os, sys, time, urllib.parse, urllib.request

# scripts.common loads .env + injects truststore at package init.
# This script previously had NO truststore inject — Anthropic calls
# would fail with CERTIFICATE_VERIFY_FAILED on Windows AV-MITM. Side
# effect of common's __init__ fixes that latent bug.
# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect
from scripts.common.llm import HAIKU

ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = HAIKU

PROMPT = """Extract accessibility (ADA / wheelchair) signals from this park/beach website text.

Output ONE JSON object with these keys. Use null when not explicitly stated. DO NOT INFER. DO NOT HALLUCINATE.

{
  "accessible_parking":   true | false | null,
  "accessible_restrooms": true | false | null,
  "path_to_sand":         "boardwalk" | "ramp" | "mat" | "stairs_only" | "none" | null,
  "beach_mat":            true | false | null,
  "wheelchair_rental":    {"available": true, "provider": "...", "url": "..."} | null,
  "accessible_viewing":   true | false | null,
  "notes":                "short paraphrase of specific access detail" | null
}

Rules:
- "accessible_parking": true only if text mentions ADA/wheelchair/accessible parking specifically. Generic "parking available" → null.
- "accessible_restrooms": true only if text mentions ADA/wheelchair restrooms specifically.
- "path_to_sand": pick ONE value if mentioned. "boardwalk" for boardwalk/wood paths to beach; "ramp" for paved ramps; "mat" for beach mats (Mobi-Mat etc.); "stairs_only" if only stairs; "none" if no accessible path.
- "beach_mat": true if text mentions beach mat / Mobi-Mat / sand mat installed. Specifically for sand access.
- "wheelchair_rental": fill if text mentions beach-wheelchair / floating-chair / similar rental program. Provider = organization name. URL if given.
- "accessible_viewing": true only if text mentions accessible overlook/viewing platform/ADA-accessible vista distinct from accessing the beach itself.
- "notes": short paraphrased detail like "boardwalk ends at first dune line" or "wheelchairs available at Bay Club office, free with ID". Only include if it adds specific accessible-access info not captured by other fields.

Output ONLY the JSON object. No commentary, no markdown code fence.

Text:
%s
"""

ADA_KEYWORDS = r"\m(wheelchair|ADA|accessible|accessibility|mobi.?mat|handicap|disabled access)\M"


def fetch_targets(states: list[str] | None, fids: list[int] | None):
    """Return list of (fid, concatenated_text) for ADA-keyword-rich park_url_extractions."""
    with connect() as c, c.cursor() as cur:
        clauses = []
        params = []
        if fids:
            clauses.append("pue.arena_group_id = any(%s::bigint[])")
            params.append(fids)
        elif states:
            clauses.append("g.state = any(%s::text[])")
            params.append(states)
        # Always: must have ADA-relevant keyword in the text
        clauses.append(f"lower(pue.raw_text) ~ '{ADA_KEYWORDS}'")
        clauses.append("g.is_active")
        where = " and ".join(clauses)
        sql = f"""
          select pue.arena_group_id,
                 string_agg(coalesce(pue.raw_text,''), E'\\n\\n--page--\\n\\n'
                            order by pue.scraped_at desc) as concatenated_text
            from public.park_url_extractions pue
            join public.beaches_gold g on g.fid = pue.arena_group_id
           where {where}
           group by pue.arena_group_id
           order by pue.arena_group_id
        """
        cur.execute(sql, params)
        return cur.fetchall()


def call_haiku(text: str) -> tuple[dict, dict]:
    """Send page text to Haiku, return (parsed_json, usage)."""
    # Cap text to ~6000 chars to keep input cost down
    text = (text or "")[:6000]
    body = {
        "model": MODEL,
        "max_tokens": 400,
        "messages": [{"role": "user", "content": PROMPT % text}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(), method="POST",
        headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.loads(r.read())
    out = "".join(p["text"] for p in resp["content"] if p["type"] == "text").strip()
    # Strip code fence if model added one
    if out.startswith("```"):
        out = out.split("```", 2)[1]
        if out.startswith("json\n"):
            out = out[5:]
        out = out.rstrip("` \n")
    try:
        parsed = json.loads(out)
    except json.JSONDecodeError:
        parsed = None
    return parsed, resp.get("usage", {})


def upsert_bep(fid: int, parsed: dict):
    """Upsert accessibility BEP row with source='park_url'."""
    # Strip null-everything values; if nothing extracted, skip
    cleaned = {k: v for k, v in parsed.items() if v is not None}
    if not cleaned:
        return False
    with connect() as c, c.cursor() as cur:
        cur.execute("""
          insert into public.beach_enrichment_provenance
            (gold_fid, field_group, source, source_url, claimed_values, confidence,
             is_canonical, extraction_type, updated_at)
          values (%s, 'accessibility', 'park_url',
                  (select source_url from public.park_url_extractions
                    where arena_group_id=%s order by scraped_at desc limit 1),
                  %s::jsonb, 0.80, false, 'derived_url_crawl', now())
          on conflict (gold_fid, field_group, source) do update
            set claimed_values = excluded.claimed_values, updated_at = now()
          where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
        """, (fid, fid, json.dumps(cleaned)))
        c.commit()
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fids", help="comma-separated fid list")
    ap.add_argument("--state", help="restrict to state code")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    fids = [int(s) for s in args.fids.split(",")] if args.fids else None
    states = [args.state.upper()] if args.state else None

    targets = fetch_targets(states, fids)
    print(f"targets: {len(targets)} fids with ADA-keyword in park_url text", flush=True)
    if args.dry_run:
        for fid, _ in targets[:20]: print(f"  fid {fid}")
        return

    in_tot = out_tot = 0
    emitted = skipped = errored = 0
    t0 = time.time()
    for i, (fid, text) in enumerate(targets, 1):
        try:
            parsed, usage = call_haiku(text)
            in_tot += usage.get("input_tokens", 0)
            out_tot += usage.get("output_tokens", 0)
            if not parsed:
                errored += 1
                print(f"  [{i:3d}/{len(targets)}] fid={fid} PARSE_FAIL", flush=True)
                continue
            wrote = upsert_bep(fid, parsed)
            if wrote:
                emitted += 1
                non_null = [k for k, v in parsed.items() if v not in (None, '', {})]
                print(f"  [{i:3d}/{len(targets)}] fid={fid} ok  fields={non_null}", flush=True)
            else:
                skipped += 1
                print(f"  [{i:3d}/{len(targets)}] fid={fid} all-null, skip", flush=True)
        except Exception as e:
            errored += 1
            print(f"  [{i:3d}/{len(targets)}] fid={fid} ERROR: {str(e)[:140]}", flush=True)

    cost = in_tot * 1 / 1e6 + out_tot * 5 / 1e6   # Haiku 4.5 pricing
    print(f"\n=== DONE === emitted={emitted} skipped={skipped} errored={errored}  "
          f"tokens={in_tot}/{out_tot}  est_cost=${cost:.3f}  wall={time.time()-t0:.1f}s")


if __name__ == "__main__":
    sys.exit(main())
