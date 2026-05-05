"""repass_zone_rules.py — production text-repass for section-aware dog rules.

Reads existing dogs evidence in BEP for beaches that signal sectional
ambiguity (allowed in restricted/mixed/seasonal AND have prose), runs
the closed_zones_v3 prompt to extract structured zone_rules jsonb, and
writes the result back as a new BEP source row (`text_repass_v1`).

Inputs come from text we already paid to extract — no new web fetches.
Output is structured zone_rules ready for the moderation UI + (eventually)
consumer surface promotion.

Usage:
  # Pilot on the 8 anchor beaches (~$0.05)
  python scripts/repass_zone_rules.py --anchors

  # Pilot on N random "restricted/mixed/seasonal" beaches (~$0.005 each)
  python scripts/repass_zone_rules.py --pilot 20

  # Specific fids
  python scripts/repass_zone_rules.py --fids 6202,9716,3407

  # Full run (~$3 for ~600 unique beaches)
  python scripts/repass_zone_rules.py --full

  # Dry run (build the prompts, don't call API or write rows)
  python scripts/repass_zone_rules.py --pilot 5 --dry-run

Writes to public.beach_enrichment_provenance:
  source = 'text_repass_v1'
  field_group = 'dogs'
  claimed_values = {
    'zone_rules': <structured jsonb>,
    'allowed': <derived>,    # for legacy flat-key consensus voting
    'leash_required': <derived>,
    'notes': <global_notes from output>
  }
  confidence = inherits from the strongest input source's confidence × 0.85
               (re-pass adds inferential structure but doesn't elevate raw fact)
  source_url = canonical governance source_url or null

Idempotent via on-conflict update.
"""

from __future__ import annotations
import argparse
import io
import json
import os
import random
import sys
import time
import urllib.parse
from pathlib import Path

import anthropic
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

# Force UTF-8 stdout for Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

ANTHROPIC = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
MODEL = "claude-haiku-4-5-20251001"
SOURCE = "text_repass_v1"

# Same prompt as eval_closed_zones_v3.py — single source of truth.
# Pulled inline from the eval script to keep one canonical version.
from eval_closed_zones_v3 import (  # type: ignore
    PROMPT_BODY,
    pull_beach_inputs,
    format_source_text,
    parse_output,
)

ANCHOR_FIDS = [6202, 9716, 8339, 9717, 3407, 8673, 8356, 8740]


# ============================================================================
# Target selection
# ============================================================================

def select_targets(conn, args):
    """Return list of fids to process based on CLI args."""
    if args.fids:
        return [int(f.strip()) for f in args.fids.split(",")]
    if args.anchors:
        return ANCHOR_FIDS

    # --pilot N or --full: pull from the gap surface
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct e.gold_fid
              from public.beach_enrichment_provenance e
              join public.beaches_gold g on g.fid = e.gold_fid
             where e.field_group = 'dogs'
               and e.gold_fid is not null
               and g.is_active
               and (e.claimed_values->>'allowed') in ('restricted','mixed','seasonal')
               and length(coalesce(
                     e.claimed_values->>'notes',
                     e.claimed_values->>'zone_description', '')) > 50
            """
        )
        all_fids = [row[0] for row in cur.fetchall()]

    # Exclude beaches that already have a fresh text_repass_v1 row
    if not args.force:
        with conn.cursor() as cur:
            cur.execute(
                """
                select gold_fid from public.beach_enrichment_provenance
                 where source = %s and gold_fid is not null
                """,
                (SOURCE,),
            )
            already = {row[0] for row in cur.fetchall()}
        all_fids = [f for f in all_fids if f not in already]
        if already:
            print(f"  ({len(already)} already processed; pass --force to redo)")

    if args.pilot:
        random.seed(42)  # deterministic pilot selection
        random.shuffle(all_fids)
        return all_fids[: args.pilot]
    if args.full:
        return all_fids
    print("ERROR: provide --anchors, --pilot N, --full, or --fids")
    sys.exit(2)


# ============================================================================
# Derivations from zone_rules
# ============================================================================

def derive_flat(zone_rules):
    """Derive flat allowed/leash_required values from the zone_rules jsonb,
    so legacy consensus voting on those fields includes the repass output."""
    sections_seen = []
    for region in zone_rules.get("regions", []):
        for sec, body in (region.get("sections") or {}).items():
            sections_seen.append((sec, body.get("rule")))

    if not sections_seen:
        return {"allowed": "unknown", "leash_required": None}

    # Sand is the load-bearing section for "allowed". Pull its rule.
    sand_rule = next((r for s, r in sections_seen if s == "sand"), None)
    if sand_rule == "off_leash":
        return {"allowed": "yes", "leash_required": "off_leash"}
    if sand_rule == "on_leash":
        return {"allowed": "yes", "leash_required": "on_leash"}
    if sand_rule == "not_allowed":
        # Check if any other section allows dogs (e.g., parking lot at NPS)
        any_allowed = any(r in ("on_leash", "off_leash") for _, r in sections_seen)
        if any_allowed:
            return {"allowed": "restricted", "leash_required": "on_leash"}
        return {"allowed": "no", "leash_required": None}
    # Sand absent: pick the most-permissive rule across all sections
    rules = {r for _, r in sections_seen if r}
    if "off_leash" in rules:
        return {"allowed": "yes", "leash_required": "off_leash"}
    if "on_leash" in rules:
        return {"allowed": "yes", "leash_required": "on_leash"}
    if rules <= {"not_allowed", "unknown"}:
        return {"allowed": "no", "leash_required": None}
    return {"allowed": "unknown", "leash_required": None}


def derive_confidence(rows):
    """Take the max input confidence and discount by 0.85.
    Re-pass adds structure but doesn't add raw factual confidence."""
    if not rows:
        return 0.5
    max_conf = max(r["confidence"] for r in rows if r["confidence"] is not None)
    return round(max_conf * 0.85, 3)


def derive_source_url(rows):
    """Use the highest-confidence row's source_url, if any."""
    for r in sorted(rows, key=lambda x: x["confidence"] or 0, reverse=True):
        if r.get("source_url"):
            return r["source_url"]
    return None


# ============================================================================
# Write
# ============================================================================

def write_repass(conn, gold_fid, zone_rules, derived, confidence, source_url):
    """Upsert a BEP row for source='text_repass_v1' on this fid."""
    claimed = {
        "zone_rules": zone_rules,
        "allowed": derived["allowed"],
        "notes": zone_rules.get("global_notes", ""),
    }
    if derived["leash_required"] is not None:
        claimed["leash_required"] = derived["leash_required"]

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.beach_enrichment_provenance
              (gold_fid, field_group, source, confidence,
               claimed_values, source_url, updated_at)
            values (%s, 'dogs', %s, %s, %s::jsonb, %s, now())
            on conflict (gold_fid, field_group, source) where gold_fid is not null
            do update set
              confidence = excluded.confidence,
              claimed_values = excluded.claimed_values,
              source_url = excluded.source_url,
              updated_at = now(),
              is_canonical = false
            """,
            (gold_fid, SOURCE, confidence, json.dumps(claimed), source_url),
        )
    conn.commit()


# ============================================================================
# Main
# ============================================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--anchors", action="store_true", help="run on the 8 anchor fids")
    ap.add_argument("--pilot", type=int, default=0, help="N random gap-surface beaches")
    ap.add_argument("--full", action="store_true", help="run on all gap-surface beaches")
    ap.add_argument("--fids", help="comma-separated list of fids")
    ap.add_argument("--force", action="store_true", help="re-process already-done fids")
    ap.add_argument("--dry-run", action="store_true", help="build prompts, don't call API")
    args = ap.parse_args()

    conn = psycopg2.connect(**PG)
    targets = select_targets(conn, args)
    print(f"Targets: {len(targets)} beaches")

    if args.dry_run:
        print("--dry-run: not calling API")
        for fid in targets[:5]:
            beach, rows = pull_beach_inputs(conn, fid)
            print(f"  fid={fid:>5} {beach['name']:<35} sources={len(rows)} "
                  f"operator={beach.get('operator_name')}")
        return 0

    if not targets:
        print("Nothing to do.")
        return 0

    # Cost preview
    est = len(targets) * 0.0057
    print(f"Estimated cost: ${est:.3f} ({len(targets)} × ~$0.006 Haiku 4.5)")
    if len(targets) > 50:
        print("  (large run — Ctrl-C in the next 3 seconds to abort)")
        time.sleep(3)

    succeeded = 0
    failed = 0
    parse_errors = 0
    total_in = 0
    total_out = 0

    for i, fid in enumerate(targets, 1):
        try:
            beach, rows = pull_beach_inputs(conn, fid)
        except Exception as e:
            print(f"  [{i}/{len(targets)}] fid={fid} FETCH ERROR: {e}")
            failed += 1
            continue

        if not rows:
            print(f"  [{i}/{len(targets)}] fid={fid} {beach['name'][:30]:<30} no source rows; skip")
            continue

        source_text = format_source_text(rows)
        prompt = PROMPT_BODY.format(
            beach_name=beach["name"],
            operator_name=beach.get("operator_name") or "(unknown)",
            county=beach.get("county_name") or "",
            source_text=source_text,
        )

        try:
            resp = ANTHROPIC.messages.create(
                model=MODEL,
                max_tokens=2500,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text
            total_in += resp.usage.input_tokens
            total_out += resp.usage.output_tokens
        except Exception as e:
            print(f"  [{i}/{len(targets)}] fid={fid} LLM ERROR: {e}")
            failed += 1
            continue

        try:
            zone_rules = parse_output(raw)
        except json.JSONDecodeError as e:
            parse_errors += 1
            print(f"  [{i}/{len(targets)}] fid={fid} {beach['name'][:30]:<30} JSON PARSE ERROR")
            print(f"    raw[:200]: {raw[:200]}")
            continue

        derived = derive_flat(zone_rules)
        confidence = derive_confidence(rows)
        source_url = derive_source_url(rows)

        try:
            write_repass(conn, fid, zone_rules, derived, confidence, source_url)
        except Exception as e:
            conn.rollback()
            print(f"  [{i}/{len(targets)}] fid={fid} WRITE ERROR: {e}")
            failed += 1
            continue

        # Summary count of what we got
        n_regions = len(zone_rules.get("regions", []))
        n_sections = sum(len(r.get("sections") or {}) for r in zone_rules.get("regions", []))
        print(f"  [{i}/{len(targets)}] fid={fid} {beach['name'][:35]:<35} "
              f"regions={n_regions} sections={n_sections} "
              f"allowed={derived['allowed']} conf={confidence}")
        succeeded += 1

    print()
    print(f"=== TOTALS ===")
    print(f"  succeeded:    {succeeded}")
    print(f"  failed:       {failed}")
    print(f"  parse_errors: {parse_errors}")
    print(f"  tokens in:    {total_in:,}")
    print(f"  tokens out:   {total_out:,}")
    cost = total_in * 1.0 / 1_000_000 + total_out * 5.0 / 1_000_000
    print(f"  est cost:     ${cost:.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
