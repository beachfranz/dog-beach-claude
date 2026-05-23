"""Translate beach_active_alert rows → dog-impact text via YAML + LLM wildcards.

Per docs/water_conditions_advisory_spec.md Phase 2.

Two paths:
  YAML lookup     — direct mapping for ~30 well-defined NWS event types
  LLM auto        — wildcards (Beach Hazards Statement / Special Weather
                    Statement / Marine Weather Statement) get Haiku-translated
                    with confidence gate. Below threshold → wildcard_pending
                    (admin queue).

Run:
  python scripts/translate_nws_alerts.py                # YAML lookups only
  python scripts/translate_nws_alerts.py --use-llm      # YAML + LLM wildcards
  python scripts/translate_nws_alerts.py --rebuild      # re-translate everything

Placeholders in YAML text ({valid_to_local}, {beach_name}, etc.) are
substituted at render time (Phase 4 surface).
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import json
import os
from pathlib import Path

import psycopg2
import psycopg2.extras

from scripts.common.db import connect
from scripts.common.llm import HAIKU

# ROOT is still needed for YAML_PATH (config/nws_dog_impact_map.yaml).
ROOT = Path(__file__).resolve().parents[1]

YAML_PATH = ROOT / "config" / "nws_dog_impact_map.yaml"
LLM_CONFIDENCE_THRESHOLD = 0.7

# Wildcard event types that always need per-instance translation
WILDCARD_EVENTS = {
    "Beach Hazards Statement",
    "Special Weather Statement",
    "Marine Weather Statement",
    "Hazardous Weather Outlook",
}


def load_yaml_map() -> dict[str, dict]:
    """Returns {event_type: {dog_impact_class, severity, text}}."""
    import yaml
    with open(YAML_PATH, encoding="utf-8") as f:
        rows = yaml.safe_load(f) or []
    return {r["event_type"]: r for r in rows if "event_type" in r}


def llm_translate(event_type: str, headline: str, description: str, instruction: str) -> dict:
    """Haiku auto-translate for wildcard alerts.
    Returns {dog_impact_class, dog_impact_text, confidence}.
    """
    import anthropic
    client = anthropic.Anthropic()
    prompt = f"""You translate NWS active alerts into dog-walker safety advisories.

Alert:
  event_type: {event_type}
  headline: {headline or '(none)'}
  description: {(description or '')[:600]}
  instruction: {(instruction or '')[:400]}

Return JSON only, no prose, no markdown. Keys:
  dog_impact_class: one of [skip_swim, paws_warning, no_dogs_today, blowing_sand, visibility, air_quality, fire_risk, evacuate, cold_paws, review_required]
  dog_impact_text: ONE conversational sentence telling a dog owner what to do (≤140 chars). No emojis. Start with the action, not "you should".
  confidence: 0.0-1.0 — how sure you are this translation is right for a dog-walker context (low if alert is unclear or not really dog-relevant)
"""
    msg = client.messages.create(
        model=HAIKU,
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = "".join(b.text for b in msg.content if b.type == "text").strip()
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"dog_impact_class": "review_required",
                "dog_impact_text": None,
                "confidence": 0.0}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--use-llm", action="store_true",
                    help="Auto-translate wildcards via Haiku (small cost)")
    ap.add_argument("--rebuild", action="store_true",
                    help="Re-translate every row (default: only NULL translations)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print decisions, no DB writes")
    args = ap.parse_args()

    yaml_map = load_yaml_map()
    print(f"Loaded {len(yaml_map)} YAML translations", flush=True)

    conn = connect()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        where = "" if args.rebuild else "AND translation_source IS NULL"
        cur.execute(f"""
            SELECT beach_fid, advisory_key, event_type AS nws_event_type, severity,
                   raw_data->>'headline'    AS headline,
                   raw_data->>'description' AS description,
                   raw_data->>'instruction' AS instruction
              FROM public.beach_advisory
             WHERE source = 'nws'
               AND valid_to > now()
               {where}
        """)
        rows = cur.fetchall()
        print(f"Translation candidates: {len(rows)} rows", flush=True)
        if not rows:
            return 0

        upd = conn.cursor()
        n_yaml = n_llm_used = n_llm_pending = n_unknown = 0
        for r in rows:
            evt = r["nws_event_type"]
            yaml_hit = yaml_map.get(evt)

            decision: dict | None = None
            if yaml_hit and yaml_hit.get("text"):
                # Direct YAML lookup with non-null text
                decision = {
                    "dog_impact_class": yaml_hit["dog_impact_class"],
                    "dog_impact_text":  yaml_hit["text"],
                    "translation_source": "yaml_lookup",
                    "translation_confidence": None,
                }
                n_yaml += 1
            elif evt in WILDCARD_EVENTS or (yaml_hit and not yaml_hit.get("text")):
                if args.use_llm:
                    res = llm_translate(evt, r["headline"], r["description"], r["instruction"])
                    conf = float(res.get("confidence") or 0)
                    if conf >= LLM_CONFIDENCE_THRESHOLD and res.get("dog_impact_text"):
                        decision = {
                            "dog_impact_class": res.get("dog_impact_class", "review_required"),
                            "dog_impact_text":  res["dog_impact_text"],
                            "translation_source": "llm_auto",
                            "translation_confidence": conf,
                        }
                        n_llm_used += 1
                    else:
                        decision = {
                            "dog_impact_class": "review_required",
                            "dog_impact_text":  None,
                            "translation_source": "wildcard_pending",
                            "translation_confidence": conf,
                        }
                        n_llm_pending += 1
                else:
                    decision = {
                        "dog_impact_class": "review_required",
                        "dog_impact_text":  None,
                        "translation_source": "wildcard_pending",
                        "translation_confidence": None,
                    }
                    n_llm_pending += 1
            else:
                # Unknown event_type not in YAML → log + mark pending
                decision = {
                    "dog_impact_class": "review_required",
                    "dog_impact_text":  None,
                    "translation_source": "wildcard_pending",
                    "translation_confidence": None,
                }
                n_unknown += 1
                print(f"  [unknown event_type] {evt!r} (fid={r['beach_fid']}, id={r['alert_id'][:30]})", flush=True)

            if args.dry_run:
                continue
            upd.execute("""
                UPDATE public.beach_advisory
                   SET dog_impact_class       = %(dog_impact_class)s,
                       dog_impact_text        = %(dog_impact_text)s,
                       translation_source     = %(translation_source)s,
                       translation_confidence = %(translation_confidence)s
                 WHERE beach_fid = %(beach_fid)s AND advisory_key = %(advisory_key)s
            """, {**decision, "beach_fid": r["beach_fid"], "advisory_key": r["advisory_key"]})

        if not args.dry_run:
            conn.commit()

        print(f"\n=== SUMMARY ===")
        print(f"  YAML direct hits:      {n_yaml}")
        print(f"  LLM auto-translated:   {n_llm_used}")
        print(f"  Wildcards pending:     {n_llm_pending}")
        print(f"  Unknown event_types:   {n_unknown}")
        if args.dry_run:
            print(f"  [dry-run] no writes")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
