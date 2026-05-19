"""Operate Phase 1c — per-operator audit of the 418 extractions.

CORRECTED 2026-05-18 evening: prior version JOINed operator_dogs_policy
against public.operator (singular, 153 rows) but the actual FK target is
public.operators (plural, 9,275 rows). The previous "15 corrupt" finding
was all false positives from coincidental ID collisions across unrelated
tables. This version uses the correct FK.

Classifies each operator_dogs_policy row into:
  - corrupt           — extraction source_url's domain doesn't match the
                        operator's name (e.g., op_id=7 PSHDB with source_url
                        pointing to longbeach.gov/rosies-dog-beach)
  - duplicative_clean — operator already has a operator_posted_policy ps row
                        AND the extraction's source_url matches its content
  - duplicative_dirty — operator has a ps row BUT the extraction adds
                        something new (different URL / richer content) —
                        worth bridging as supplementary
  - net_new           — operator has NO ps row + extraction looks legitimate
                        → real Operate gap; bridge candidate
  - low_conf          — pass_c_confidence below threshold; skip
  - no_extraction     — operator has no extraction (shouldn't appear; sanity check)

OUTPUTS:
  tmp/operate_phase1c_audit_<ts>.jsonl     (per-row classification)
  tmp/operate_phase1c_summary_<ts>.md      (human-readable summary)

Per Franz "1c" + docs/operate_pipeline_spec.md §7 Phase 1c.
Read-only; emits no SQL.
"""

from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import datetime
import json
import os
import urllib.parse
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")


def _connect_pg():
    import psycopg2
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


def _enc(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    raise TypeError(f"not JSON serializable: {type(o).__name__}")


# ─── Corruption heuristic ────────────────────────────────────────────

def _name_tokens(name: str) -> set[str]:
    """Lowercase token set from operator name, dropping stopwords."""
    stop = {"city", "of", "county", "the", "and", "national", "state", "park",
            "recreation", "area", "society", "preservation", "department",
            "preserve", "dept", "service", "services", "park", "parks",
            "association", "district", "land", "trust", "conservancy"}
    tokens = {t for t in name.lower().replace(",", " ").replace("-", " ").split()
              if t and t not in stop and len(t) > 2}
    return tokens


def _url_domain_tokens(url: str) -> set[str]:
    """Lowercase token set from URL host (drops common TLDs + gov/org/com)."""
    if not url:
        return set()
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return set()
    # Strip TLD + www
    host = host.replace("www.", "")
    parts = host.split(".")
    # Drop the last 1-2 parts (TLD) — e.g., "longbeach.gov" → "longbeach"
    if len(parts) >= 2:
        parts = parts[:-1]
    # Also drop subdomains like "www2", numeric
    tokens = {p for p in parts if p and not p.isdigit() and p not in
              {"www", "ww2", "ww3", "secure"}}
    return tokens


def classify_corruption(operator_name: str, source_url: str) -> tuple[bool, str]:
    """Detect corruption: extraction URL doesn't match operator name tokens.
    Returns (is_corrupt, reason)."""
    if not source_url:
        return False, "no source_url to check"
    op_tokens = _name_tokens(operator_name)
    url_tokens = _url_domain_tokens(source_url)
    if not op_tokens:
        return False, "no meaningful op-name tokens to compare"
    if not url_tokens:
        return False, "no meaningful URL host tokens to compare"
    # Token overlap heuristic: if NO op-name token appears in url tokens
    # AND no url-host token appears in op-name → corrupt
    overlap = op_tokens & url_tokens
    if overlap:
        return False, f"token overlap: {sorted(overlap)}"
    # Also check substring (longbeach.gov vs "Long Beach")
    op_concat = "".join(op_tokens)
    url_concat = "".join(url_tokens)
    if op_concat and url_concat:
        if op_concat in url_concat or url_concat in op_concat:
            return False, f"substring match: {op_concat!r} / {url_concat!r}"
        # Any token substring
        for ot in op_tokens:
            if ot in url_concat:
                return False, f"op-token {ot!r} in url"
        for ut in url_tokens:
            if ut in op_concat:
                return False, f"url-token {ut!r} in op-name"
    return True, f"no overlap: op={sorted(op_tokens)} vs url={sorted(url_tokens)}"


# ─── Audit core ──────────────────────────────────────────────────────

def audit(conf_threshold: float):
    conn = _connect_pg()
    try:
        with conn, conn.cursor() as cur:
            # operator_dogs_policy.operator_id → public.operators (plural)
            # ALSO probe public.operator (singular) by canonical_name for bridge-match potential
            cur.execute("""
                SELECT
                    o.id AS operator_id, o.canonical_name AS operator_name,
                    o.level AS operator_type, o.state_code, o.usbeach_count AS beach_count,
                    odp.source_url, odp.default_rule, odp.leash_required,
                    odp.pass_a_confidence, odp.pass_c_confidence,
                    odp.summary,
                    -- Bridge-match: does a singular-operator row exist with matching name?
                    (SELECT op2.id FROM public.operator op2
                     WHERE LOWER(op2.name) = LOWER(o.canonical_name)
                     LIMIT 1) AS singular_op_match_id,
                    (SELECT COUNT(*) FROM public.beach_operator bo
                     JOIN public.operator op2 ON op2.id = bo.operator_id
                     WHERE LOWER(op2.name) = LOWER(o.canonical_name)) AS n_beach_links,
                    (SELECT COUNT(*) FROM public.policy_source ps
                     JOIN public.operator op2 ON op2.id = ps.issuing_operator_id
                     WHERE LOWER(op2.name) = LOWER(o.canonical_name)
                       AND ps.subtype = 'operator_posted_policy') AS n_op_ps_rows,
                    (SELECT array_agg(ps.id::text || ': ' || COALESCE(ps.source_url,'<no url>'))
                     FROM public.policy_source ps
                     JOIN public.operator op2 ON op2.id = ps.issuing_operator_id
                     WHERE LOWER(op2.name) = LOWER(o.canonical_name)
                       AND ps.subtype = 'operator_posted_policy') AS existing_op_ps
                FROM public.operators o
                JOIN public.operator_dogs_policy odp ON odp.operator_id = o.id
                WHERE odp.pass_c_confidence IS NOT NULL
                ORDER BY odp.pass_c_confidence DESC, o.id
            """)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

            # Singular operators with ps rows but no matching plural-extraction by name
            cur.execute("""
                SELECT o.id, o.name, o.type
                FROM public.operator o
                WHERE EXISTS (
                    SELECT 1 FROM public.policy_source ps
                    WHERE ps.issuing_operator_id = o.id
                      AND ps.subtype = 'operator_posted_policy'
                ) AND NOT EXISTS (
                    SELECT 1 FROM public.operators op2
                    JOIN public.operator_dogs_policy odp ON odp.operator_id = op2.id
                    WHERE LOWER(op2.canonical_name) = LOWER(o.name)
                )
            """)
            no_extr_w_ps = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    finally:
        conn.close()

    print(f"Operators w/ extraction: {len(rows)}")
    print(f"Operators w/ ps_row but NO extraction (sanity): {len(no_extr_w_ps)}")

    classifications = []
    for r in rows:
        op_id = r["operator_id"]
        op_name = r["operator_name"]
        source_url = r["source_url"] or ""
        conf = float(r["pass_c_confidence"]) if r["pass_c_confidence"] else 0.0

        if conf < conf_threshold:
            cls, reason = "low_conf", f"conf={conf:.2f} < {conf_threshold}"
        else:
            is_corrupt, corrupt_reason = classify_corruption(op_name, source_url)
            if is_corrupt:
                cls, reason = "corrupt", corrupt_reason
            elif r["n_op_ps_rows"] and r["n_op_ps_rows"] > 0:
                # Already has ps row → check if source_url matches one of them
                existing = r.get("existing_op_ps") or []
                url_match = any(source_url and source_url in (e or "") for e in existing)
                if url_match:
                    cls, reason = "duplicative_clean", f"same source_url as ps row(s) {existing}"
                else:
                    cls, reason = "duplicative_dirty", f"diff source_url; existing ps={existing}"
            else:
                cls, reason = "net_new", f"no existing op ps; conf={conf:.2f}"

        classifications.append({
            "operator_id_plural": op_id,
            "operator_name": op_name,
            "operator_type": r["operator_type"],
            "state_code": r["state_code"],
            "source_url": source_url,
            "default_rule": r["default_rule"],
            "leash_required": r["leash_required"],
            "pass_c_confidence": conf,
            "singular_op_match_id": r.get("singular_op_match_id"),
            "n_beach_links": r["n_beach_links"],
            "n_op_ps_rows": r["n_op_ps_rows"] or 0,
            "classification": cls,
            "reason": reason,
            "summary_snip": (r["summary"] or "")[:200],
        })

    return classifications, no_extr_w_ps


def main():
    ap = argparse.ArgumentParser(description="Operate Phase 1c — per-operator extraction audit")
    ap.add_argument("--confidence-threshold", type=float, default=0.7,
                    help="Min pass_c_confidence (default 0.7; matches Codify auto-commit)")
    args = ap.parse_args()

    classifications, no_extr = audit(args.confidence_threshold)

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    tmp = ROOT / "tmp"
    tmp.mkdir(exist_ok=True, parents=True)

    jsonl_path = tmp / f"operate_phase1c_audit_{ts}.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for c in classifications:
            f.write(json.dumps(c, default=_enc) + "\n")
    print(f"\nJSONL: {jsonl_path}")

    # Tally
    tally: dict[str, int] = {}
    for c in classifications:
        tally[c["classification"]] = tally.get(c["classification"], 0) + 1
    print("\n=== CLASSIFICATIONS ===")
    for k in sorted(tally):
        print(f"  {k:<22} {tally[k]}")
    print(f"  TOTAL                  {len(classifications)}")

    # Per-class examples
    md_lines = [
        f"# Operate Phase 1c — Per-operator extraction audit",
        f"",
        f"Generated 2026-05-18 by `scripts/audit_operator_extractions.py`.",
        f"Confidence threshold: {args.confidence_threshold}",
        f"",
        f"## Summary",
        f"",
        f"| Classification | Count | Meaning |",
        f"|---|---:|---|",
        f"| net_new            | {tally.get('net_new', 0):>4} | Operator has NO existing operator_posted_policy ps row + extraction looks legitimate (no URL/name mismatch). Real Operate gap; bridge candidates. |",
        f"| duplicative_clean  | {tally.get('duplicative_clean', 0):>4} | Operator already has a ps row with the same source_url. Extraction is redundant. |",
        f"| duplicative_dirty  | {tally.get('duplicative_dirty', 0):>4} | Operator has ps row(s) but extraction's source_url is different. Possible supplementary value. |",
        f"| corrupt            | {tally.get('corrupt', 0):>4} | Extraction source_url's domain has NO token overlap with operator name. Likely wrong attribution (PSHDB→Rosie's pattern). |",
        f"| low_conf           | {tally.get('low_conf', 0):>4} | pass_c_confidence < threshold; skip. |",
        f"",
        f"Operators with ps row but NO extraction (sanity check): {len(no_extr)}",
        f"",
    ]

    for cls_name in ["corrupt", "net_new", "duplicative_dirty", "duplicative_clean"]:
        examples = [c for c in classifications if c["classification"] == cls_name]
        if not examples:
            continue
        md_lines.append(f"## `{cls_name}` ({len(examples)} rows)")
        md_lines.append("")
        md_lines.append("| op_id | operator | rule | conf | beaches | source_url | reason |")
        md_lines.append("|---:|---|---|---:|---:|---|---|")
        for ex in examples[:30]:  # cap at 30 per class
            url_short = (ex["source_url"] or "")[:60]
            md_lines.append(
                f"| {ex['operator_id_plural']} | {ex['operator_name'][:35]} | "
                f"{ex['default_rule']} | {ex['pass_c_confidence']:.2f} | "
                f"{ex['n_beach_links']} | `{url_short}` | {ex['reason'][:80]} |"
            )
        if len(examples) > 30:
            md_lines.append(f"\n*(… and {len(examples) - 30} more)*")
        md_lines.append("")

    md_path = tmp / f"operate_phase1c_summary_{ts}.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    print(f"Summary: {md_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
