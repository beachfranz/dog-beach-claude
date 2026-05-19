"""Operate pipeline Phase 1: bridge operator_dogs_policy extractions →
policy_source (subtype=operator_posted_policy) + beach_policy_source rows
so the existing 3-pass extraction work reaches the cascade.

Per docs/operate_pipeline_spec.md Phase 1.
Per Franz 2026-05-18: "python + migration" path (not SQL trigger), matches
Codify v1 discipline (per-batch DB apply approval).

USAGE

  # bridge specific operator(s):
  python scripts/bridge_operator_to_cascade.py --operator-ids 7 --label phase1_hbdb

  # bridge all operators that have both extraction + beach_operator link:
  python scripts/bridge_operator_to_cascade.py --all-bridgable --label phase1_full

  # dry-run + report what WOULD bridge:
  python scripts/bridge_operator_to_cascade.py --all-bridgable --dry-run

  # tune confidence:
  python scripts/bridge_operator_to_cascade.py --all-bridgable --confidence-threshold 0.5

OUTPUTS
  supabase/migrations/<date>_operate_phase1_<label>.sql  (idempotent migration)
  tmp/operate_phase1_<label>_<ts>_outcomes.jsonl         (per-operator outcome log)

Bridge mechanism:
  - Read operator_dogs_policy rows w/ pass_c_confidence >= threshold
  - For each: look up beach_operator links (the attribution path)
  - Skip operators with NO beach_operator rows (defer to Phase 2 backfill)
  - For each beach link: emit
      INSERT INTO policy_source (subtype='operator_posted_policy',
        issuing_operator_id=<op>, source_url=<extraction url>,
        full_text=<extraction summary + verbatim quotes>)
      INSERT INTO beach_policy_source (beach_fid, policy_source_id,
        section=<scope_section or 'sand'>, rule=<mapped from default_rule
        + leash_required>, evidence_verbatim=<pass_a_quotes[0]>, ...)
  - Emit ONE migration file; curator applies w/ per-batch approval per playbook §9

Rule mapping (operator_dogs_policy.default_rule + leash_required → enum):
  'restricted'/'yes' + leash_required=true   → on_leash
  'restricted'/'yes' + leash_required=false  → off_leash_voice_control
  'restricted'/'yes' + leash_required=null   → on_leash  (conservative)
  'no'/'not_allowed'                          → not_allowed
  'off_leash'                                 → off_leash
  'on_leash'                                  → on_leash
  unknown                                     → defer (no bps emit)
"""

from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import datetime
import json
import os
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

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


# ─── Rule mapping ─────────────────────────────────────────────────────

def map_rule(default_rule: str | None, leash_required: bool | None) -> str | None:
    """Map operator_dogs_policy.default_rule + leash_required to the
    beach_policy_source.rule enum. Returns None when un-mappable."""
    if default_rule is None:
        return None
    dr = default_rule.lower().strip()
    if dr in {"no", "not_allowed", "prohibited", "restricted_no_dogs"}:
        return "not_allowed"
    if dr in {"off_leash"}:
        return "off_leash"
    if dr in {"on_leash"}:
        return "on_leash"
    if dr in {"restricted", "yes", "allowed", "permitted"}:
        # Disambiguate via leash_required
        if leash_required is False:
            return "off_leash_voice_control"
        return "on_leash"  # conservative when null or true
    return None


# ─── Bridge primitives ────────────────────────────────────────────────

@dataclass
class BridgeOutcome:
    operator_id: int
    operator_name: str
    operator_type: str
    outcome: str               # success | defer_no_beach_links | defer_unmappable_rule | defer_low_conf
    n_beach_links: int
    rule: str | None
    citation: str | None
    confidence: float | None
    source_url: str | None
    notes: str | None


def fetch_bridgable(conn, confidence_threshold: float,
                    only_operator_ids: list[int] | None = None) -> list[dict]:
    """Return rows of (operator, best-extraction, beach-link-count).

    CORRECTED 2026-05-18: operator_dogs_policy.operator_id FK → public.operators
    (plural, 9,275 rows) NOT public.operator (singular, 153 rows). Previous
    version coincidentally cross-joined; this version uses the correct table
    and also probes for matching singular-operator rows by canonical_name (the
    bridge match candidate).
    """
    sql = """
        SELECT
            o.id AS operator_id, o.canonical_name AS operator_name,
            o.level AS operator_type, o.state_code, o.website, o.subtype,
            odp.source_url, odp.default_rule, odp.leash_required, odp.summary,
            odp.pass_a_quotes, odp.time_windows, odp.seasonal_closures,
            odp.spatial_zones, odp.pass_c_quotes, odp.pass_c_confidence,
            -- Bridge match: does a singular-operator row exist with same name?
            (SELECT op2.id FROM public.operator op2
             WHERE LOWER(op2.name) = LOWER(o.canonical_name)
             LIMIT 1) AS singular_op_id,
            (SELECT COUNT(*) FROM public.beach_operator bo
             JOIN public.operator op2 ON op2.id = bo.operator_id
             WHERE LOWER(op2.name) = LOWER(o.canonical_name)) AS n_beach_links
        FROM public.operators o
        JOIN public.operator_dogs_policy odp ON odp.operator_id = o.id
        WHERE odp.pass_c_confidence >= %s
    """
    params: list = [confidence_threshold]
    if only_operator_ids:
        sql += " AND o.id = ANY(%s)"
        params.append(list(only_operator_ids))
    sql += " ORDER BY o.id"
    with conn, conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def fetch_beach_links(conn, operator_id: int) -> list[dict]:
    """Return beach_operator rows for an operator."""
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT bo.beach_fid, bo.scope_section, bo.precedence_rank, bo.notes,
                   b.name AS beach_name
            FROM public.beach_operator bo
            JOIN public.beaches_gold b ON b.fid = bo.beach_fid
            WHERE bo.operator_id = %s
            ORDER BY b.name
        """, (operator_id,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ─── Migration SQL emit ────────────────────────────────────────────────

def esc(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def emit_operator_block(row: dict, beach_links: list[dict], rule: str) -> str:
    """Build a single per-operator SQL block: ps INSERT + bps INSERTs."""
    op_id = row["operator_id"]
    op_name = row["operator_name"]

    # Build a concise citation
    citation = (row.get("summary") or f"{op_name} posted policy")[:200]
    source_url = row["source_url"] or row.get("rules_url") or row.get("web_url") or ""

    # Extract a clean verbatim quote from pass_a_quotes
    pass_a_q = row.get("pass_a_quotes") or []
    if isinstance(pass_a_q, str):
        try:
            pass_a_q = json.loads(pass_a_q)
        except Exception:
            pass_a_q = [pass_a_q]
    evidence_verbatim = ""
    if pass_a_q and isinstance(pass_a_q, list) and pass_a_q:
        evidence_verbatim = str(pass_a_q[0])[:500]

    # Pull additional structured info into full_text for the consensus engine's
    # rendering pipeline + future temporal extractor
    pass_b_tw = row.get("time_windows") or []
    pass_b_sc = row.get("seasonal_closures") or []
    full_text_parts = [
        row.get("summary") or "",
        "",
        f"[Operator: {op_name} (id={op_id}); type={row['operator_type']}; "
        f"extraction pass_c confidence={row['pass_c_confidence']:.2f}]",
    ]
    if pass_b_tw:
        full_text_parts.append(f"\nTime windows: {json.dumps(pass_b_tw)[:500]}")
    if pass_b_sc:
        full_text_parts.append(f"\nSeasonal closures: {json.dumps(pass_b_sc)[:500]}")
    if pass_a_q:
        full_text_parts.append(f"\nVerbatim quotes (pass A):")
        for q in pass_a_q[:3]:
            full_text_parts.append(f"  - {str(q)[:400]}")
    full_text_parts.append(
        f"\n[Bridged 2026-05-18 via scripts/bridge_operator_to_cascade.py "
        f"from operator_dogs_policy.operator_id={op_id}.]"
    )
    full_text = "\n".join(full_text_parts)

    # Citation prefix for NOT EXISTS guard (first 60 chars)
    citation_prefix = citation[:60].replace("'", "''")

    parts = [
        f"-- ─── Operator: {op_name} (id={op_id}; type={row['operator_type']}) ───",
        f"-- rule={rule}  conf={row['pass_c_confidence']:.2f}  beach_links={len(beach_links)}",
        f"-- source_url={source_url}",
        "",
    ]
    # policy_source insert
    parts.append(
        f"INSERT INTO public.policy_source\n"
        f"  (subtype, citation, issuing_operator_id, scope, source_url, full_text)\n"
        f"SELECT 'operator_posted_policy',\n"
        f"       {esc(citation)},\n"
        f"       {op_id},\n"
        f"       ARRAY['dog_policy']::text[],\n"
        f"       {esc(source_url)},\n"
        f"       {esc(full_text)}\n"
        f"WHERE NOT EXISTS (\n"
        f"  SELECT 1 FROM public.policy_source\n"
        f"   WHERE issuing_operator_id = {op_id}\n"
        f"     AND subtype = 'operator_posted_policy'\n"
        f"     AND source_url = {esc(source_url)}\n"
        f");\n"
    )
    # bps inserts (one per beach_operator link)
    for bl in beach_links:
        section = bl.get("scope_section") or "sand"
        parts.append(
            f"INSERT INTO public.beach_policy_source\n"
            f"  (beach_fid, policy_source_id, section, rule, operative_status,\n"
            f"   evidence_verbatim, evidence_url, extracted_at, last_verified)\n"
            f"SELECT {bl['beach_fid']}, ps.id, {esc(section)}, {esc(rule)},\n"
            f"       'operative'::operative_status,\n"
            f"       {esc(evidence_verbatim)},\n"
            f"       ps.source_url, NOW(), NOW()\n"
            f"FROM public.policy_source ps\n"
            f"WHERE ps.issuing_operator_id = {op_id}\n"
            f"  AND ps.subtype = 'operator_posted_policy'\n"
            f"  AND ps.source_url = {esc(source_url)}\n"
            f"ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;\n"
        )
    return "\n".join(parts) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Operate Phase 1 bridge — operator extraction → cascade")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--operator-ids", help="Comma-separated operator IDs to bridge")
    g.add_argument("--all-bridgable", action="store_true",
                   help="Bridge ALL operators with pass_c_confidence >= threshold AND ≥1 beach_operator link")
    ap.add_argument("--confidence-threshold", type=float, default=0.7,
                    help="Min pass_c_confidence (default 0.7 matches Codify v1 auto-commit threshold)")
    ap.add_argument("--label", default="phase1_bridge",
                    help="Migration label")
    ap.add_argument("--out-dir", default="supabase/migrations",
                    help="Output dir for migration SQL")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what WOULD bridge; do not write migration")
    ap.add_argument("--overwrite", action="store_true",
                    help="Overwrite migration file if exists")
    args = ap.parse_args()

    only_ids: list[int] | None = None
    if args.operator_ids:
        only_ids = [int(x.strip()) for x in args.operator_ids.split(",") if x.strip()]

    conn = _connect_pg()
    try:
        candidates = fetch_bridgable(conn, args.confidence_threshold, only_ids)
        print(f"Found {len(candidates)} extraction row(s) at conf >= {args.confidence_threshold}")

        outcomes: list[BridgeOutcome] = []
        sql_blocks: list[str] = []

        for row in candidates:
            op_id = row["operator_id"]              # plural-operator id (extraction-side)
            singular_op_id = row.get("singular_op_id")
            op_name = row["operator_name"]
            op_type = row["operator_type"]
            n_links = row["n_beach_links"]

            # Filter: bridge requires a matching singular-operator row (per the
            # two-table architecture clarified 2026-05-18). For plural-only ops,
            # defer to Phase 1d (create singular row first; PSHDB pattern) or
            # Phase 2 (beach_operator backfill).
            if singular_op_id is None:
                outcomes.append(BridgeOutcome(
                    op_id, op_name, op_type, "defer_no_singular_match",
                    0, None, None, row["pass_c_confidence"], row["source_url"],
                    "no singular-operator row matches this plural operator by canonical_name; needs Phase 1d curator-create"))
                print(f"  defer  pluralop={op_id:<5} {op_name:<55} (no singular-operator match by name)")
                continue

            # Filter: skip if singular operator exists but has no beach links
            if n_links == 0:
                outcomes.append(BridgeOutcome(
                    op_id, op_name, op_type, "defer_no_beach_links",
                    0, None, None, row["pass_c_confidence"], row["source_url"],
                    f"singular_op_id={singular_op_id} has no beach_operator links; needs Phase 2 backfill"))
                print(f"  defer  pluralop={op_id:<5} {op_name:<55} (singular={singular_op_id}, no beach_operator links)")
                continue

            # Use singular_op_id as the FK target for emit
            row["operator_id"] = singular_op_id  # rebind for emit_operator_block

            # Map rule
            rule = map_rule(row["default_rule"], row["leash_required"])
            if rule is None:
                outcomes.append(BridgeOutcome(
                    op_id, op_name, op_type, "defer_unmappable_rule",
                    n_links, None, None, row["pass_c_confidence"], row["source_url"],
                    f"default_rule={row['default_rule']!r} not in mapping table"))
                print(f"  defer  op={op_id:<4} {op_name:<55} (unmappable default_rule={row['default_rule']!r})")
                continue

            # Get beach links
            beach_links = fetch_beach_links(conn, op_id)

            # Emit
            sql_block = emit_operator_block(row, beach_links, rule)
            sql_blocks.append(sql_block)
            outcomes.append(BridgeOutcome(
                op_id, op_name, op_type, "success",
                n_links, rule, (row.get("summary") or f"{op_name} posted policy")[:80],
                row["pass_c_confidence"], row["source_url"],
                None))
            print(f"  ok     op={op_id:<4} {op_name:<55} rule={rule:<24} n_beaches={n_links}")

        # Output
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        date_str = datetime.date.today().strftime("%Y%m%d")
        tmp_dir = ROOT / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = tmp_dir / f"operate_{args.label}_{ts}_outcomes.jsonl"
        # Custom encoder: psycopg2 returns Decimal for numeric columns;
        # serialize as float for JSONL compatibility.
        from decimal import Decimal
        def _enc(o):
            if isinstance(o, Decimal):
                return float(o)
            raise TypeError(f"not JSON serializable: {type(o).__name__}")
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for o in outcomes:
                f.write(json.dumps(o.__dict__, default=_enc) + "\n")
        print(f"\nOutcomes log: {jsonl_path}")

        # Tally
        tally: dict[str, int] = {}
        for o in outcomes:
            tally[o.outcome] = tally.get(o.outcome, 0) + 1
        print("\n=== OUTCOMES ===")
        for k in sorted(tally):
            print(f"  {k:<32} {tally[k]}")

        if args.dry_run:
            print(f"\n[dry-run] {len(sql_blocks)} SQL block(s) would be written.")
            return 0

        if not sql_blocks:
            print("\nNo SQL emitted (no bridgable operators in this run).")
            return 0

        out_dir = ROOT / args.out_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{date_str}_operate_{args.label}.sql"
        if out_path.exists() and not args.overwrite:
            print(f"\nERROR: {out_path} already exists. Use --overwrite.", file=sys.stderr)
            return 6

        # Write migration with header + transactional wrap
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"-- Operate Phase 1 bridge — label={args.label} ts={ts}\n")
            f.write(f"-- {len(sql_blocks)} operator(s) bridged from operator_dogs_policy → policy_source\n")
            f.write(f"-- Per docs/operate_pipeline_spec.md Phase 1.\n")
            f.write(f"-- Confidence threshold: {args.confidence_threshold}\n")
            f.write(f"--\n")
            f.write(f"-- Idempotent: NOT EXISTS guard on ps (issuing_operator_id + subtype + source_url);\n")
            f.write(f"-- ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING on bps.\n")
            f.write(f"-- Per playbook tenet #5: cascade fires automatically on bps INSERT.\n\n")
            f.write("BEGIN;\n\n")
            for block in sql_blocks:
                f.write(block)
                f.write("\n")
            f.write("COMMIT;\n")
        print(f"\nMigration written: {out_path}")
        print(f"  {len(sql_blocks)} ps row(s) + ~{sum(o.n_beach_links for o in outcomes if o.outcome == 'success')} bps row(s) expected")
        print(f"\nNext steps:")
        print(f"  1. Review: cat {out_path}")
        print(f"  2. Quality gates (read-only)")
        print(f"  3. Apply via psql with per-batch approval (per playbook §9)")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
