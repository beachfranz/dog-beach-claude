"""Dry run: simulate Phase B of the dog_policy_exceptions migration.

Phase B = rewrite writers to INSERT directly into dog_policy_exceptions
table instead of writing to the parent jsonb columns.

This script does NOT touch the database. It:
  1. Re-runs the merge logic from scripts/one_off/merge_operator_dogs_policy.py
     (without UPSERT)
  2. Re-derives operator exception rows that the rewrite would emit
  3. Compares against the current dog_policy_exceptions table state
  4. Reports diff: missing rows, extra rows, value mismatches

If the diff is empty (or nearly so), Phase B is safe — the rewritten
writers produce the same data the sync trigger has already populated
into the table.

Usage:
  python scripts/one_off/dry_run_exceptions_to_table.py
"""
from __future__ import annotations
import os
import sys
from collections import defaultdict
from pathlib import Path

# Path setup so we can reuse merge logic
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "one_off"))

from dotenv import load_dotenv
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

# Reach into Supabase via psycopg2 directly (sidesteps PostgREST envelope)
import psycopg2
import urllib.parse

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG_KW = dict(host=p.hostname, port=p.port or 5432,
             user=p.username, password=os.environ["SUPABASE_DB_PASSWORD"],
             dbname=(p.path or "/postgres").lstrip("/"))


def fetch_extractions():
    sql = """
        select operator_id, source_url, pass_c_status, pass_c_confidence,
               pass_c_exceptions, extracted_at
          from public.operator_policy_extractions
    """
    with psycopg2.connect(**PG_KW) as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_current_table_rows():
    """Current state of dog_policy_exceptions WHERE source_kind='operator'."""
    sql = """
        select source_id as operator_id, beach_name, rule, source_quote, source_url
          from public.dog_policy_exceptions
         where source_kind = 'operator'
    """
    with psycopg2.connect(**PG_KW) as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [c.name for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return {(r["operator_id"], r["beach_name"]): r for r in rows}


def merge_exceptions_for_operator(rows: list[dict]) -> list[dict]:
    """Mirrors merge_operator_dogs_policy.merge_exceptions, but emits
    flat (operator_id, beach_name, rule, source_quote, source_url)
    tuples instead of a jsonb array."""
    seen = {}
    for r in rows:
        if r.get("pass_c_status") != "ok":
            continue
        excs = r.get("pass_c_exceptions") or []
        if isinstance(excs, str):
            import json
            excs = json.loads(excs)
        for item in excs:
            name = (item.get("beach_name") or "").strip()
            if not name:
                continue
            if name in seen:
                # Mirrors merge_exceptions: keep first
                continue
            seen[name] = {
                "operator_id":  r["operator_id"],
                "beach_name":   name,
                "rule":         item.get("rule"),
                "source_quote": item.get("source_quote"),
                "source_url":   item.get("source_url"),
            }
    return list(seen.values())


def main():
    print("Fetching operator_policy_extractions...")
    extractions = fetch_extractions()
    print(f"  {len(extractions)} rows")

    print("Fetching current dog_policy_exceptions (operator)...")
    current = fetch_current_table_rows()
    print(f"  {len(current)} rows")

    # Group extractions by operator
    by_op = defaultdict(list)
    for r in extractions:
        by_op[r["operator_id"]].append(r)

    # Compute what Phase B would emit
    proposed = []
    for op_id, rows in by_op.items():
        proposed.extend(merge_exceptions_for_operator(rows))
    proposed_index = {(r["operator_id"], r["beach_name"]): r for r in proposed}

    print(f"\n=== DRY RUN ===")
    print(f"Phase B would emit:    {len(proposed)} operator exception rows")
    print(f"Currently in table:    {len(current)} operator exception rows")
    print()

    # Diff: rows in current but not proposed (would be DELETED if Phase B replaces all)
    missing_in_proposed = set(current) - set(proposed_index)
    # Rows in proposed but not current (would be INSERTED)
    extra_in_proposed   = set(proposed_index) - set(current)
    # Rows in both — check for value differences
    common = set(current) & set(proposed_index)
    value_mismatches = []
    for k in common:
        c, p = current[k], proposed_index[k]
        for f in ("rule", "source_quote", "source_url"):
            if (c.get(f) or "") != (p.get(f) or ""):
                value_mismatches.append((k, f, c.get(f), p.get(f)))
                break

    print(f"Missing from Phase B (would be removed): {len(missing_in_proposed)}")
    for k in sorted(missing_in_proposed)[:5]:
        print(f"  - operator_id={k[0]}, beach_name={k[1]!r}")
    if len(missing_in_proposed) > 5: print(f"  ... and {len(missing_in_proposed)-5} more")

    print(f"\nNew in Phase B (would be added): {len(extra_in_proposed)}")
    for k in sorted(extra_in_proposed)[:5]:
        print(f"  + operator_id={k[0]}, beach_name={k[1]!r}, rule={proposed_index[k]['rule']}")
    if len(extra_in_proposed) > 5: print(f"  ... and {len(extra_in_proposed)-5} more")

    print(f"\nValue mismatches in shared rows: {len(value_mismatches)}")
    for (k, f, c, p) in value_mismatches[:5]:
        print(f"  {k} field={f}: current={c!r} -> proposed={p!r}")
    if len(value_mismatches) > 5: print(f"  ... and {len(value_mismatches)-5} more")

    print()
    if not (missing_in_proposed or extra_in_proposed or value_mismatches):
        print("[PASS] Phase B output matches current table exactly. Safe to migrate.")
    else:
        print("[NOTE] Phase B output differs from current. Review diff above.")


if __name__ == "__main__":
    main()
