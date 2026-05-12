"""
quality_check_anchors.py — coverage + classification audit for curated dog beaches.

Loads YAML fixtures from `tests/quality_anchors/*.yaml`, matches each anchor
against `beaches_gold` via trigram similarity (scoped by county), and reports:

  * OK                    — match found, scoreable, classification matches expected
  * MISSING               — no match above 0.50 sim
  * NAME_MISMATCH         — match found but sim 0.50-0.85 (worth verifying)
  * NOT_SCOREABLE         — match found but is_scoreable=false
  * CLASSIFICATION_DRIFT  — match found but dogs_allowed/leash_policy disagrees
  * NO_DOG_POLICY         — match found, scoreable, but no beach_dog_policy row
                            (consensus hasn't promoted yet)

Tier semantics (per fixture):
  * Tier 1 — marquee, must be correct. Exit 1 if any non-OK.
  * Tier 2 — regional anchor. Warn on non-OK; exit 0.
  * Tier 3 — long-tail. Coverage-only check (no expected verdict).

Usage:
  python scripts/quality_check_anchors.py
  python scripts/quality_check_anchors.py --tier 1                  # only Tier 1
  python scripts/quality_check_anchors.py --region SoCal            # one region
  python scripts/quality_check_anchors.py --json                    # machine output
  python scripts/quality_check_anchors.py --report-out audit/foo.md # write report

Outputs:
  * Markdown report to audit/quality_anchors_<date>.md by default
  * Console summary
  * Exit code 0 if all Tier 1 OK; 1 otherwise
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import sys
import urllib.parse
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

FIXTURE_DIR = ROOT / "tests" / "quality_anchors"
DEFAULT_REPORT = ROOT / "audit" / f"quality_anchors_{dt.date.today().isoformat()}.md"

SIM_MIN_PRESENT  = 0.50  # below this = MISSING
SIM_MIN_NAME_OK  = 0.85  # below this (but >= MIN_PRESENT) = NAME_MISMATCH


def load_fixtures(region_filter: str | None) -> list[dict]:
    """Load all *.yaml fixtures from FIXTURE_DIR, optionally filter by region."""
    anchors: list[dict] = []
    for path in sorted(FIXTURE_DIR.glob("*.yaml")):
        with path.open("r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        if not doc or "anchors" not in doc:
            continue
        meta = doc.get("metadata", {})
        if region_filter and meta.get("region") != region_filter:
            continue
        for entry in doc["anchors"]:
            entry["_fixture"] = path.name
            entry["_region"] = meta.get("region", path.stem)
            anchors.append(entry)
    return anchors


def match_anchor(cur, anchor: dict) -> dict:
    """Find the best beaches_gold match for an anchor and classify it."""
    name = anchor["name"]
    county = anchor.get("county")
    aliases = anchor.get("aliases") or []
    candidates_raw = [name] + aliases

    cur.execute(
        """
        with candidates(label) as (
          select unnest(%s::text[])
        ),
        ranked as (
          select bg.fid, bg.name, bg.county_name, bg.is_scoreable,
                 coalesce(bg.display_name_override, bg.name) as display_name,
                 bdp.dogs_allowed, bdp.leash_policy, bdp.off_leash_flag,
                 bdp.consensus_confidence, bdp.disagreement_flag,
                 max(similarity(lower(bg.name), lower(c.label))) as sim
          from public.beaches_gold bg
          left join public.beach_dog_policy bdp on bdp.arena_group_id = bg.fid
          cross join candidates c
          where (%s::text is null or bg.county_name = %s)
            and similarity(lower(bg.name), lower(c.label)) > 0.20
          group by bg.fid, bg.name, bg.county_name, bg.is_scoreable,
                   bg.display_name_override, bdp.dogs_allowed, bdp.leash_policy,
                   bdp.off_leash_flag, bdp.consensus_confidence, bdp.disagreement_flag
        )
        select * from ranked order by sim desc, fid asc limit 1;
        """,
        (candidates_raw, county, county),
    )
    row = cur.fetchone()
    if not row:
        return {"status": "MISSING", "match": None}

    sim = float(row["sim"])
    if sim < SIM_MIN_PRESENT:
        return {"status": "MISSING", "match": dict(row), "sim": sim}

    base = {
        "fid": row["fid"],
        "matched_name": row["display_name"],
        "county_name": row["county_name"],
        "is_scoreable": row["is_scoreable"],
        "dogs_allowed": row["dogs_allowed"],
        "leash_policy": row["leash_policy"],
        "off_leash_flag": row["off_leash_flag"],
        "consensus_confidence": float(row["consensus_confidence"]) if row["consensus_confidence"] is not None else None,
        "disagreement_flag": row["disagreement_flag"],
        "sim": sim,
    }

    if sim < SIM_MIN_NAME_OK:
        return {"status": "NAME_MISMATCH", "match": base}
    if not row["is_scoreable"]:
        return {"status": "NOT_SCOREABLE", "match": base}
    if row["dogs_allowed"] is None and row["leash_policy"] is None:
        return {"status": "NO_DOG_POLICY", "match": base}

    expected = anchor.get("expected") or {}
    if not expected:
        # Tier 3 / coverage-only — present + scoreable is enough
        return {"status": "OK", "match": base}

    drift = []
    if "dogs_allowed" in expected and expected["dogs_allowed"] != row["dogs_allowed"]:
        drift.append(f"dogs_allowed: expected={expected['dogs_allowed']!r} actual={row['dogs_allowed']!r}")
    if "leash_policy" in expected and expected["leash_policy"] != row["leash_policy"]:
        drift.append(f"leash_policy: expected={expected['leash_policy']!r} actual={row['leash_policy']!r}")
    if "off_leash_flag" in expected and expected["off_leash_flag"] != row["off_leash_flag"]:
        drift.append(f"off_leash_flag: expected={expected['off_leash_flag']!r} actual={row['off_leash_flag']!r}")

    if drift:
        return {"status": "CLASSIFICATION_DRIFT", "match": base, "drift": drift}
    return {"status": "OK", "match": base}


def render_markdown(anchors: list[dict], results: list[dict]) -> str:
    """Render a tier-grouped markdown report."""
    lines: list[str] = []
    today = dt.date.today().isoformat()
    lines.append(f"# Quality Anchors Audit — {today}\n")
    lines.append(f"Total anchors: **{len(anchors)}**\n")

    # Tier rollup
    by_tier: dict[int, dict[str, int]] = {}
    for a, r in zip(anchors, results):
        t = a.get("tier", 99)
        by_tier.setdefault(t, {"OK": 0, "MISSING": 0, "NAME_MISMATCH": 0,
                              "NOT_SCOREABLE": 0, "NO_DOG_POLICY": 0,
                              "CLASSIFICATION_DRIFT": 0})
        by_tier[t][r["status"]] = by_tier[t].get(r["status"], 0) + 1

    lines.append("## Summary by Tier\n")
    lines.append("| Tier | OK | MISSING | NAME_MISMATCH | NOT_SCOREABLE | NO_DOG_POLICY | DRIFT |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|")
    for t in sorted(by_tier.keys()):
        b = by_tier[t]
        lines.append(f"| {t} | {b['OK']} | {b['MISSING']} | {b['NAME_MISMATCH']} | "
                     f"{b['NOT_SCOREABLE']} | {b['NO_DOG_POLICY']} | {b['CLASSIFICATION_DRIFT']} |")
    lines.append("")

    # Per-anchor detail, grouped by tier
    for tier in sorted(by_tier.keys()):
        tier_label = {1: "Tier 1 — Marquee (must-be-correct)",
                      2: "Tier 2 — Regional anchors",
                      3: "Tier 3 — Long-tail (coverage-only)"}.get(tier, f"Tier {tier}")
        lines.append(f"## {tier_label}\n")
        lines.append("| Anchor | County | Status | fid | Matched | dogs / leash / off | Notes |")
        lines.append("|---|---|---|---:|---|---|---|")
        for a, r in zip(anchors, results):
            if a.get("tier") != tier:
                continue
            m = r.get("match") or {}
            status_emoji = {
                "OK": "✅",
                "MISSING": "❌ MISSING",
                "NAME_MISMATCH": "⚠️ NAME",
                "NOT_SCOREABLE": "⚠️ NOT_SCOREABLE",
                "NO_DOG_POLICY": "⚠️ NO_POLICY",
                "CLASSIFICATION_DRIFT": "🔴 DRIFT",
            }.get(r["status"], r["status"])
            fid_str = str(m.get("fid")) if m.get("fid") else "—"
            matched_str = m.get("matched_name") or "—"
            verdict = (
                f"{m.get('dogs_allowed') or '—'} / "
                f"{m.get('leash_policy') or '—'} / "
                f"{m.get('off_leash_flag') if m.get('off_leash_flag') is not None else '—'}"
            )
            note = ""
            if r["status"] == "CLASSIFICATION_DRIFT":
                note = "; ".join(r.get("drift", []))
            elif r["status"] == "NAME_MISMATCH":
                note = f"sim={m.get('sim'):.2f}"
            lines.append(f"| {a['name']} | {a.get('county') or '—'} | {status_emoji} | {fid_str} | "
                         f"{matched_str} | {verdict} | {note} |")
        lines.append("")

    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", type=int, help="Only check this tier")
    ap.add_argument("--region", type=str, help="Only this region (e.g., SoCal)")
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of markdown")
    ap.add_argument("--report-out", type=str, default=str(DEFAULT_REPORT),
                    help=f"Write markdown report here (default: {DEFAULT_REPORT.relative_to(ROOT)})")
    args = ap.parse_args()

    anchors = load_fixtures(args.region)
    if args.tier:
        anchors = [a for a in anchors if a.get("tier") == args.tier]
    if not anchors:
        print("No anchors matched the filter.", file=sys.stderr)
        return 0

    print(f"Checking {len(anchors)} anchor(s)...", file=sys.stderr)

    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    results: list[dict] = []
    for a in anchors:
        results.append(match_anchor(cur, a))

    if args.json:
        # Tag each result with the anchor metadata for machine consumption
        out = []
        for a, r in zip(anchors, results):
            out.append({
                "name": a["name"],
                "county": a.get("county"),
                "state": a.get("state"),
                "tier": a.get("tier"),
                "fixture": a["_fixture"],
                **r,
            })
        print(json.dumps({"checked_at": dt.datetime.utcnow().isoformat(),
                          "results": out}, indent=2, default=str))
    else:
        md = render_markdown(anchors, results)
        report_path = Path(args.report_out)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(md, encoding="utf-8")
        print(md)
        print(f"\n📄 Report written to {report_path}", file=sys.stderr)

    # Console summary
    counts = {"OK": 0, "MISSING": 0, "NAME_MISMATCH": 0, "NOT_SCOREABLE": 0,
              "NO_DOG_POLICY": 0, "CLASSIFICATION_DRIFT": 0}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    print(f"\nSummary: {counts['OK']} OK, "
          f"{counts['MISSING']} MISSING, "
          f"{counts['NAME_MISMATCH']} NAME, "
          f"{counts['NOT_SCOREABLE']} NOT_SCOREABLE, "
          f"{counts['NO_DOG_POLICY']} NO_POLICY, "
          f"{counts['CLASSIFICATION_DRIFT']} DRIFT", file=sys.stderr)

    # Exit code: non-zero if any Tier 1 anchor isn't OK
    tier_1_problems = sum(
        1 for a, r in zip(anchors, results)
        if a.get("tier") == 1 and r["status"] != "OK"
    )
    if tier_1_problems:
        print(f"❌ {tier_1_problems} Tier 1 anchor(s) not OK — exit code 1", file=sys.stderr)
        return 1
    print("✅ All Tier 1 anchors OK", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
