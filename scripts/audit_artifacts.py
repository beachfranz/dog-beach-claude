"""audit_artifacts.py — produce a single markdown report of every artifact in
the system + what references it. From the report, mark each item ACTIVE /
LEGACY / ORPHAN / AMBIGUOUS, then act in a separate session.

Inventory:
  * DB:    tables, views, functions, triggers, cron jobs (system catalogs)
  * Code:  edge functions, python scripts, HTML pages (filesystem grep)

Cross-reference detection:
  * pg_depend for DB→DB dependencies (reliable)
  * pg_proc.prosrc text-search for SQL function bodies (catches dynamic SQL)
  * cron.job.command text-search for cron→fn references
  * grep filesystem for code→DB and code→code references

Status classification:
  * ACTIVE       — reachable (transitively) from a seed
  * LEGACY       — seeded as legacy or only reachable from legacy
  * ORPHAN       — not reachable from any seed
  * AMBIGUOUS    — referenced via dynamic / hard-to-resolve patterns

Output: audit/audit_YYYY-MM-DD.md (committable, diffable across runs).

Run:
  python scripts/audit_artifacts.py
  python scripts/audit_artifacts.py --report-only   # skip DB queries; reuse cached graph
"""
from __future__ import annotations
import argparse
import io
import json
import os
import re
import sys
import urllib.parse
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")


# ============================================================================
# Seed lists — define what's known-active. The reachability search starts here.
# ============================================================================

# Cron jobs that ARE the daily/weekly schedule (their bodies define what's live)
ACTIVE_CRON_JOBS = {
    "daily_beach_refresh_nightly",
    "hourly-beach-now-refresh",
    "weekly_tide_refresh",
    "verdict_cascade_nightly",
}

# Edge functions called from production HTML pages or cron
ACTIVE_EDGE_FNS = {
    "daily-beach-refresh", "get-beach-now",
    "get-beach-summary", "get-beach-detail", "get-beaches-find", "get-beach-compare",
    "beach-chat", "get-calendar-event",
    "admin-update-beach-dog-policy", "admin-update-beach", "admin-update-beach-amenities",
    "admin-update-beaches-gold", "admin-move-beach-point", "admin-update-off-leash-beach",
    "send-daily-alerts",
}

# HTML pages currently linked from CLAUDE.md or part of the consumer surface
ACTIVE_HTML = {
    "index.html", "detail.html", "find.html", "paywall.html", "compare.html",
    "admin/zone-rules-editor.html",
    "admin/pipeline-monitor.html",
    "admin/gold-set-curator-v3.html",
    "admin/beach-editor-gold.html",
    "admin/dogs-allowed-audit.html",
    "admin/active-map.html",
    "admin/find.html",
    "admin/index.html",
    "admin/daily-refresh.html",
    "admin/dedupe-review.html",
    "admin/auto-polygon-review.html",
}

# Functions that are explicitly active even if pg_depend doesn't catch them
ACTIVE_FUNCTIONS = {
    "compute_beach_field_consensus", "promote_canonical_dogs_to_beach_dog_policy",
    "promote_canonical_practical_to_beach_amenities",
    "promote_canonical_to_consumer_tables",
    "_resolve_dogs_gold", "_resolve_field_group_gold",
    "_consensus_normalize", "_calibration_weight", "_wilson_lower_bound",
    "_norm_dogs_allowed", "_norm_leash_policy", "_norm_bool", "_norm_time_text",
    "_emit_evidence_from_cpad_unit_dogs_policy",
    "_emit_evidence_from_pad_us_dogs_policy",
    "_emit_evidence_from_operator_dogs_policy",
    "_emit_evidence_from_zone_rules_derived",
    "_emit_evidence_from_zone_rules_practical",
    "_emit_evidence_from_osm_amenities",
    "find_beaches", "increment_chat_rate",
    "_recompute_is_scoreable_for_fid",
    "_make_location_slug",
    "_fire_daily_beach_refresh", "_fire_weekly_tide_refresh",
    "tg_promote_dogs_chain", "tg_auto_scoreable_socal_gold",
    "tg_backfill_scoring_metadata_for_socal",
    "tg_recompute_scoreable_on_dog_policy",
}

# Tables that are CANONICAL data sources — always considered active even if
# nothing actively reads them (they're inputs that get queried ad-hoc).
ACTIVE_DATA_TABLES = {
    "beaches_gold", "beach_dog_policy", "beach_amenities",
    "beach_day_hourly_scores", "beach_day_recommendations",
    "scoring_config", "beach_enrichment_provenance", "beach_field_consensus",
    "consensus_field_config", "field_source_calibration", "field_source_weight_cap",
    "beach_views",
    "arena", "us_beach_points",
    "cpad_units", "cpad_unit_dogs_policy", "cpad_unit_policy_exceptions",
    "pad_us_units", "pad_us_unit_dogs_policy",
    "wildlife_critical_habitat", "dog_policy_zones", "osm_amenities",
    "operators", "operator_dogs_policy", "operator_policy_exceptions",
    "noaa_stations", "counties", "states", "jurisdictions",
    "csp_parks", "nps_places", "tribal_lands", "military_bases", "mpas",
    "ccc_access_points", "osm_features",
    "subscribers", "subscriber_locations", "notification_log",
    "chat_rate_limits", "refresh_errors", "admin_audit", "admin_rate_limits",
    "pipeline_runs",
}

# Tables/functions explicitly known to be LEGACY (parity-only or superseded)
LEGACY_KNOWN = {
    "compute_dogs_verdict_core", "compute_dogs_verdict_ccc_friendly",
    "compute_dogs_verdict_by_origin", "recompute_all_dogs_verdicts_by_origin",
    "beach_locations",   # legacy 805-view per CLAUDE.md
    "beach_verdicts",    # parity output
    "us_beach_points",   # superseded by beaches_gold per CLAUDE.md
    "public.beaches",    # dropped 2026-05-02 (ghost references should surface here)
}


# ============================================================================
# Inventory: pull artifacts from DB + filesystem
# ============================================================================
@dataclass
class Artifact:
    kind: str       # 'table' | 'view' | 'function' | 'trigger' | 'cron' |
                    # 'edge_fn' | 'python' | 'html' | 'migration'
    name: str
    body: str = ""               # SQL body / file contents, used for grep
    file_path: str | None = None
    metadata: dict = field(default_factory=dict)
    references: set[tuple[str, str]] = field(default_factory=set)   # (kind, name)
    referenced_by: set[tuple[str, str]] = field(default_factory=set)
    status: str = "UNCLASSIFIED"
    notes: list[str] = field(default_factory=list)


def fetch_db_artifacts() -> list[Artifact]:
    """Pull tables, views, functions, triggers, cron jobs from system catalogs."""
    artifacts: list[Artifact] = []
    with psycopg2.connect(**PG) as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Tables
        cur.execute("""
            select table_name from information_schema.tables
             where table_schema='public' and table_type='BASE TABLE'
             order by table_name
        """)
        for r in cur.fetchall():
            artifacts.append(Artifact(kind="table", name=r["table_name"]))

        # Views
        cur.execute("""
            select table_name, view_definition from information_schema.views
             where table_schema='public' order by table_name
        """)
        for r in cur.fetchall():
            artifacts.append(Artifact(kind="view", name=r["table_name"], body=r["view_definition"] or ""))

        # Functions — skip extension-owned (postgis, pgcrypto, etc.)
        cur.execute("""
            select p.proname, p.prosrc
              from pg_proc p
              join pg_namespace n on n.oid = p.pronamespace
              left join pg_depend d
                on d.objid = p.oid and d.deptype = 'e'
             where n.nspname = 'public'
               and p.prokind in ('f','p')
               and d.objid is null              -- exclude extension-owned
             order by p.proname
        """)
        for r in cur.fetchall():
            artifacts.append(Artifact(kind="function", name=r["proname"],
                                       body=(r["prosrc"] or "")))

        # Triggers
        cur.execute("""
            select t.tgname, c.relname as table_name, p.proname as function_name
              from pg_trigger t
              join pg_class c on c.oid = t.tgrelid
              join pg_proc p on p.oid = t.tgfoid
              join pg_namespace n on n.oid = c.relnamespace
             where n.nspname='public' and not t.tgisinternal
             order by t.tgname
        """)
        for r in cur.fetchall():
            a = Artifact(kind="trigger", name=r["tgname"],
                         metadata={"table": r["table_name"], "function": r["function_name"]})
            a.references.add(("table",    r["table_name"]))
            a.references.add(("function", r["function_name"]))
            artifacts.append(a)

        # Cron jobs
        try:
            cur.execute("select jobname, schedule, command from cron.job order by jobname")
            for r in cur.fetchall():
                artifacts.append(Artifact(kind="cron", name=r["jobname"],
                                           body=r["command"],
                                           metadata={"schedule": r["schedule"]}))
        except psycopg2.errors.UndefinedTable:
            pass

    return artifacts


def fetch_file_artifacts() -> list[Artifact]:
    """Walk filesystem for edge functions, python scripts, HTML pages."""
    artifacts: list[Artifact] = []

    # Edge functions: supabase/functions/<name>/index.ts
    fn_root = ROOT / "supabase" / "functions"
    if fn_root.exists():
        for fn_dir in sorted(fn_root.iterdir()):
            if not fn_dir.is_dir() or fn_dir.name.startswith("_"):
                continue
            index_ts = fn_dir / "index.ts"
            if index_ts.exists():
                try:
                    body = index_ts.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    body = ""
                artifacts.append(Artifact(kind="edge_fn", name=fn_dir.name,
                                           body=body, file_path=str(index_ts.relative_to(ROOT))))

    # Python scripts: scripts/**/*.py
    scripts_root = ROOT / "scripts"
    if scripts_root.exists():
        for py in sorted(scripts_root.rglob("*.py")):
            try:
                body = py.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                body = ""
            rel = py.relative_to(ROOT)
            artifacts.append(Artifact(kind="python", name=rel.name,
                                       body=body, file_path=str(rel)))

    # HTML pages: top-level + admin/
    for pat in ["*.html", "admin/*.html"]:
        for h in sorted(ROOT.glob(pat)):
            try:
                body = h.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                body = ""
            rel = h.relative_to(ROOT)
            artifacts.append(Artifact(kind="html", name=str(rel),
                                       body=body, file_path=str(rel)))

    return artifacts


# ============================================================================
# Cross-reference detection — text search of bodies for known names
# ============================================================================
def build_reference_index(artifacts: list[Artifact]) -> None:
    """For each artifact, find what it references. Mutate artifacts in place."""
    by_kn: dict[tuple[str, str], Artifact] = {(a.kind, a.name): a for a in artifacts}

    # Pre-compile regex per artifact name (escape special chars)
    table_names    = {a.name for a in artifacts if a.kind == "table"}
    view_names     = {a.name for a in artifacts if a.kind == "view"}
    function_names = {a.name for a in artifacts if a.kind == "function"}
    edge_fn_names  = {a.name for a in artifacts if a.kind == "edge_fn"}

    # Word-boundary regex per name; faster to bulk-scan with one alternation per kind
    def alt(names: Iterable[str]) -> re.Pattern | None:
        names = [re.escape(n) for n in names if len(n) >= 4]   # skip 1-3 char names (false-positive prone)
        if not names: return None
        return re.compile(r"\b(" + "|".join(sorted(names, key=len, reverse=True)) + r")\b")

    re_tables    = alt(table_names)
    re_views     = alt(view_names)
    re_functions = alt(function_names)
    re_edge_fns  = alt(edge_fn_names)

    for a in artifacts:
        if not a.body: continue
        body = a.body

        # Skip self-references
        def found(pat: re.Pattern | None, target_kind: str):
            if pat is None: return
            for m in pat.finditer(body):
                target_name = m.group(1)
                if (target_kind, target_name) == (a.kind, a.name):
                    continue
                a.references.add((target_kind, target_name))

        found(re_tables, "table")
        found(re_views,  "view")
        found(re_functions, "function")
        found(re_edge_fns, "edge_fn")

    # Build inverse index
    for a in artifacts:
        for (kind, name) in a.references:
            target = by_kn.get((kind, name))
            if target is not None:
                target.referenced_by.add((a.kind, a.name))


# ============================================================================
# pg_depend overlay — catches view/function/trigger dependencies pg's parser
# can resolve cleanly (no false positives from text grep).
# ============================================================================
def overlay_pg_depend(artifacts: list[Artifact]) -> None:
    by_kn = {(a.kind, a.name): a for a in artifacts}
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("""
            select
              dep_n.nspname as dep_schema, dep_c.relname as dep_name, dep_c.relkind as dep_kind,
              ref_n.nspname as ref_schema, ref_c.relname as ref_name, ref_c.relkind as ref_kind
              from pg_depend d
              join pg_class dep_c on dep_c.oid = d.objid
              join pg_namespace dep_n on dep_n.oid = dep_c.relnamespace
              join pg_class ref_c on ref_c.oid = d.refobjid
              join pg_namespace ref_n on ref_n.oid = ref_c.relnamespace
             where d.deptype in ('n','a')
               and dep_n.nspname = 'public'
               and ref_n.nspname = 'public'
               and dep_c.relname <> ref_c.relname
        """)
        kind_map = {'r': 'table', 'v': 'view', 'm': 'view'}
        for row in cur.fetchall():
            dep_n_, dep_name, dep_relkind, ref_n_, ref_name, ref_relkind = row
            dep_kind = kind_map.get(dep_relkind)
            ref_kind = kind_map.get(ref_relkind)
            if not dep_kind or not ref_kind: continue
            src = by_kn.get((dep_kind, dep_name))
            tgt = by_kn.get((ref_kind, ref_name))
            if src and tgt:
                src.references.add((tgt.kind, tgt.name))
                tgt.referenced_by.add((src.kind, src.name))


# ============================================================================
# Classification — BFS from seeds, mark active; LEGACY for known-legacy seeds
# ============================================================================
def classify(artifacts: list[Artifact]) -> None:
    by_kn = {(a.kind, a.name): a for a in artifacts}

    # Seed set
    seeds: set[tuple[str, str]] = set()
    for n in ACTIVE_CRON_JOBS:    seeds.add(("cron",     n))
    for n in ACTIVE_EDGE_FNS:     seeds.add(("edge_fn",  n))
    for n in ACTIVE_HTML:         seeds.add(("html",     n))
    for n in ACTIVE_FUNCTIONS:    seeds.add(("function", n))
    for n in ACTIVE_DATA_TABLES:  seeds.add(("table",    n))

    # Seed triggers on active tables — a trigger is "active" iff it fires on an active table.
    # The trigger's function is then reachable via the trigger's references.
    for a in artifacts:
        if a.kind == "trigger":
            tbl = a.metadata.get("table")
            if ("table", tbl) in seeds:
                seeds.add(("trigger", a.name))

    # BFS — mark all reachable as ACTIVE
    queue = deque(seeds)
    visited = set()
    while queue:
        kn = queue.popleft()
        if kn in visited: continue
        visited.add(kn)
        a = by_kn.get(kn)
        if a is None: continue
        for child in a.references:
            if child not in visited:
                queue.append(child)

    for kn in visited:
        a = by_kn.get(kn)
        if a is not None:
            a.status = "ACTIVE"

    # Python scripts: pure callers (nothing imports them in the running pipeline);
    # mark as AMBIGUOUS rather than ORPHAN so they don't drown the report.
    # Also surface their referenced tables/functions/edge_fns separately.
    for a in artifacts:
        if a.kind == "python" and a.status == "UNCLASSIFIED":
            a.status = "AMBIGUOUS"

    # Mark known-legacy
    for name in LEGACY_KNOWN:
        for kind in ("function", "view", "table"):
            a = by_kn.get((kind, name.replace("public.", "")))
            if a is not None:
                a.status = "LEGACY"

    # Anything not classified = ORPHAN
    for a in artifacts:
        if a.status == "UNCLASSIFIED":
            a.status = "ORPHAN"


# ============================================================================
# Markdown report
# ============================================================================
def render_report(artifacts: list[Artifact]) -> str:
    out = []
    out.append(f"# Artifact audit — {date.today().isoformat()}")
    out.append("")
    out.append("Generated by `scripts/audit_artifacts.py`. Each artifact's "
               "status is inferred from a reachability search starting at "
               "seeds (cron jobs, consumer edge fns, HTML pages, canonical "
               "data tables). False positives are tolerable; false negatives "
               "(marking something dead that's used) are not, so seeds are "
               "permissive.")
    out.append("")

    # Aggregate summary
    out.append("## Summary")
    out.append("")
    out.append("| Kind | ACTIVE | LEGACY | ORPHAN | AMBIGUOUS | Total |")
    out.append("|---|---:|---:|---:|---:|---:|")
    counts = defaultdict(lambda: defaultdict(int))
    for a in artifacts:
        counts[a.kind][a.status] += 1
        counts[a.kind]["TOTAL"] += 1
    for kind in sorted(counts.keys()):
        c = counts[kind]
        out.append(f"| {kind} | {c['ACTIVE']} | {c['LEGACY']} | {c['ORPHAN']} | {c['AMBIGUOUS']} | {c['TOTAL']} |")
    out.append("")

    # ORPHAN list — the kill candidates
    out.append("## ORPHAN — kill candidates")
    out.append("")
    out.append("These are unreferenced from any seed-reachable surface. "
               "Review each before deleting; some may be referenced via "
               "dynamic SQL or rare scripts not in the seed list.")
    out.append("")
    orphans = [a for a in artifacts if a.status == "ORPHAN"]
    by_kind = defaultdict(list)
    for a in orphans:
        by_kind[a.kind].append(a)
    for kind in sorted(by_kind.keys()):
        out.append(f"### Orphan {kind}s ({len(by_kind[kind])})")
        out.append("")
        for a in sorted(by_kind[kind], key=lambda x: x.name):
            referenced_count = len(a.referenced_by)
            ref_note = f"  — referenced by {referenced_count} (cycle?)" if referenced_count else ""
            file_note = f" `{a.file_path}`" if a.file_path else ""
            out.append(f"- `{a.name}`{file_note}{ref_note}")
        out.append("")

    # LEGACY list
    out.append("## LEGACY — intentionally kept for parity / history")
    out.append("")
    legacies = [a for a in artifacts if a.status == "LEGACY"]
    for a in sorted(legacies, key=lambda x: (x.kind, x.name)):
        ref_count = len(a.referenced_by)
        out.append(f"- `{a.kind}:{a.name}` — referenced by {ref_count} other artifacts")
    out.append("")

    # ACTIVE — full per-artifact detail
    out.append("## ACTIVE — full reference list")
    out.append("")
    actives = [a for a in artifacts if a.status == "ACTIVE"]
    by_kind_active = defaultdict(list)
    for a in actives:
        by_kind_active[a.kind].append(a)

    for kind in sorted(by_kind_active.keys()):
        out.append(f"### {kind}s ({len(by_kind_active[kind])} active)")
        out.append("")
        for a in sorted(by_kind_active[kind], key=lambda x: x.name):
            file_note = f" `{a.file_path}`" if a.file_path else ""
            out.append(f"#### `{a.name}`{file_note}")
            if a.metadata:
                meta_str = ", ".join(f"{k}={v}" for k, v in a.metadata.items())
                out.append(f"- meta: {meta_str}")
            if a.references:
                refs_by_kind = defaultdict(list)
                for (k, n) in sorted(a.references):
                    refs_by_kind[k].append(n)
                out.append(f"- references:")
                for k in sorted(refs_by_kind):
                    out.append(f"  - {k}: {', '.join(refs_by_kind[k][:25])}"
                               + (f" (+{len(refs_by_kind[k]) - 25} more)" if len(refs_by_kind[k]) > 25 else ""))
            if a.referenced_by:
                rb_by_kind = defaultdict(list)
                for (k, n) in sorted(a.referenced_by):
                    rb_by_kind[k].append(n)
                out.append(f"- referenced by:")
                for k in sorted(rb_by_kind):
                    out.append(f"  - {k}: {', '.join(rb_by_kind[k][:25])}"
                               + (f" (+{len(rb_by_kind[k]) - 25} more)" if len(rb_by_kind[k]) > 25 else ""))
            out.append("")

    return "\n".join(out)


# ============================================================================
# CLI
# ============================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "audit" / f"audit_{date.today().isoformat()}.md"),
                    help="Markdown output path")
    args = ap.parse_args()

    print("=== Audit: pulling DB catalogs ===")
    db_arts = fetch_db_artifacts()
    print(f"  {sum(1 for a in db_arts if a.kind=='table')} tables, "
          f"{sum(1 for a in db_arts if a.kind=='view')} views, "
          f"{sum(1 for a in db_arts if a.kind=='function')} functions, "
          f"{sum(1 for a in db_arts if a.kind=='trigger')} triggers, "
          f"{sum(1 for a in db_arts if a.kind=='cron')} cron jobs")

    print("=== Audit: walking filesystem ===")
    file_arts = fetch_file_artifacts()
    print(f"  {sum(1 for a in file_arts if a.kind=='edge_fn')} edge fns, "
          f"{sum(1 for a in file_arts if a.kind=='python')} python scripts, "
          f"{sum(1 for a in file_arts if a.kind=='html')} html pages")

    artifacts = db_arts + file_arts
    print("=== Building reference index (text-grep) ===")
    build_reference_index(artifacts)
    print("=== Overlaying pg_depend ===")
    overlay_pg_depend(artifacts)
    print("=== Classifying ===")
    classify(artifacts)

    out_path = Path(args.out)
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(render_report(artifacts), encoding="utf-8")
    print(f"\nReport written to {out_path}")

    # Stdout summary
    counts = defaultdict(lambda: defaultdict(int))
    for a in artifacts:
        counts[a.kind][a.status] += 1
        counts[a.kind]["TOTAL"] += 1
    print(f"\n{'Kind':<12} {'ACTIVE':>8} {'LEGACY':>8} {'ORPHAN':>8} {'AMBIG':>8} {'Total':>8}")
    for kind in sorted(counts.keys()):
        c = counts[kind]
        print(f"{kind:<12} {c['ACTIVE']:>8} {c['LEGACY']:>8} {c['ORPHAN']:>8} {c['AMBIGUOUS']:>8} {c['TOTAL']:>8}")


if __name__ == "__main__":
    main()
