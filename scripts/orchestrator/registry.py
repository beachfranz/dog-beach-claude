"""Pipeline registry — single source of truth for "what scripts exist + how to run them."

Edit this dict to add a pipeline. Keys are stable identifiers stored in
`pipeline_runs.pipeline_name`; the admin UI reads from here too (via
the JSON sibling at scripts/orchestrator/registry.json).

When you change this file, run `python -m scripts.orchestrator.export_registry`
from the repo root to refresh registry.json so admin/pipelines.html
sees the latest list.
"""
from __future__ import annotations
from typing import TypedDict


class Pipeline(TypedDict, total=False):
    description: str          # one-line human description
    command: list[str]        # subprocess argv (relative to repo root)
    schedule: str | None      # cron-ish hint ("@daily" / "0 3 * * *" / None)
    cost_estimate: str        # "$0.05" / "$0" / "varies — see --help"
    cost_class: str           # 'free' / 'cheap' / 'medium' / 'expensive'
    depends_on: list[str]     # pipeline names this should run after
    notes: str                # operational notes (when to run, gotchas)
    cwd: str                  # working dir override (default: repo root)


PIPELINES: dict[str, Pipeline] = {
    # ---- zone_rules ------------------------------------------------------
    'zone_rules_repass_anchors': {
        'description': 'Section-aware text-repass on 8 anchor beaches (smoke test)',
        'command': ['python', 'scripts/repass_zone_rules.py', '--anchors'],
        'schedule': None,
        'cost_estimate': '$0.05',
        'cost_class': 'cheap',
        'depends_on': [],
        'notes': 'Sanity check — runs the closed_zones_v3 prompt against the '
                 '8 hand-curated anchors. Auto-promotes via DB trigger to '
                 'beach_dog_policy.zone_rules.',
    },
    'zone_rules_repass_full': {
        'description': 'Full text-repass on all eligible beaches (~600)',
        'command': ['python', 'scripts/repass_zone_rules.py', '--full'],
        'schedule': None,
        'cost_estimate': '$3.00',
        'cost_class': 'expensive',
        'depends_on': [],
        'notes': 'Quarterly refresh. Re-extracts zone_rules over all beaches '
                 'with prose evidence + ambiguous allowed signal. Run after '
                 'major prompt or taxonomy changes.',
    },

    # ---- catalog ingest --------------------------------------------------
    'extract_beach_policies': {
        'description': 'Bulk LLM extraction over scoreable beaches missing '
                       'recent BEP rows',
        'command': ['python', 'scripts/extract_beach_policies.py', '--skip-existing'],
        'schedule': None,
        'cost_estimate': 'varies',
        'cost_class': 'medium',
        'depends_on': [],
        'notes': 'Run when adding new scoreable beaches or when prompt '
                 'variants change. Reads URL pool, fans out per beach.',
    },
    'extract_for_orphans': {
        'description': 'Per-orphan policy extraction (rare-source URLs)',
        'command': ['python', 'scripts/extract_for_orphans.py'],
        'schedule': None,
        'cost_estimate': 'varies',
        'cost_class': 'medium',
        'depends_on': [],
        'notes': 'Targets specific beaches lacking park_url evidence. '
                 'Use for surgical backfill, not bulk runs.',
    },
    'discover_urls': {
        'description': 'Brave/Anthropic URL discovery for orphan beaches',
        'command': ['python', 'scripts/discover_urls.py'],
        'schedule': None,
        'cost_estimate': 'varies',
        'cost_class': 'medium',
        'depends_on': [],
        'notes': 'Brave search + Anthropic fallback. Gate before extract_*.',
    },

    # ---- spatial sources -------------------------------------------------
    'load_osm_beach_polys': {
        'description': 'Refresh OSM natural=beach polygons via Overpass API',
        'command': ['python', 'scripts/load_osm_beach_polys.py'],
        'schedule': '0 3 1 * *',  # monthly, 1st @ 3am
        'cost_estimate': '$0',
        'cost_class': 'free',
        'depends_on': [],
        'notes': 'OSM doesnt change daily. Monthly is plenty.',
    },
    'load_dog_policy_zones': {
        'description': 'Refresh NPS Point Reyes pet-allowed carve-outs',
        'command': ['python', 'scripts/load_dog_policy_zones.py'],
        'schedule': None,
        'cost_estimate': '$0',
        'cost_class': 'free',
        'depends_on': [],
        'notes': 'Hand-curated 4 polygons. Re-run only when NPS changes '
                 'pet policies or we add more carve-outs.',
    },

    # ---- consumer scoring (already wired via pg_cron — surfaced here for visibility) ----
    'daily_beach_refresh': {
        'description': '7-day forecast refresh for all scoreable beaches',
        'command': ['echo', 'wired via pg_cron — edge fn daily-beach-refresh'],
        'schedule': '0 9 * * *',  # already running on this schedule
        'cost_estimate': '~$0.20/run',
        'cost_class': 'cheap',
        'depends_on': [],
        'notes': 'pg_cron job — runs nightly at 02:00 PT. Listed here for '
                 'lineage visibility; orchestrator does NOT manage it.',
    },
}


def list_names() -> list[str]:
    return list(PIPELINES.keys())


def get(name: str) -> Pipeline:
    if name not in PIPELINES:
        raise KeyError(f"Unknown pipeline: {name!r}. Known: {sorted(PIPELINES.keys())}")
    return PIPELINES[name]
