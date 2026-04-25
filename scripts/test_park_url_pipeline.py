"""
test_park_url_pipeline.py
-------------------------
Golden-output regression test for the park_url evidence/resolve/promote
pipeline. Locks the current state of ~20 fixture beaches as a baseline,
then on every subsequent run diffs current state vs golden. Catches
behavioral regressions in populate_from_park_url() (and its helpers).

Two modes:
    python scripts/test_park_url_pipeline.py --record
        Captures current DB state for fixture beaches -> tests/golden_park_url_pipeline.json
        Use after intentional behavior changes to update the baseline.

    python scripts/test_park_url_pipeline.py
        Reads golden file, re-queries DB for the same fixtures, diffs.
        Exits 0 if identical, 1 if any diffs (with detail printed).

Fixture set covers the patterns we care about:
    • multi_cpad_disagreement (Mickey's, Stinson, Coronado Dog, Mission Beach, Carpinteria)
    • source_governing_mismatch (Wavecrest, Coronado Dog, Stinson)
    • buffer-rescue + page-confirms governance promotion (Carpinteria)
    • environmental_overlay fallback (San Simeon, Santa Rosa Creek, WRH Memorial SB)
    • clean canonical (Commons Beach Tahoe, others)
    • single-CPAD with rich practical data
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

# Fixture beach fids — chosen to exercise every code path in the pipeline.
# Don't change these IDs without re-recording the golden file.
FIXTURE_FIDS = [
    1093168,   # Natural Bridges SB     — multi_cpad: Antonelli Pond vs NBSB; state-park override
    1163237,   # Morro Rock Beach       — multi_cpad: Coleman Park vs Morro Bay City Beach
    1620294,   # Mickey's Beach         — multi_cpad + source_mismatch (Mt Tam vs GGNRA)
    2052963,   # Morro Strand SB        — dogs disagreement + multi_cpad + source_mismatch
    2649621,   # Mission Bay Bike Path  — multi_cpad: Mission Bay Park vs Mission Beach Park
    3322659,   # Houghton M. Roberts    — multi_cpad: Monterey SB vs Roberts Lake Open Space
    4411973,   # Wavecrest Beach        — multi_cpad: HMB SB vs Poplar Beach
    6754273,   # WRH Memorial SB        — environmental_overlay (Cambria SMP demoted)
    8135145,   # Playa Del Rey Beach    — multi_cpad: Del Rey Lagoon vs Dockweiler SB
    8778653,   # Seaside Reef           — Cardiff SB vs San Elijo Lagoon ER
    8783604,   # Commons Beach (Tahoe)  — Burton Creek SP vs Commons Beach
    8912496,   # Mission Beach          — Mission Bay Park vs Mission Beach Park
    9031619,   # La Jolla Cove          — La Jolla Cove vs Ellen Browning Scripps
    11558454,  # Coronado Dog Beach     — multi_cpad + source_mismatch (Coronado Muni vs Sunset Park)
    12690329,  # Santa Rosa Creek Beach — env_overlay (Cambria SMP)
    13566763,  # Carpinteria City Beach — buffer-rescue page-confirms + state-park override
    16276554,  # Stinson Beach          — 3-way multi_cpad + source_mismatch (GGNRA + Mt Tam + Bolinas)
    17070553,  # Santa Monica Beach     — only beach with multi-containing CPAD
    20158735,  # San Simeon Beach       — env_overlay (Cambria SMP)
    # 6953070 omitted — not active/valid in locations_stage
]

GOLDEN_FILE = Path(__file__).parent.parent / "tests" / "golden_park_url_pipeline.json"


def fetch_state() -> list[dict]:
    """Run the golden-state SQL query via supabase CLI; return parsed list."""
    fids_csv = ",".join(str(f) for f in FIXTURE_FIDS)
    sql = f"""
    WITH fixture_fids AS (SELECT unnest(ARRAY[{fids_csv}]::int[]) AS fid),
    stage_state AS (
      SELECT s.fid, b.name,
        jsonb_build_object(
          'governing_body_name',  s.governing_body_name,
          'governing_body_type',  s.governing_body_type,
          'dogs_allowed',         s.dogs_allowed,
          'dogs_leash_required',  s.dogs_leash_required,
          'dogs_restricted_hours',s.dogs_restricted_hours,
          'dogs_seasonal_rules',  s.dogs_seasonal_rules,
          'dogs_zone_description',s.dogs_zone_description,
          'has_parking',          s.has_parking,
          'parking_type',         s.parking_type,
          'parking_notes',        s.parking_notes,
          'has_restrooms',        s.has_restrooms,
          'has_showers',          s.has_showers,
          'has_drinking_water',   s.has_drinking_water,
          'has_lifeguards',       s.has_lifeguards,
          'has_disabled_access',  s.has_disabled_access,
          'has_food',             s.has_food,
          'has_fire_pits',        s.has_fire_pits,
          'has_picnic_area',      s.has_picnic_area,
          'review_status',        s.review_status,
          'review_notes',         s.review_notes
        ) AS stage_data
      FROM fixture_fids f
      JOIN public.locations_stage s USING (fid)
      JOIN public.us_beach_points b USING (fid)
    ),
    evidence_state AS (
      SELECT e.fid, jsonb_agg(
        jsonb_build_object(
          'field_group',     e.field_group,
          'source',          e.source,
          'source_url',      e.source_url,
          'cpad_unit_name',  e.cpad_unit_name,
          'extraction_type', e.extraction_type,
          'cpad_role',       e.cpad_role,
          'is_canonical',    e.is_canonical,
          'confidence',      e.confidence,
          'claimed_values',  e.claimed_values
        ) ORDER BY e.field_group, e.source, e.source_url
      ) AS evidence
      FROM public.beach_enrichment_provenance e
      WHERE e.fid IN (SELECT fid FROM fixture_fids)
        AND e.source IN ('park_url','park_url_buffer_attribution')
      GROUP BY e.fid
    )
    SELECT jsonb_agg(jsonb_build_object(
      'fid', ss.fid,
      'name', ss.name,
      'stage', ss.stage_data,
      'evidence', COALESCE(es.evidence, '[]'::jsonb)
    ) ORDER BY ss.fid) AS golden
    FROM stage_state ss
    LEFT JOIN evidence_state es USING (fid);
    """
    proc = subprocess.run(
        ['supabase', 'db', 'query', '--linked'],
        input=sql, capture_output=True, text=True, timeout=120,
    )
    if proc.returncode != 0:
        print(f"SQL query failed: {proc.stderr}", file=sys.stderr)
        sys.exit(2)
    # The CLI wraps results in {"rows": [...], "boundary": ...}. Extract.
    out = proc.stdout
    s, e = out.find('{'), out.rfind('}')
    parsed = json.loads(out[s:e+1])
    rows = parsed.get('rows', [])
    if not rows or rows[0].get('golden') is None:
        return []
    return rows[0]['golden']


def diff_states(golden: list[dict], current: list[dict]) -> list[str]:
    """Return a list of human-readable diff descriptions. Empty list = no drift."""
    diffs: list[str] = []
    by_fid_golden = {row['fid']: row for row in golden}
    by_fid_current = {row['fid']: row for row in current}

    only_in_golden = set(by_fid_golden) - set(by_fid_current)
    only_in_current = set(by_fid_current) - set(by_fid_golden)
    for fid in sorted(only_in_golden):
        diffs.append(f"fid={fid} {by_fid_golden[fid]['name']!r}: in golden but missing from current")
    for fid in sorted(only_in_current):
        diffs.append(f"fid={fid} {by_fid_current[fid]['name']!r}: in current but missing from golden")

    for fid in sorted(set(by_fid_golden) & set(by_fid_current)):
        g = by_fid_golden[fid]
        c = by_fid_current[fid]
        name = g['name']

        # Stage diffs
        for k in g['stage'].keys() | c['stage'].keys():
            gv = g['stage'].get(k)
            cv = c['stage'].get(k)
            if gv != cv:
                diffs.append(f"fid={fid} {name!r} stage.{k}: golden={gv!r} current={cv!r}")

        # Evidence diffs (compare full sorted lists)
        if g['evidence'] != c['evidence']:
            # Compute deeper diff at evidence-row level
            g_keys = {(r['field_group'], r['source'], r['source_url']): r for r in g['evidence']}
            c_keys = {(r['field_group'], r['source'], r['source_url']): r for r in c['evidence']}

            for k in sorted(g_keys.keys() - c_keys.keys()):
                diffs.append(f"fid={fid} {name!r} evidence: golden has {k} not in current")
            for k in sorted(c_keys.keys() - g_keys.keys()):
                diffs.append(f"fid={fid} {name!r} evidence: current has {k} not in golden")
            for k in sorted(g_keys.keys() & c_keys.keys()):
                gr, cr = g_keys[k], c_keys[k]
                for field in ('cpad_unit_name', 'extraction_type', 'cpad_role',
                              'is_canonical', 'confidence', 'claimed_values'):
                    if gr.get(field) != cr.get(field):
                        diffs.append(
                            f"fid={fid} {name!r} evidence{k}.{field}: "
                            f"golden={gr.get(field)!r} current={cr.get(field)!r}"
                        )
    return diffs


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--record', action='store_true',
                   help='Capture current DB state as the new golden baseline')
    args = p.parse_args()

    current = fetch_state()
    if not current:
        print("No state returned from DB — fixture fids missing?", file=sys.stderr)
        return 2
    print(f"Fetched state for {len(current)} fixture beaches")

    if args.record:
        GOLDEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_FILE.write_text(json.dumps(current, indent=2, sort_keys=True), encoding='utf-8')
        print(f"Wrote golden baseline -> {GOLDEN_FILE}")
        return 0

    if not GOLDEN_FILE.exists():
        print(f"No golden file at {GOLDEN_FILE}. Run with --record first.", file=sys.stderr)
        return 2
    golden = json.loads(GOLDEN_FILE.read_text(encoding='utf-8'))

    diffs = diff_states(golden, current)
    if not diffs:
        print(f"PASS — no drift across {len(current)} fixture beaches")
        return 0
    print(f"FAIL — {len(diffs)} differences vs golden:")
    for d in diffs:
        print(f"  {d}")
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
