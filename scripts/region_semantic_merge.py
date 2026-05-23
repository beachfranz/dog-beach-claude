"""region_semantic_merge.py — LLM-driven canonical-region rewrite.

Per Franz 2026-05-21, Phase 3b (Class C residue cleanup).

After Phase 3 collapsed strict-key duplicates, some beaches still have
multiple region_name variants that describe the SAME geographic area
but slipped through because the rules / temporal windows differ
slightly. Type case: Del Mar Dog Beach fid 8560 — 5 sand rows that
should be 3 distinct carve-outs (one duplicate pair was identical
clauses extracted twice; one set has no temporal vs full temporal).

Pipeline:
  1. SELECT beaches with >=2 distinct region_name values (still named
     after P3). Per beach, fetch the unique region_names + their
     sample evidence quote.
  2. Send list to Sonnet: "group these region_names by geographic
     area, one group per distinct place at this beach. Return JSON."
  3. Apply each group:
     - If the beach has only ONE effective group after merge → set
       all rows' region_name = NULL (they fold into default).
     - If multiple groups → set each group's rows' region_name to the
       SHORTEST member name as canonical.
  4. Within a group, conflict-aware DELETE losers (mirror Phase 3
     migration pattern: pick keeper, move bps_temporal, delete loser).
  5. Re-cascade affected fids.

Usage:
  python scripts/region_semantic_merge.py --fid 8560     # one beach
  python scripts/region_semantic_merge.py --all          # all
  python scripts/region_semantic_merge.py --all --dry-run
"""
from __future__ import annotations
import sys, os, json, time, argparse
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]

import psycopg2.extras
from anthropic import Anthropic

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect


SYSTEM = """You group region descriptions from a beach's policy data.

For ONE beach, you receive a list of region_name strings (and a sample
evidence quote per region). Some regions describe the SAME physical
area using different wording (e.g. "Between 25th Street and 29th
Street extended westward" and "Between 25th Street and 29th Street
westerly prolongations" are the same place). Group them.

Return JSON ONLY in this shape:
{
  "groups": [
    {
      "label": "short canonical label (5-30 chars) for this geographic area",
      "members": ["exact region_name string", "exact region_name string", ...]
    },
    ...
  ]
}

Rules:
- Each input region_name must appear in exactly ONE group's members.
- Use the exact original strings in members[] (preserve casing/punctuation).
- A group can have 1 member (a region that's geographically unique).
- "label" is a NEW short label you choose; it must be a substring or
  obvious paraphrase of the longest member. Optimize for readability.
- Group by GEOGRAPHIC AREA, not by rule. Two regions describing the
  same place with different rules still belong in one group.
- Conservative bias: when uncertain, KEEP regions in separate groups."""




def fetch_target_beaches(cur) -> list[tuple[int, str]]:
    cur.execute("""
      SELECT bps.beach_fid, bg.name
        FROM beach_policy_source bps
        JOIN beaches_gold bg ON bg.fid = bps.beach_fid
       WHERE bps.region_name IS NOT NULL
       GROUP BY bps.beach_fid, bg.name
      HAVING COUNT(DISTINCT bps.region_name) >= 2
       ORDER BY bps.beach_fid
    """)
    return cur.fetchall()


def fetch_regions_for_beach(cur, fid: int) -> list[dict]:
    cur.execute("""
      SELECT DISTINCT ON (bps.region_name)
             bps.region_name, bps.section, bps.rule,
             LEFT(COALESCE(bps.evidence_verbatim, ''), 200) AS evidence_snippet
        FROM beach_policy_source bps
       WHERE bps.beach_fid = %s
         AND bps.region_name IS NOT NULL
       ORDER BY bps.region_name, bps.id
    """, (fid,))
    return [{'region_name': r[0], 'section': r[1], 'rule': r[2], 'evidence': r[3]}
            for r in cur.fetchall()]


def ask_llm(cli: Anthropic, beach_name: str, regions: list[dict]) -> dict:
    user = f"BEACH: {beach_name}\n\nREGIONS:\n"
    for i, r in enumerate(regions, 1):
        user += f"\n{i}. {r['region_name']!r}\n   ({r['section']} / {r['rule']})\n"
        if r['evidence']:
            user += f"   evidence: {r['evidence']}\n"
    user += "\nGroup these into geographic areas. Return JSON."
    msg = cli.messages.create(
        model='claude-sonnet-4-6', max_tokens=2048,
        system=SYSTEM,
        messages=[{'role': 'user', 'content': user}],
    )
    text = msg.content[0].text.strip()
    if text.startswith('```'):
        text = text.split('```', 2)[1]
        if text.startswith('json'): text = text[4:]
        text = text.rsplit('```', 1)[0].strip()
    return {'parsed': json.loads(text),
            'in_tokens': msg.usage.input_tokens,
            'out_tokens': msg.usage.output_tokens}


def apply_groupings(cur, fid: int, groups: list[dict], dry_run: bool) -> dict:
    """For each group: pick canonical (NULL if only one group at this
    beach, else shortest member); merge all member rows in bps to that
    region_name; conflict-aware delete losers + move temporals."""
    counts = {'updates': 0, 'deletes': 0, 'temporal_moves': 0}
    one_group_only = len(groups) == 1

    for grp in groups:
        members = grp.get('members') or []
        if not members: continue
        canonical = None if one_group_only else min(members, key=len)

        # All bps row ids that have a region_name in this group
        cur.execute("""
          SELECT id, region_name, policy_source_id, section, rule
            FROM beach_policy_source
           WHERE beach_fid = %s AND region_name = ANY(%s)
           ORDER BY id
        """, (fid, members))
        rows = cur.fetchall()
        if not rows: continue

        # Group by (policy_source_id, section, rule) — the unique-index key.
        # Within each unique-key bucket, pick keeper + delete losers.
        buckets: dict[tuple, list[tuple]] = {}
        for r in rows:
            buckets.setdefault((r[2], r[3], r[4]), []).append(r)

        for (ps_id, section, rule), bucket in buckets.items():
            # Keeper: prefer existing canonical match, else min id.
            keeper = sorted(bucket, key=lambda r: (r[1] != canonical, r[0]))[0]
            keeper_id = keeper[0]
            loser_ids = [r[0] for r in bucket if r[0] != keeper_id]

            if not dry_run:
                # Move temporal rows from losers to keeper (ON CONFLICT DO NOTHING).
                for lid in loser_ids:
                    cur.execute("""
                      UPDATE beach_policy_source_temporal SET bps_id = %s WHERE bps_id = %s
                    """, (keeper_id, lid))
                    counts['temporal_moves'] += cur.rowcount
                # Delete losers.
                if loser_ids:
                    cur.execute("DELETE FROM beach_policy_source WHERE id = ANY(%s)", (loser_ids,))
                    counts['deletes'] += len(loser_ids)
                # Update keeper's region_name to canonical (NULL or shortest).
                if keeper[1] != canonical:
                    cur.execute("UPDATE beach_policy_source SET region_name = %s WHERE id = %s",
                                (canonical, keeper_id))
                    counts['updates'] += 1
            else:
                counts['deletes'] += len(loser_ids)
                if keeper[1] != canonical: counts['updates'] += 1

    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument('--fid', type=int)
    grp.add_argument('--all', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, default=None,
                    help='process only first N target beaches (for testing)')
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor()
    cli = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])

    if args.fid:
        cur.execute("SELECT fid, name FROM beaches_gold WHERE fid=%s", (args.fid,))
        targets = [cur.fetchone()]
    else:
        targets = fetch_target_beaches(cur)
        if args.limit: targets = targets[:args.limit]
    print(f"Targets: {len(targets)} beaches\n")

    totals = {'beaches': 0, 'llm_ok': 0, 'llm_fail': 0,
              'updates': 0, 'deletes': 0, 'temporal_moves': 0,
              'recascade_ok': 0, 'in_tokens': 0, 'out_tokens': 0}

    for i, (fid, name) in enumerate(targets, 1):
        regions = fetch_regions_for_beach(cur, fid)
        if len(regions) < 2:
            print(f'  [{i}/{len(targets)}] fid={fid} {name!r}: <2 regions, skip')
            continue
        print(f'  [{i}/{len(targets)}] fid={fid} {name!r}: {len(regions)} regions → LLM')
        try:
            out = ask_llm(cli, name, regions)
            totals['in_tokens']  += out['in_tokens']
            totals['out_tokens'] += out['out_tokens']
            groups = out['parsed'].get('groups') or []
            totals['llm_ok'] += 1
        except Exception as e:
            print(f'    LLM FAIL: {e}')
            totals['llm_fail'] += 1
            continue

        # Validate: every input region_name appears in exactly one group
        all_members = [m for g in groups for m in (g.get('members') or [])]
        input_names = [r['region_name'] for r in regions]
        missing = set(input_names) - set(all_members)
        extra = set(all_members) - set(input_names)
        if missing or extra:
            print(f'    SCHEMA MISMATCH: missing={missing} extra={extra} — skip')
            totals['llm_fail'] += 1
            continue

        counts = apply_groupings(cur, fid, groups, args.dry_run)
        totals['updates']        += counts['updates']
        totals['deletes']        += counts['deletes']
        totals['temporal_moves'] += counts['temporal_moves']
        totals['beaches']        += 1

        # Re-cascade this beach
        if not args.dry_run:
            try:
                cur.execute("SELECT public._promote_zone_rules_for_fid(%s)", (fid,))
                conn.commit()
                totals['recascade_ok'] += 1
            except Exception as e:
                conn.rollback()
                print(f'    re-cascade fail: {e}')

        gn = len(groups)
        print(f'    → {gn} groups, {counts["deletes"]} deletes, {counts["updates"]} updates,'
              f' {counts["temporal_moves"]} temporals moved')

    cost = totals['in_tokens'] * 3e-6 + totals['out_tokens'] * 15e-6
    print()
    print('=== SUMMARY ===')
    print(f'  beaches processed: {totals["beaches"]}')
    print(f'  LLM ok / fail:     {totals["llm_ok"]} / {totals["llm_fail"]}')
    print(f'  updates:           {totals["updates"]}')
    print(f'  deletes:           {totals["deletes"]}')
    print(f'  temporals moved:   {totals["temporal_moves"]}')
    print(f'  re-cascaded:       {totals["recascade_ok"]}')
    print(f'  tokens:            in={totals["in_tokens"]:,} out={totals["out_tokens"]:,}'
          f'  est cost=${cost:.3f}')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
