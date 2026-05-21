"""region_scope_filter.py — Class D scope mis-attachment cleanup.

Per Franz 2026-05-21. When one policy_source covers multiple sibling
beaches (e.g. Del Mar Municipal Code §4.08.020 covers Del Mar Dog
Beach AND main Del Mar Beach), the deep extractor attaches every
clause to every beach. Result: Del Mar Dog Beach (fid 8560) has bps
rows describing Powerhouse-29th and Tot Lot — areas that belong to
fid 8992, not 8560.

Type case:
  fid 8560 = "The North Beach Area of Del Mar, north of 29th Street"
  Mis-attached rows say "Between Powerhouse Park and 29th Street",
  "Tot Lot playground", "southern turf of Powerhouse Park" — these
  geographically don't apply to 8560.

Pipeline:
  1. For each (beach_fid, policy_source_id) pair where the beach has
     bps rows with multiple distinct region_name values:
  2. Pull beach.canonical_area_description (if set) + name + lat/lng
     + the list of distinct region descriptions on this beach.
  3. Ask Sonnet: which regions apply to THIS beach, which don't?
  4. DELETE non-matching bps rows from THIS beach (the matching beach's
     own pass will keep them; data isn't lost from the system).
  5. NULL region_name on matching rows (since the region IS the beach —
     Franz's insight: don't double-count the beach itself as a region).
  6. Re-cascade.

Usage:
  python scripts/region_scope_filter.py --fid 8560
  python scripts/region_scope_filter.py --all
  python scripts/region_scope_filter.py --all --dry-run
"""
from __future__ import annotations
import sys, os, json, argparse, urllib.parse
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
import truststore; truststore.inject_into_ssl()
from pathlib import Path
import psycopg2
from anthropic import Anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')


SYSTEM = """You judge whether each policy rule GEOGRAPHICALLY APPLIES to a
specific beach.

You receive:
  - A beach identity (name, lat/lng, optional canonical_area_description
    sentence describing its precise boundary)
  - A numbered list of rows extracted from one policy_source. Each row
    has: row number, optional region_name, section, rule, and the
    verbatim evidence quote that produced it.

For each row, decide whether this rule's geographic scope applies to
THIS specific beach.

Return JSON ONLY:
{
  "matches":        [row_number, row_number, ...],
  "does_not_match": [row_number, row_number, ...]
}

Rules:
- Every input row_number must appear in exactly one list.
- A row "matches" when its evidence either:
    (a) explicitly names this beach or a feature inside its area, OR
    (b) is jurisdiction-wide / generic (default leash law, nuisance,
        waste rules) — these apply to every beach in the jurisdiction.
- A row "does not match" when the evidence describes a SIBLING beach
  or sub-area that is geographically OUTSIDE this beach's identity
  (e.g. a Powerhouse Park rule shown to a beach north of 29th Street;
  a Tot Lot rule shown to a wild beach with no Tot Lot).
- Bias toward CONSERVATIVE: when in doubt, treat as match.
- Watch for "except as stated in Subsection B" type carve-outs — the
  base rule still applies to THIS beach unless the carve-out clearly
  exempts THIS beach's area."""


def _connect():
    pp = urllib.parse.urlparse((ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip())
    return psycopg2.connect(host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ['SUPABASE_DB_PASSWORD'],
        dbname=(pp.path or '/postgres').lstrip('/'), sslmode='require')


def fetch_targets(cur, fid: int | None) -> list[tuple]:
    """Returns (beach_fid, ps_id) pairs to process — any beach×source
    pair with at least one bps row (named OR NULL-region). Class D
    now evaluates NULL-region rows too (Franz 2026-05-21)."""
    if fid is not None:
        cur.execute("""SELECT beach_fid, policy_source_id, COUNT(*)
                         FROM beach_policy_source
                        WHERE beach_fid = %s
                        GROUP BY beach_fid, policy_source_id
                        ORDER BY policy_source_id""", (fid,))
    else:
        cur.execute("""SELECT beach_fid, policy_source_id, COUNT(*)
                         FROM beach_policy_source
                        GROUP BY beach_fid, policy_source_id
                       HAVING COUNT(*) >= 1
                        ORDER BY beach_fid, policy_source_id""")
    return cur.fetchall()


def fetch_beach_identity(cur, fid: int) -> dict:
    cur.execute("""SELECT name, lat, lon, canonical_area_description
                     FROM beaches_gold WHERE fid = %s""", (fid,))
    r = cur.fetchone()
    return {'name': r[0], 'lat': r[1], 'lng': r[2], 'desc': r[3]}


def fetch_rows(cur, fid: int, ps_id: int) -> list[dict]:
    """Returns ALL bps rows for this (beach, source) pair — both named
    and NULL-region. Caller passes row indices to LLM and looks up bps.id
    on the way back."""
    cur.execute("""SELECT id, region_name, section, rule,
                          LEFT(COALESCE(evidence_verbatim, ''), 280) AS evidence
                     FROM beach_policy_source
                    WHERE beach_fid = %s AND policy_source_id = %s
                    ORDER BY region_name NULLS FIRST, section, rule, id""",
                (fid, ps_id))
    return [{'id': r[0], 'region_name': r[1], 'section': r[2], 'rule': r[3],
             'evidence': r[4]} for r in cur.fetchall()]


def ask_llm(cli: Anthropic, beach: dict, rows: list[dict]) -> dict:
    user = f"BEACH: {beach['name']!r}\n"
    if beach.get('lat') is not None:
        user += f"COORDS: {beach['lat']:.4f}, {beach['lng']:.4f}\n"
    if beach.get('desc'):
        user += f"AREA: {beach['desc']}\n"
    user += "\nROWS FROM THIS POLICY SOURCE:\n"
    for i, r in enumerate(rows, 1):
        region_part = (f"region={r['region_name']!r}" if r['region_name']
                       else "region=(none — applies to default/whole-beach)")
        user += f"\nROW {i}: {region_part} section={r['section']} rule={r['rule']}\n"
        if r['evidence']:
            user += f"  evidence: {r['evidence'][:220]}\n"
    user += ("\nWhich row_numbers apply to THIS beach? Which describe a sibling area?\n"
             "RETURN ONLY THE JSON OBJECT — NO PROSE, NO REASONING, NO PREAMBLE. "
             "Start your response with `{`.")
    msg = cli.messages.create(
        model='claude-sonnet-4-6', max_tokens=2048,
        system=SYSTEM,
        messages=[{'role': 'user', 'content': user}],
    )
    raw = msg.content[0].text if msg.content else ''
    text = (raw or '').strip()
    # Strip ```json / ``` fences if present
    if text.startswith('```'):
        # find content between first ``` and last ```
        first = text.find('\n', 3)
        last  = text.rfind('```')
        if first >= 0 and last > first:
            text = text[first+1:last].strip()
    if not text:
        raise ValueError(f'empty LLM response (stop_reason={msg.stop_reason!r}, raw={raw!r})')
    # Try direct parse first; if it fails, extract first JSON object via regex.
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        import re
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if not m:
            raise ValueError(f'no JSON in response: cleaned text={text[:200]!r}; raw={raw[:300]!r}')
        try:
            parsed = json.loads(m.group(0))
        except json.JSONDecodeError as e:
            raise ValueError(f'json parse failed: {e}; extracted={m.group(0)[:200]!r}')
    return {'parsed': parsed,
            'in_tokens': msg.usage.input_tokens,
            'out_tokens': msg.usage.output_tokens}


def apply(cur, match_ids: list[int], non_match_ids: list[int],
          all_rows: list[dict], dry_run: bool) -> dict:
    """match_ids / non_match_ids are bps.id values. Non-matches get
    DELETED (with their temporals). Matches that have a non-null
    region_name get NULL'd, with conflict-aware DELETE-losers if
    another keeper exists at the same canonical key."""
    counts = {'deletes_non_match': 0, 'null_match': 0,
              'temporal_orphans_deleted': 0, 'collapse_deletes': 0}

    # 1. DELETE non-match rows + their temporals.
    if non_match_ids and not dry_run:
        cur.execute("DELETE FROM beach_policy_source_temporal WHERE bps_id = ANY(%s)",
                    (non_match_ids,))
        counts['temporal_orphans_deleted'] = cur.rowcount
        cur.execute("DELETE FROM beach_policy_source WHERE id = ANY(%s)", (non_match_ids,))
        counts['deletes_non_match'] = cur.rowcount
    elif non_match_ids:
        counts['deletes_non_match'] = len(non_match_ids)

    # 2. NULL region_name on matches that have one. Conflict-aware:
    # if multiple matches collide at the same (section, rule) when NULL'd,
    # keep one (prefer existing NULL, else min id), delete others +
    # move temporals to keeper.
    matches_with_region = [r for r in all_rows
                            if r['id'] in match_ids and r['region_name'] is not None]
    if matches_with_region and not dry_run:
        # Re-fetch the full set including any existing NULL-region rows
        # at this beach+source, to group correctly.
        fid = None; ps_id = None
        for r in all_rows:
            if r['id'] in match_ids:
                # We need fid+ps_id; derive from one of the rows. They're all
                # from the same (fid, ps_id) by construction (caller).
                # Easier: caller passes them in main(). For now, look up.
                break
        # All match rows + existing NULL rows
        match_set = set(match_ids)
        relevant = [r for r in all_rows
                    if r['id'] in match_set or r['region_name'] is None]
        buckets: dict[tuple, list[dict]] = {}
        for r in relevant:
            buckets.setdefault((r['section'], r['rule']), []).append(r)
        for (section, rule), bucket in buckets.items():
            keeper = sorted(bucket, key=lambda r: (r['region_name'] is not None, r['id']))[0]
            keeper_id = keeper['id']
            loser_ids = [r['id'] for r in bucket if r['id'] != keeper_id]
            for lid in loser_ids:
                cur.execute("""UPDATE beach_policy_source_temporal SET bps_id = %s
                                WHERE bps_id = %s""", (keeper_id, lid))
            if loser_ids:
                cur.execute("DELETE FROM beach_policy_source WHERE id = ANY(%s)",
                            (loser_ids,))
                counts['collapse_deletes'] += len(loser_ids)
            if keeper['region_name'] is not None:
                cur.execute("UPDATE beach_policy_source SET region_name = NULL WHERE id = %s",
                            (keeper_id,))
                counts['null_match'] += 1
    elif matches_with_region:
        counts['null_match'] = len(matches_with_region)
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument('--fid', type=int)
    grp.add_argument('--all', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, default=None)
    args = ap.parse_args()

    conn = _connect()
    cur = conn.cursor()
    cli = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])

    targets = fetch_targets(cur, args.fid if not args.all else None)
    if args.limit: targets = targets[:args.limit]
    print(f"Targets: {len(targets)} (beach,policy_source) pairs\n")

    totals = {'pairs': 0, 'llm_ok': 0, 'llm_fail': 0,
              'deletes_non_match': 0, 'null_match': 0, 'collapse_deletes': 0,
              'temporal_orphans': 0, 'recascade_ok': 0,
              'in_tokens': 0, 'out_tokens': 0,
              'affected_fids': set()}

    def reconnect():
        nonlocal conn, cur
        try: conn.close()
        except Exception: pass
        conn = _connect(); cur = conn.cursor()

    for i, (fid, ps_id, _) in enumerate(targets, 1):
        # pgbouncer drops idle connections after the LLM call; ping + reconnect.
        try:
            cur.execute("SELECT 1"); cur.fetchone()
        except Exception:
            reconnect()

        rows = fetch_rows(cur, fid, ps_id)
        if not rows:
            continue
        beach = fetch_beach_identity(cur, fid)
        print(f'  [{i}/{len(targets)}] fid={fid} ps={ps_id} {beach["name"]!r}: '
              f'{len(rows)} rows → LLM')
        try:
            out = ask_llm(cli, beach, rows)
            totals['in_tokens']  += out['in_tokens']
            totals['out_tokens'] += out['out_tokens']
            totals['llm_ok']     += 1
        except Exception as e:
            print(f'    LLM FAIL: {e}'); totals['llm_fail'] += 1; continue

        # LLM returns row_numbers (1-based indices); map back to bps.id
        try:
            match_ix    = [int(n) for n in (out['parsed'].get('matches') or [])]
            non_match_ix = [int(n) for n in (out['parsed'].get('does_not_match') or [])]
        except Exception:
            print(f'    PARSE FAIL: {out["parsed"]}'); totals['llm_fail'] += 1; continue
        if set(match_ix) | set(non_match_ix) != set(range(1, len(rows)+1)):
            missing = set(range(1, len(rows)+1)) - (set(match_ix) | set(non_match_ix))
            extra   = (set(match_ix) | set(non_match_ix)) - set(range(1, len(rows)+1))
            print(f'    SCHEMA MISMATCH: missing={missing} extra={extra}')
            totals['llm_fail'] += 1; continue

        match_ids    = [rows[ix-1]['id'] for ix in match_ix]
        non_match_ids = [rows[ix-1]['id'] for ix in non_match_ix]

        # Reconnect again if LLM took long enough to kill the conn.
        try:
            cur.execute("SELECT 1"); cur.fetchone()
        except Exception:
            reconnect()

        counts = apply(cur, match_ids, non_match_ids, rows, args.dry_run)
        totals['deletes_non_match'] += counts['deletes_non_match']
        totals['null_match']        += counts['null_match']
        totals['collapse_deletes']  += counts['collapse_deletes']
        totals['temporal_orphans']  += counts['temporal_orphans_deleted']
        totals['affected_fids'].add(fid)
        totals['pairs']             += 1

        if not args.dry_run:
            try:
                cur.execute("SELECT public._promote_zone_rules_for_fid(%s)", (fid,))
                conn.commit()
                totals['recascade_ok'] += 1
            except Exception as e:
                conn.rollback()
                print(f'    re-cascade fail: {e}')

        print(f'    → match={len(match_ids)} non={len(non_match_ids)}'
              f' (deleted={counts["deletes_non_match"]} null\'d={counts["null_match"]}'
              f' collapse={counts["collapse_deletes"]})')

    cost = totals['in_tokens']*3e-6 + totals['out_tokens']*15e-6
    print()
    print('=== SUMMARY ===')
    print(f'  pairs processed:        {totals["pairs"]}')
    print(f'  LLM ok / fail:          {totals["llm_ok"]} / {totals["llm_fail"]}')
    print(f'  non-match deletes:      {totals["deletes_non_match"]}')
    print(f'  matches NULL\'d:         {totals["null_match"]}')
    print(f'  temporal orphans del:   {totals["temporal_orphans"]}')
    print(f'  re-cascaded fids:       {totals["recascade_ok"]}')
    print(f'  affected fids:          {len(totals["affected_fids"])}')
    print(f'  tokens: in={totals["in_tokens"]:,} out={totals["out_tokens"]:,}'
          f'  est cost=${cost:.3f}')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
