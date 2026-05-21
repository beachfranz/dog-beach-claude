"""extract_and_load_deep.py — production deep-extract for codified policy sources.

Per docs/codify_deep_extract_full_spec.md §4.

Reads policy_source.full_text via beach_policy_source links, sends to
Sonnet using the pilot prompt (scripts/one_off/pilot_deep_extract_zone_rules.py),
normalizes vocab via scripts/codify_vocab.py, writes:

  - beach_policy_source  (one row per distinct region+section+rule)
  - beach_policy_source_temporal  (one row per temporal layer linked via bps_id)
  - vocab_review_queue   (one row per non-canonical rule/section value)

Idempotency:
  - Dedupe index (beach_fid, policy_source_id, section, COALESCE(region_name,
    '__default__'), rule) means re-running produces no dupes — ON CONFLICT
    DO NOTHING skips identical rows.
  - Stale rows from prior runs with different output are NOT auto-cleaned;
    use --purge to delete-first.

Usage:
  python scripts/extract_and_load_deep.py --fid 8560                # one beach
  python scripts/extract_and_load_deep.py --policy-source-id 47     # one source
  python scripts/extract_and_load_deep.py --fids 8560,6202,9716    # batch
  python scripts/extract_and_load_deep.py --all-mvp                 # CA/OR/WA full
  python scripts/extract_and_load_deep.py --fid 8560 --dry-run     # plan, no writes
  python scripts/extract_and_load_deep.py --fid 8560 --purge       # delete prior first
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
import truststore                # Win Python 3.14 OS-cert store
truststore.inject_into_ssl()

import argparse, json, os, time, urllib.parse
from pathlib import Path

import psycopg2, psycopg2.extras
from anthropic import Anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'scripts' / 'one_off'))

from codify_vocab import (
    normalize_rule, normalize_section, rule_is_global_note,
    authority_tier_for_subtype,
)
from pilot_deep_extract_zone_rules import SYSTEM, SCHEMA, FEW_SHOT, build_user

load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')

MODEL = 'claude-sonnet-4-6'
COST_IN  = 3e-6
COST_OUT = 15e-6

# bps_temporal CHECK constraints enforce these.
ALLOWED_ANCHORS = {
    'labor_day', 'labor_day_after', 'memorial_day', 'memorial_day_after',
    'mlk_day', 'presidents_day', 'plover_open', 'plover_close',
    'dst_start', 'dst_end', 'dawn', 'dusk',
}
# Translation from prompt-emitted anchor phrases to canonical enum values.
ANCHOR_ALIASES = {
    'labor-day':           'labor_day',
    'labor day':           'labor_day',
    'day-after-labor-day': 'labor_day_after',
    'day after labor day': 'labor_day_after',
    'memorial-day':        'memorial_day',
    'memorial day':        'memorial_day',
    'day-after-memorial-day': 'memorial_day_after',
    'mlk-day':             'mlk_day',
    'mlk day':             'mlk_day',
    'presidents-day':      'presidents_day',
    'presidents day':      'presidents_day',
    'plover-open':         'plover_open',
    'plover-close':        'plover_close',
    'dst-start':           'dst_start',
    'dst-end':             'dst_end',
    'dawn':                'dawn',
    'dusk':                'dusk',
    'sunrise':             'dawn',
    'sunset':              'dusk',
}
ALLOWED_TEMPORAL_RULES = {'not_allowed', 'on_leash', 'off_leash', 'off_leash_voice_control'}

# operative_status enum allowed values. Pilot sometimes emits non-enum
# values (e.g. 'seasonal', 'proposed') — coerce to 'operative' since the
# temporal layer already encodes seasonality and 'operative' is the
# default-active state.
ALLOWED_OPERATIVE_STATUS = {'operative', 'non_enforced', 'superseded_by_lower_tier', 'inactive'}


# Amenity-class sections — region_name is meaningless for these (per
# Franz 2026-05-21, Phase 3). The LLM sometimes emits a region_name
# like "Long Beach beach between Granada Avenue and Roycroft Avenue —
# parking lot access point" for a parking_lot rule. The rule still
# applies (parking is on-leash), but it attaches to the parking
# amenity tile in the default region — no separate zone card needed.
AMENITY_CLASS_SECTIONS = {
    'parking_lot', 'walkway', 'restrooms', 'showers', 'restrooms_showers',
    'picnic_area', 'fire_pits', 'playground', 'campground',
}


# ── region_name contamination guards (Franz 2026-05-20, Phase 5) ──────
# When the LLM emits a region_name that names a DIFFERENT beach in
# beaches_gold, skip the insert under THIS beach_fid — the row will land
# under the named beach's own extraction pass. Prevents the Avila ↔
# Fisherman's ↔ Olde Port cross-contamination class.
_BEACH_NAMES_CACHE: dict[str, int] | None = None
def _beach_name_index(cur) -> dict[str, int]:
    global _BEACH_NAMES_CACHE
    if _BEACH_NAMES_CACHE is None:
        cur.execute("SELECT lower(trim(name)), fid FROM beaches_gold")
        _BEACH_NAMES_CACHE = {n: fid for n, fid in cur.fetchall() if n}
    return _BEACH_NAMES_CACHE

# Class B: jurisdictional baseline region_names that add no per-beach
# value (citywide rules, "all X parks", generic carve-out clauses). Skip
# entirely. Patterns mirror the Phase 5 cleanup migration's denylist.
import re as _re
_CLASS_B_REGEX = _re.compile(
    r'^(all\s+.*\s+(beaches|parks|public places|state parks|city beaches|county beaches|ocean beaches|public beaches|public streets|other)|'
    r'all\s+(public places|santa barbara|pacifica|malibu|la county|long beach|other|port of|ocean beaches|public beaches|public streets)|'
    r'los angeles city\s|long beach city\s|'
    r'city of\s+\S.*\s[-—–]\s*all|'
    r'citywide|.*\bcitywide\b|.*\(citywide\)|'
    r'designated\s+(off-leash|off leash|dog\s+\w*areas|dog play|nesting|nature|picnic|swimming|areas)|'
    r'formally designated|off-leash/no-leash|'
    r'.*\(general\)|.*\s\(default\)|'
    r'jurisdiction|.*\bjurisdiction\b|'
    r'seattle city parks|monterey city parks|laguna beach public beaches|.*state-park-wide)',
    _re.IGNORECASE,
)
def _region_is_jurisdictional_baseline(region_name: str | None) -> bool:
    if not region_name: return False
    return bool(_CLASS_B_REGEX.match(region_name.strip()))


def _norm_anchor(v: str | None) -> str | None:
    """Map raw anchor text to canonical enum value, or None if not mappable.
    Returns None for unrecognized strings (caller should drop the temporal row)."""
    if not v: return None
    s = str(v).strip().lower()
    return ANCHOR_ALIASES.get(s)


def _connect():
    pooler = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ['SUPABASE_DB_PASSWORD'],
        dbname=(pp.path or '/postgres').lstrip('/'), sslmode='require',
    )


# ── Target selection ──────────────────────────────────────────────────
def select_targets(conn, args) -> list[tuple[int, int]]:
    """Returns list of (beach_fid, policy_source_id) pairs to process.
    Used for per-beach modes (--fid, --fids, --policy-source-id).
    For --all-mvp, use select_sources_for_bulk() instead — many sources
    are jurisdiction-level and link to N beaches, so a per-source loop
    collapses API calls (~250 sources vs ~2,250 pairs)."""
    cur = conn.cursor()
    where_parts = ['ps.full_text is not null', 'length(ps.full_text) > 100']
    params: tuple = ()
    if args.policy_source_id:
        where_parts.append('bps.policy_source_id = %s')
        params = (args.policy_source_id,)
    elif args.fid:
        where_parts.append('bps.beach_fid = %s')
        params = (args.fid,)
    elif args.fids:
        fid_list = [int(x) for x in args.fids.split(',') if x.strip()]
        where_parts.append('bps.beach_fid = any(%s)')
        params = (fid_list,)
    else:
        raise SystemExit('select_targets called without per-beach scope')

    sql = (
        'select distinct bps.beach_fid, bps.policy_source_id '
        'from beach_policy_source bps '
        'join beaches_gold g on g.fid = bps.beach_fid '
        'join policy_source ps on ps.id = bps.policy_source_id '
        'where ' + ' and '.join(where_parts) +
        ' order by bps.beach_fid, bps.policy_source_id'
    )
    cur.execute(sql, params)
    return [(r[0], r[1]) for r in cur.fetchall()]


def select_sources_for_bulk(conn) -> list[int]:
    """For --all-mvp: return unique policy_source IDs that any MVP+ beach
    references. Codified text is jurisdiction-level so we extract ONCE
    per source and apply to all linked beaches."""
    cur = conn.cursor()
    cur.execute("""
      SELECT DISTINCT ps.id
        FROM policy_source ps
        JOIN beach_policy_source bps ON bps.policy_source_id = ps.id
        JOIN beaches_gold g ON g.fid = bps.beach_fid
       WHERE ps.full_text IS NOT NULL
         AND length(ps.full_text) > 100
         AND g.state IN ('CA','OR','WA')
         AND g.is_active
       ORDER BY ps.id
    """)
    return [r[0] for r in cur.fetchall()]


def beaches_for_source(conn, policy_source_id: int) -> list[int]:
    cur = conn.cursor()
    cur.execute("""
      SELECT DISTINCT bps.beach_fid
        FROM beach_policy_source bps
        JOIN beaches_gold g ON g.fid = bps.beach_fid
       WHERE bps.policy_source_id = %s
         AND g.state IN ('CA','OR','WA')
         AND g.is_active
    """, (policy_source_id,))
    return [r[0] for r in cur.fetchall()]


# ── Anthropic extraction ──────────────────────────────────────────────
def extract_one(cli: Anthropic, beach_name: str, jurisdiction: str,
                subtype: str, citation: str, source_url: str | None,
                full_text: str) -> tuple[dict, int, int]:
    """Returns (parsed_json, input_tokens, output_tokens)."""
    user = build_user(
        beach_name=beach_name, jurisdiction=jurisdiction,
        source_subtype=subtype, source_citation=citation,
        source_url=source_url or '(none)', full_text=full_text,
    )
    msg = cli.messages.create(
        model=MODEL, max_tokens=4096,
        system=SYSTEM + '\n\n' + SCHEMA + '\n\n' + FEW_SHOT,
        messages=[{'role': 'user', 'content': user}],
    )
    text = msg.content[0].text.strip()
    if text.startswith('```'):
        text = text.split('```', 2)[1]
        if text.startswith('json'): text = text[4:]
        text = text.rsplit('```', 1)[0].strip()
    return (json.loads(text), msg.usage.input_tokens, msg.usage.output_tokens)


# ── Write logic ───────────────────────────────────────────────────────
def write_rows(conn, beach_fid: int, policy_source_id: int, subtype: str,
               parsed: dict, dry_run: bool = False) -> dict:
    """Writes bps + bps_temporal rows; returns counts.
    Returns {bps_inserted, bps_skipped, temporal_inserted, vocab_queued}."""
    counts = {'bps_inserted': 0, 'bps_skipped': 0,
              'temporal_inserted': 0, 'vocab_queued': 0}
    rows = parsed.get('rows', [])
    if not rows: return counts

    cur = conn.cursor()
    tier = authority_tier_for_subtype(subtype)

    for raw_row in rows:
        # Normalize vocabulary
        rule_raw    = raw_row.get('rule', 'unknown')
        section_raw = raw_row.get('section', 'global')
        rule, rule_is_new       = normalize_rule(rule_raw)
        section, section_is_new = normalize_section(section_raw)
        is_global_note          = rule_is_global_note(rule)

        evidence = (raw_row.get('evidence_verbatim') or '')[:1500]
        region_name = raw_row.get('region')      # None/null → __default__ via index
        region_anchor = raw_row.get('region_anchor')

        # Phase 3 guard (Franz 2026-05-21). Amenity-class sections never
        # carry region_name — the rule attaches to the amenity tile in
        # the default region.
        if region_name and section in AMENITY_CLASS_SECTIONS:
            region_name = None

        # Phase 5 contamination guards (Franz 2026-05-20).
        if region_name:
            # Class B: jurisdictional baselines add no per-beach value.
            if _region_is_jurisdictional_baseline(region_name):
                counts['bps_skipped'] += 1
                print(f'    skip (Class B baseline): region={region_name!r}')
                continue
            # Class A: region_name names a DIFFERENT beach — let that
            # beach's own extraction pass attach the row.
            name_idx = _beach_name_index(cur)
            other_fid = name_idx.get(region_name.strip().lower())
            if other_fid and other_fid != beach_fid:
                counts['bps_skipped'] += 1
                print(f'    skip (Class A cross-beach): region={region_name!r} → fid={other_fid}')
                continue
        # rule_modifier is JSONB — wrap string into {"modifier": "..."} when given
        rm_raw = raw_row.get('rule_modifier')
        rule_modifier_json = (json.dumps({'modifier': rm_raw}) if rm_raw else None)
        operative_raw = raw_row.get('operative_status', 'operative')
        operative = operative_raw if operative_raw in ALLOWED_OPERATIVE_STATUS else 'operative'

        # ── INSERT into beach_policy_source with ON CONFLICT DO NOTHING ──
        # Use RETURNING id; if no row returned (conflict), fetch existing id.
        if not dry_run:
            cur.execute("""
              INSERT INTO beach_policy_source
                (beach_fid, policy_source_id, region_name, region_anchor,
                 section, rule, rule_modifier, operative_status,
                 evidence_verbatim, authority_tier, is_global_note,
                 extracted_at, last_verified, created_at)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s::operative_status,
                      %s, %s, %s, now(), now(), now())
              ON CONFLICT (beach_fid, policy_source_id, section,
                           (COALESCE(region_name, '__default__')), rule)
                DO NOTHING
              RETURNING id
            """, (beach_fid, policy_source_id, region_name, region_anchor,
                  section, rule, rule_modifier_json, operative,
                  evidence, tier, is_global_note))
            row = cur.fetchone()
            if row:
                bps_id = row[0]
                counts['bps_inserted'] += 1
            else:
                # Conflict — find existing id
                cur.execute("""
                  SELECT id FROM beach_policy_source
                   WHERE beach_fid = %s AND policy_source_id = %s
                     AND section = %s
                     AND COALESCE(region_name, '__default__')
                         = COALESCE(%s::text, '__default__')
                     AND rule = %s
                """, (beach_fid, policy_source_id, section, region_name, rule))
                existing = cur.fetchone()
                bps_id = existing[0] if existing else None
                counts['bps_skipped'] += 1
        else:
            bps_id = None
            counts['bps_inserted'] += 1  # would-have-been

        # ── INSERT into beach_policy_source_temporal ──
        temporal = raw_row.get('temporal')
        if temporal and bps_id and not dry_run:
            season = temporal.get('season') or {}
            daily  = temporal.get('daily')  or {}
            year_round = bool(temporal.get('year_round'))

            # Skip temporal row entirely when there's no actual window —
            # year_round-only rules are fully encoded by the bps row.
            # window_kind check constraint only allows seasonal/daily/
            # seasonal_and_daily; year_round isn't a valid window.
            has_season = bool(season.get('start') or season.get('end'))
            has_daily  = bool(daily.get('start') or daily.get('end'))
            if not (has_season or has_daily):
                pass  # nothing to encode beyond what bps already says
            else:
                window_kind = (
                    'seasonal_and_daily' if has_season and has_daily
                    else 'daily' if has_daily
                    else 'seasonal'  # has_season only
                )
                # Temporal table only accepts these 4 rule values
                if rule not in ALLOWED_TEMPORAL_RULES:
                    pass  # skip temporal row; bps row carries the rule
                else:
                    # Daily times: store as TIME when HH:MM; anchor when dawn/dusk
                    def _split_daily(v):
                        if not v: return (None, None)
                        s = str(v).strip()
                        if ':' in s and s.replace(':','').isdigit():
                            return (s, None)  # daily_start='HH:MM'
                        return (None, _norm_anchor(s))  # 'dawn'/'dusk'

                    ds_t, ds_a = _split_daily(daily.get('start'))
                    de_t, de_a = _split_daily(daily.get('end'))

                    # Season: MM-DD goes into effective_from/to_md; named anchor
                    # goes into anchor_start/end (normalized via _norm_anchor).
                    def _split_season(v):
                        if not v: return (None, None)
                        s = str(v).strip()
                        if len(s) == 5 and s[2] == '-' and s[:2].isdigit():
                            return (s, None)
                        return (None, _norm_anchor(s))

                    fs_md, fs_a = _split_season(season.get('start'))
                    ts_md, ts_a = _split_season(season.get('end'))

                    # For 'seasonal' window: anchor_start/end must NOT be dawn/dusk
                    # (those are reserved for 'daily'). Use season anchors only.
                    if window_kind == 'seasonal':
                        anchor_start, anchor_end = fs_a, ts_a
                    elif window_kind == 'daily':
                        anchor_start, anchor_end = ds_a, de_a
                    else:  # seasonal_and_daily
                        anchor_start = fs_a or ds_a
                        anchor_end   = ts_a or de_a

                    # Drop the temporal row if window_kind requires data we
                    # couldn't normalize (e.g. seasonal with no MM-DD and no
                    # known anchor — the LLM emitted text like "summer" we
                    # can't represent).
                    consistent = True
                    if window_kind == 'seasonal':
                        if not (fs_md or ts_md or anchor_start or anchor_end):
                            consistent = False
                    elif window_kind == 'daily':
                        if not (ds_t or de_t or anchor_start in ('dawn','dusk') or anchor_end in ('dawn','dusk')):
                            consistent = False
                    elif window_kind == 'seasonal_and_daily':
                        season_ok = (fs_md or ts_md or fs_a or ts_a)
                        daily_ok  = (ds_t or de_t)
                        if not (season_ok and daily_ok):
                            consistent = False

                    if not consistent:
                        pass  # skip; downstream consensus will note absence
                    else:
                        # ux_temporal_unique_window is an INDEX (md5 of
                        # window fields) scoped to (beach, source, section,
                        # kind) — does NOT include bps_id. Pre-existing
                        # rows from earlier extractors will block. ON
                        # CONFLICT against index expressions is awkward;
                        # use savepoint + catch IntegrityError as the
                        # "DO NOTHING" equivalent.
                        cur.execute('SAVEPOINT sp_temp_ins')
                        try:
                            cur.execute("""
                              INSERT INTO beach_policy_source_temporal
                                (beach_fid, policy_source_id, bps_id, section,
                                 exception_rule, window_kind,
                                 effective_from_md, effective_to_md,
                                 anchor_start, anchor_end,
                                 daily_start, daily_end,
                                 season_label, extracted_via, extractor_confidence,
                                 created_at, updated_at)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                      %s::time, %s::time, %s, 'deep_v1', %s, now(), now())
                            """, (beach_fid, policy_source_id, bps_id, section,
                                  rule, window_kind,
                                  fs_md, ts_md, anchor_start, anchor_end,
                                  ds_t, de_t,
                                  (season.get('description') or daily.get('description')),
                                  0.85))
                            counts['temporal_inserted'] += 1
                            cur.execute('RELEASE SAVEPOINT sp_temp_ins')
                        except psycopg2.IntegrityError:
                            cur.execute('ROLLBACK TO SAVEPOINT sp_temp_ins')
                            # existing row wins (older extractor data preserved)

        # ── vocab review queue ──
        if (rule_is_new or section_is_new) and not dry_run:
            if rule_is_new:
                cur.execute("""
                  INSERT INTO vocab_review_queue
                    (vocab_type, value, bps_id, policy_source_id, context_snippet)
                  VALUES ('rule', %s, %s, %s, %s)
                """, (rule, bps_id, policy_source_id, evidence[:200]))
                counts['vocab_queued'] += 1
            if section_is_new:
                cur.execute("""
                  INSERT INTO vocab_review_queue
                    (vocab_type, value, bps_id, policy_source_id, context_snippet)
                  VALUES ('section', %s, %s, %s, %s)
                """, (section, bps_id, policy_source_id, evidence[:200]))
                counts['vocab_queued'] += 1

    if not dry_run:
        conn.commit()
    return counts


def purge_prior(conn, beach_fid: int, policy_source_id: int) -> int:
    """Delete all bps rows for this (beach, source) pair. Caller should
    only invoke with --purge. Cascades to bps_temporal via FK."""
    cur = conn.cursor()
    cur.execute("""
      DELETE FROM beach_policy_source
       WHERE beach_fid = %s AND policy_source_id = %s
    """, (beach_fid, policy_source_id))
    n = cur.rowcount
    conn.commit()
    return n


# ── Main ──────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument('--fid', type=int)
    grp.add_argument('--fids', type=str)
    grp.add_argument('--policy-source-id', type=int)
    grp.add_argument('--ps-ids', type=str,
                     help='Comma-separated policy_source IDs (per-source loop; for parallel slicing)')
    grp.add_argument('--all-mvp', action='store_true')
    ap.add_argument('--dry-run', action='store_true', help='plan + extract, no DB writes')
    ap.add_argument('--purge', action='store_true',
                    help='DELETE all prior bps rows for each (beach, source) before reinsert')
    ap.add_argument('--budget-usd', type=float, default=25.0,
                    help='abort if cumulative est cost exceeds this')
    args = ap.parse_args()

    conn = _connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cli = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])

    totals = {'bps_inserted':0, 'bps_skipped':0, 'temporal_inserted':0,
              'vocab_queued':0, 'in':0, 'out':0, 'n_pairs':0}
    t0 = time.time()

    # ── Per-source iteration when --all-mvp or --ps-ids (extract ONCE
    # per source, write to all linked beaches). Otherwise per-pair.
    if args.all_mvp or args.ps_ids:
        if args.ps_ids:
            sources = [int(x) for x in args.ps_ids.split(',') if x.strip()]
            print(f'Targets: {len(sources)} policy_source IDs (explicit slice)')
        else:
            sources = select_sources_for_bulk(conn)
            print(f'Targets: {len(sources)} unique policy_source rows')
        for i, ps_id in enumerate(sources, 1):
            cur.execute("SELECT subtype, citation, source_url, full_text FROM policy_source WHERE id = %s", (ps_id,))
            r = cur.fetchone()
            beaches = beaches_for_source(conn, ps_id)
            if not beaches: continue

            try:
                parsed, in_tok, out_tok = extract_one(
                    cli, '(applies to multiple MVP+ beaches)',
                    '(jurisdiction per citation)',
                    r['subtype'], r['citation'], r['source_url'], r['full_text'])
            except Exception as e:
                print(f'  [{i}/{len(sources)}] ps={ps_id} EXTRACT FAIL: {e}')
                continue

            totals['in']  += in_tok; totals['out'] += out_tok
            est_cost = totals['in']*COST_IN + totals['out']*COST_OUT
            if est_cost > args.budget_usd:
                print(f'BUDGET EXCEEDED (${est_cost:.2f} > ${args.budget_usd}); abort'); break

            per_source_counts = {'bps_inserted':0, 'bps_skipped':0,
                                  'temporal_inserted':0, 'vocab_queued':0}
            for fid in beaches:
                cc = write_rows(conn, fid, ps_id, r['subtype'], parsed, dry_run=args.dry_run)
                for k in per_source_counts: per_source_counts[k] += cc[k]
            for k in per_source_counts: totals[k] += per_source_counts[k]
            totals['n_pairs'] += len(beaches)

            print(f"  [{i}/{len(sources)}] ps={ps_id} {r['subtype'][:18]:<18} "
                  f"→ {len(beaches):>3} beaches  "
                  f"bps+{per_source_counts['bps_inserted']}/"
                  f"skip{per_source_counts['bps_skipped']} "
                  f"temp+{per_source_counts['temporal_inserted']} "
                  f"vocab+{per_source_counts['vocab_queued']} "
                  f"(${est_cost:.3f})")
    else:
        pairs = select_targets(conn, args)
        if not pairs:
            print('no targets'); return 1
        print(f'Targets: {len(pairs)} (beach_fid, policy_source_id) pairs')
        for i, (fid, ps_id) in enumerate(pairs, 1):
            cur.execute("""
              SELECT g.name AS beach_name, g.state, g.county_name,
                     ps.subtype, ps.citation, ps.source_url, ps.full_text
                FROM beach_policy_source bps
                JOIN beaches_gold g ON g.fid = bps.beach_fid
                JOIN policy_source ps ON ps.id = bps.policy_source_id
               WHERE bps.beach_fid = %s AND bps.policy_source_id = %s LIMIT 1
            """, (fid, ps_id))
            r = cur.fetchone()
            if not r: continue

            if args.purge and not args.dry_run:
                n_deleted = purge_prior(conn, fid, ps_id)
                print(f'  [{i}/{len(pairs)}] fid={fid} ps={ps_id} purged {n_deleted} prior rows')

            try:
                parsed, in_tok, out_tok = extract_one(
                    cli, r['beach_name'], f"{r['county_name']}, {r['state']}",
                    r['subtype'], r['citation'], r['source_url'], r['full_text'])
            except Exception as e:
                print(f'  [{i}/{len(pairs)}] fid={fid} ps={ps_id} EXTRACT FAIL: {e}'); continue

            totals['in']  += in_tok; totals['out'] += out_tok
            est_cost = totals['in']*COST_IN + totals['out']*COST_OUT
            if est_cost > args.budget_usd:
                print(f'BUDGET EXCEEDED (${est_cost:.2f} > ${args.budget_usd}); abort'); break

            counts = write_rows(conn, fid, ps_id, r['subtype'], parsed, dry_run=args.dry_run)
            for k in ('bps_inserted','bps_skipped','temporal_inserted','vocab_queued'):
                totals[k] += counts[k]
            totals['n_pairs'] += 1

            print(f"  [{i}/{len(pairs)}] fid={fid} ps={ps_id} {r['subtype'][:18]:<18} "
                  f"bps+{counts['bps_inserted']}/skip{counts['bps_skipped']} "
                  f"temp+{counts['temporal_inserted']} vocab+{counts['vocab_queued']} "
                  f"(${est_cost:.3f})")

    elapsed = time.time() - t0
    print()
    print('=== SUMMARY ===')
    print(f'  pairs processed:    {totals["n_pairs"]}')
    print(f'  bps inserted:       {totals["bps_inserted"]}')
    print(f'  bps skipped (dup):  {totals["bps_skipped"]}')
    print(f'  temporal inserted:  {totals["temporal_inserted"]}')
    print(f'  vocab queued:       {totals["vocab_queued"]}')
    print(f'  tokens:             in={totals["in"]:,} out={totals["out"]:,}')
    print(f'  est cost:           ${totals["in"]*COST_IN + totals["out"]*COST_OUT:.3f}')
    print(f'  wall time:          {elapsed:.0f}s')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
