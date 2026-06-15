"""extract_and_load_deep.py — production deep-extract for codified policy sources.

Per docs/codify_deep_extract_full_spec.md §4.

Reads policy_source.full_text via beach_policy_source links, sends to
Sonnet using the pilot prompt (scripts/pilot_deep_extract_zone_rules.py),
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

import argparse, json, os, time
from pathlib import Path

import psycopg2, psycopg2.extras
from anthropic import Anthropic

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect
from scripts.common.policy_source_cache import (
    build_cache_key, cache_lookup, cache_write,
)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
# pilot_deep_extract_zone_rules.py moved from scripts/one_off/ → scripts/
# on 2026-05-22 LATE during the clean-pipeline-v2 promotion sweep.
# Sibling import below now resolves directly (both files live in scripts/).

from codify_vocab import (
    normalize_rule, normalize_section, rule_is_global_note,
    authority_tier_for_subtype,
)
from pilot_deep_extract_zone_rules import SYSTEM, SCHEMA, FEW_SHOT, build_user

MODEL = 'claude-sonnet-4-6'
COST_IN  = 3e-6
COST_OUT = 15e-6

# Cache key for the dogs codified extraction. Hashes SYSTEM + SCHEMA +
# FEW_SHOT — bumping any of them invalidates cached extractions. MODEL
# is part of the key so model upgrades also invalidate.
CACHE_KEY = build_cache_key('dogs_v3', MODEL, SYSTEM + SCHEMA + FEW_SHOT)

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


# ── Named-exception matching ─────────────────────────────────────────
import re as _re

_NAME_STOPWORDS = {
    'the', 'a', 'an', 'of', 'and', 'beach', 'park', 'state', 'county',
    'city', 'municipal', 'national', 'regional', 'recreation', 'area',
    'reserve', 'preserve',
}


def _name_tokens(s: str) -> set[str]:
    return {w for w in _re.findall(r"\w+", (s or '').lower())
            if w not in _NAME_STOPWORDS and len(w) > 2}


def beach_matches_exception(beach_name: str, exception_name: str) -> bool:
    """Bidirectional name-match between a beach and a named-exception entry.

    Returns True if either side contains the other (after lowercasing) OR
    distinctive token overlap is ≥75%. Ported from extract_research_v2's
    name_match but bidirectional — exception names from statutes vary in
    formality (e.g. statute says 'Fort Funston' but beach is 'Fort Funston
    Beach', or statute says 'Fort Funston Beach' but beach record is
    'Fort Funston State Park').
    """
    if not exception_name:
        return False
    bn = beach_name.lower()
    en = exception_name.lower()
    if en in bn or bn in en:
        return True
    bn_tokens = _name_tokens(beach_name)
    en_tokens = _name_tokens(exception_name)
    if not bn_tokens or not en_tokens:
        return False
    overlap = bn_tokens & en_tokens
    # ≥75% of the smaller side's distinctive tokens match.
    smaller = min(len(bn_tokens), len(en_tokens))
    return len(overlap) / smaller >= 0.75


def write_deferred_sentinel(conn, beach_fid: int, policy_source_id: int,
                            citation: str, deferral_reason: str | None,
                            categorizer_reasoning: str | None,
                            dry_run: bool = False) -> int:
    """Write a low-confidence BEP row marking that THIS source defers to
    external state (signage / operator permission / condition) and
    cannot be disambiguated from text alone. Confidence 0.10 so it never
    wins canonical — its job is to PREVENT other extractors from
    hallucinating answers AND signal to consumer surface that the
    answer is genuinely "check on site." Returns 1 if a row was written.
    """
    if dry_run:
        return 1
    claimed = {
        "_deferred": True,
        "deferral_reason": deferral_reason or "unspecified",
        "citation": citation,
        "source_quote": (categorizer_reasoning or
                         f"Statute defers to {deferral_reason or 'external state'} "
                         "— cannot resolve from text alone."),
        "extraction_method": "categorizer_v2_deferred_sentinel",
    }
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO public.beach_enrichment_provenance
            (gold_fid, field_group, source, claimed_values, confidence,
             is_canonical, policy_source_id, updated_at)
        VALUES (%s, 'dogs', %s, %s::jsonb, 0.10, false, %s, now())
        ON CONFLICT (gold_fid, field_group, source) WHERE gold_fid IS NOT NULL
        DO UPDATE SET claimed_values = EXCLUDED.claimed_values,
                      updated_at = now()
    """, (
        beach_fid,
        f'codified_v1_deferred:ps_{policy_source_id}',
        json.dumps(claimed),
        policy_source_id,
    ))
    return 1


def parsed_for_exception_match(parsed: dict, beach_name: str,
                               exceptions: list[str]) -> dict:
    """If beach_name matches any entry in `exceptions`, swap parsed.rows to
    parsed.exception_rows (if the classifier provided them). Else return
    parsed unchanged.

    The categorizer emits `excepted_places[]`; here we expect the
    EXTRACTION layer (Phase 2) to emit `exception_rows` alongside `rows`
    when the policy_source has named exceptions. If `exception_rows`
    isn't present, fall through to default rule (parsed.rows) with a
    log marker — the named_exception statute will get the SAME treatment
    as a universal statute, which is the lossier but safer fallback.
    """
    if not exceptions:
        return parsed
    matched = next((e for e in exceptions
                    if beach_matches_exception(beach_name, e)), None)
    if matched is None:
        return parsed
    if 'exception_rows' in parsed and isinstance(parsed['exception_rows'], list):
        out = dict(parsed)
        out['rows'] = parsed['exception_rows']
        out['_exception_matched'] = matched
        return out
    # Fallback: classifier said this place is excepted but the extractor
    # didn't provide carve-out rows. Mark in claimed_values for audit but
    # apply default rule. Pin: bump SYSTEM_PROMPT to emit exception_rows.
    out = dict(parsed)
    out['_exception_matched_but_no_rows'] = matched
    return out


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
    ap.add_argument('--mode', type=str, default='auto',
                    choices=['auto', 'bulk', 'pair'],
                    help='auto = read policy_source.extraction_mode and route '
                         '(universal/named_exception → bulk-cache, '
                         'fuzzy_exception/per_beach/NULL → pair). '
                         'bulk forces bulk-cache for all sources (treats them '
                         'as universal — only safe if every source is genuinely '
                         'universal). pair forces per-(beach, ps) LLM calls '
                         '(legacy behaviour).')
    ap.add_argument('--no-cache', action='store_true',
                    help='Skip cache lookup. Cache writes still happen.')
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cli = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])

    totals = {'bps_inserted':0, 'bps_skipped':0, 'temporal_inserted':0,
              'vocab_queued':0, 'in':0, 'out':0, 'n_pairs':0}
    t0 = time.time()

    # ── Per-source iteration when --all-mvp or --ps-ids (extract ONCE
    # per source, write to all linked beaches). Otherwise per-pair.
    # Sources with extraction_mode='fuzzy_exception' or 'per_beach' (or
    # NULL when --mode=auto) get deferred to the pair-mode loop below.
    deferred_pair_targets: list[tuple[int, int]] = []  # (beach_fid, ps_id) for pair-loop
    cache_hits = 0
    cache_misses = 0
    exception_matches = 0
    bulk_mode_did_run = False

    if args.all_mvp or args.ps_ids:
        bulk_mode_did_run = True
        if args.ps_ids:
            sources = [int(x) for x in args.ps_ids.split(',') if x.strip()]
            print(f'Targets: {len(sources)} policy_source IDs (explicit slice)')
        else:
            sources = select_sources_for_bulk(conn)
            print(f'Targets: {len(sources)} unique policy_source rows')
        print(f'Mode: {args.mode}  cache_key={CACHE_KEY}')
        for i, ps_id in enumerate(sources, 1):
            cur.execute(
                "SELECT subtype, citation, source_url, full_text, "
                "       extraction_mode "
                "  FROM policy_source WHERE id = %s", (ps_id,))
            r = cur.fetchone()
            beaches = beaches_for_source(conn, ps_id)
            if not beaches: continue

            # Decide routing per the --mode flag + per-source category.
            eff_mode = args.mode
            if eff_mode == 'auto':
                # auto → trust extraction_mode column; NULL → pair-mode for safety
                src_mode = r.get('extraction_mode')
                if src_mode in ('universal', 'named_exception',
                                'universal_with_fuzzy_carveout'):
                    # universal_with_fuzzy_carveout is treated like universal at
                    # extraction time — one cached LLM call applied to all linked
                    # beaches. The carveout-subset disambiguation is a separate
                    # follow-up (extract_carveout_v1.py) so it doesn't burden
                    # the main extraction path. For now beaches in the carveout
                    # subset get the universal default rule.
                    eff_mode = 'bulk'
                elif src_mode == 'deferred':
                    eff_mode = 'deferred'
                elif src_mode == 'cross_reference':
                    eff_mode = 'xref'
                elif src_mode in ('fuzzy_exception', 'per_beach'):
                    eff_mode = 'pair'
                else:  # NULL — uncategorized
                    eff_mode = 'pair'

            if eff_mode == 'pair':
                # Defer all (beach, ps) pairs for this source to the pair loop.
                deferred_pair_targets.extend((fid, ps_id) for fid in beaches)
                reason = 'fuzzy/per-beach' if r.get('extraction_mode') else 'uncategorized'
                print(f"  [{i}/{len(sources)}] ps={ps_id} {r['subtype'][:18]:<18} "
                      f"→ DEFER {len(beaches)} pairs to pair-mode ({reason})")
                continue

            if eff_mode == 'deferred':
                # Write a sentinel BEP row per beach. No LLM. The sentinel
                # at confidence 0.10 prevents per_beach_disambiguate_v1 from
                # hallucinating, and tells consumer surface to defer to signage.
                cur.execute("""
                    SELECT result_json->'parsed' AS parsed
                      FROM public.policy_source_extraction_cache
                     WHERE policy_source_id = %s AND cache_key LIKE 'category:%%'
                     ORDER BY cached_at DESC LIMIT 1
                """, (ps_id,))
                ck = cur.fetchone()
                deferral_reason = None
                categorizer_reasoning = None
                if ck and ck.get('parsed'):
                    deferral_reason = ck['parsed'].get('deferral_reason')
                    categorizer_reasoning = ck['parsed'].get('reasoning')
                n_written = 0
                for fid in beaches:
                    n_written += write_deferred_sentinel(
                        conn, fid, ps_id, r['citation'], deferral_reason,
                        categorizer_reasoning, dry_run=args.dry_run)
                if not args.dry_run:
                    conn.commit()
                print(f"  [{i}/{len(sources)}] ps={ps_id} {r['subtype'][:18]:<18} "
                      f"→ DEFERRED {n_written} sentinel rows "
                      f"(reason: {deferral_reason or 'unspecified'})")
                continue

            if eff_mode == 'xref':
                # Look up the cross-reference target citation in policy_source.
                # If found AND cached, use the target's parsed dict. Otherwise
                # fall through to pair-mode (one more LLM pass per beach is
                # safer than hallucinating from an unresolved reference).
                cur.execute("""
                    SELECT result_json->'parsed' AS parsed
                      FROM public.policy_source_extraction_cache
                     WHERE policy_source_id = %s AND cache_key LIKE 'category:%%'
                     ORDER BY cached_at DESC LIMIT 1
                """, (ps_id,))
                ck = cur.fetchone()
                target_citation = None
                if ck and ck.get('parsed'):
                    target_citation = ck['parsed'].get('cross_reference_target')
                target_ps_id = None
                if target_citation:
                    # Extract the section-number pattern from the categorizer's
                    # target string. "Section 6.16.310" → "6.16.310"; "§9.04.05"
                    # → "9.04.05"; "Chapter 8" → "8". Then substring-search the
                    # number against policy_source.citation. Same jurisdiction
                    # is implied by the fact that both rows came from the same
                    # codify run; if multiple matches, pick the smaller-id one
                    # (heuristic: parent sections come first).
                    section_match = _re.search(r'\d+(?:[.\-]\d+)+|\b\d+\b',
                                               target_citation)
                    section_num = section_match.group(0) if section_match else None
                    if section_num:
                        cur.execute("""
                            SELECT id FROM public.policy_source
                             WHERE citation ~ %s
                               AND id <> %s
                             ORDER BY id LIMIT 1
                        """, (rf'(^|[^\d]){_re.escape(section_num)}([^\d]|$)',
                              ps_id))
                        xrow = cur.fetchone()
                        if xrow:
                            target_ps_id = xrow['id']
                target_parsed = None
                if target_ps_id is not None:
                    target_parsed = cache_lookup(cur, CACHE_KEY, target_ps_id)
                if target_parsed is None:
                    # Target unresolvable — fall through to pair-mode
                    deferred_pair_targets.extend((fid, ps_id) for fid in beaches)
                    print(f"  [{i}/{len(sources)}] ps={ps_id} {r['subtype'][:18]:<18} "
                          f"→ XREF UNRESOLVED (target={target_citation!r}) "
                          f"deferring {len(beaches)} pairs to pair-mode")
                    continue
                # Resolved — apply target's rules to source's beaches
                per_source_counts = {'bps_inserted':0, 'bps_skipped':0,
                                      'temporal_inserted':0, 'vocab_queued':0}
                for fid in beaches:
                    for attempt in range(5):
                        try:
                            cc = write_rows(conn, fid, ps_id, r['subtype'],
                                            target_parsed,
                                            dry_run=args.dry_run)
                            break
                        except psycopg2.OperationalError as e:
                            if attempt == 4:
                                raise
                            delay = min(2 ** attempt, 30)
                            print(f"    reconnecting in {delay}s after pooler drop on xref fid={fid}: {str(e)[:80]}", flush=True)
                            try: conn.close()
                            except Exception: pass
                            time.sleep(delay)
                            try:
                                conn = connect()
                                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            except Exception as e2:
                                print(f"    reconnect failed: {str(e2)[:80]}", flush=True)
                    for k in per_source_counts: per_source_counts[k] += cc[k]
                for k in per_source_counts: totals[k] += per_source_counts[k]
                totals['n_pairs'] += len(beaches)
                print(f"  [{i}/{len(sources)}] ps={ps_id} {r['subtype'][:18]:<18} "
                      f"→ XREF→ps={target_ps_id}  {len(beaches)} beaches  "
                      f"bps+{per_source_counts['bps_inserted']}/"
                      f"skip{per_source_counts['bps_skipped']}")
                continue

            # eff_mode == 'bulk' → cache + deterministic apply
            parsed: dict | None = None
            from_cache = False
            if not args.no_cache:
                parsed = cache_lookup(cur, CACHE_KEY, ps_id)
                if parsed is not None:
                    from_cache = True
                    cache_hits += 1
                    print(f"  [{i}/{len(sources)}] ps={ps_id} {r['subtype'][:18]:<18} "
                          f"CACHE_HIT", end='')

            if parsed is None:
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

                if not args.dry_run:
                    # Same retry-with-reconnect as write_rows below.
                    for attempt in range(5):
                        try:
                            cache_write(cur, CACHE_KEY, ps_id, parsed)
                            conn.commit()
                            break
                        except psycopg2.OperationalError as e:
                            if attempt == 4:
                                raise
                            delay = min(2 ** attempt, 30)
                            print(f"    reconnecting in {delay}s after pooler drop on cache_write ps={ps_id}: {str(e)[:80]}", flush=True)
                            try:
                                conn.close()
                            except Exception:
                                pass
                            time.sleep(delay)
                            try:
                                conn = connect()
                                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            except Exception as e2:
                                print(f"    reconnect failed: {str(e2)[:80]}", flush=True)
                cache_misses += 1
                print(f"  [{i}/{len(sources)}] ps={ps_id} {r['subtype'][:18]:<18} "
                      f"EXTRACTED", end='')

            # Named-exception application: load exceptions list from
            # the cached classifier output (category cache_key).
            exceptions: list[str] = []
            if r.get('extraction_mode') == 'named_exception':
                cur.execute(
                    "SELECT result_json FROM public.policy_source_extraction_cache "
                    " WHERE policy_source_id = %s AND cache_key LIKE 'category:%%' "
                    " ORDER BY cached_at DESC LIMIT 1", (ps_id,))
                ck = cur.fetchone()
                if ck and isinstance(ck.get('result_json'), dict):
                    p = ck['result_json'].get('parsed') or ck['result_json']
                    exceptions = p.get('excepted_places') or []

            per_source_counts = {'bps_inserted':0, 'bps_skipped':0,
                                  'temporal_inserted':0, 'vocab_queued':0}
            n_excepted = 0
            for fid in beaches:
                apply_parsed = parsed
                if exceptions:
                    cur.execute(
                        "SELECT coalesce(display_name_override, name) AS bn "
                        "  FROM public.beaches_gold WHERE fid = %s", (fid,))
                    bn = cur.fetchone()['bn'] or ''
                    apply_parsed = parsed_for_exception_match(parsed, bn, exceptions)
                    if apply_parsed.get('_exception_matched'):
                        n_excepted += 1

                # Wrap each write_rows call in a retry-with-reconnect loop
                # so transient pooler drops / network blips don't crash the
                # whole bulk run. 5 attempts × exp backoff capped at 30s.
                for attempt in range(5):
                    try:
                        cc = write_rows(conn, fid, ps_id, r['subtype'], apply_parsed,
                                        dry_run=args.dry_run)
                        break
                    except psycopg2.OperationalError as e:
                        if attempt == 4:
                            raise
                        delay = min(2 ** attempt, 30)
                        print(f"    reconnecting in {delay}s after pooler drop on fid={fid} ps={ps_id}: {str(e)[:80]}", flush=True)
                        try:
                            conn.close()
                        except Exception:
                            pass
                        time.sleep(delay)
                        try:
                            conn = connect()
                            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                        except Exception as e2:
                            print(f"    reconnect failed: {str(e2)[:80]}", flush=True)
                for k in per_source_counts: per_source_counts[k] += cc[k]
            for k in per_source_counts: totals[k] += per_source_counts[k]
            totals['n_pairs'] += len(beaches)
            exception_matches += n_excepted
            est_cost = totals['in']*COST_IN + totals['out']*COST_OUT

            print(f"  → {len(beaches):>3} beaches  "
                  f"bps+{per_source_counts['bps_inserted']}/"
                  f"skip{per_source_counts['bps_skipped']} "
                  f"temp+{per_source_counts['temporal_inserted']} "
                  f"vocab+{per_source_counts['vocab_queued']} "
                  f"{('excepted=' + str(n_excepted) + ' ') if n_excepted else ''}"
                  f"(${est_cost:.3f})")

    # ── Pair-mode iteration (per-beach LLM). Triggered when caller asks
    # for a specific (--fid/--fids/--policy-source-id) scope OR when the
    # bulk loop deferred sources whose extraction_mode requires per-beach
    # LLM (fuzzy_exception / per_beach / NULL).
    if not bulk_mode_did_run or deferred_pair_targets:
        if bulk_mode_did_run:
            pairs = deferred_pair_targets
            print(f'\nPair-mode loop: {len(pairs)} deferred (beach_fid, policy_source_id) pairs')
        else:
            pairs = select_targets(conn, args)
            print(f'Targets: {len(pairs)} (beach_fid, policy_source_id) pairs')
        if not pairs:
            if bulk_mode_did_run:
                pass  # bulk loop did the work; no deferred pairs to process
            else:
                print('no targets'); return 1
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
    if bulk_mode_did_run:
        print(f'  cache hits:         {cache_hits}')
        print(f'  cache misses:       {cache_misses}')
        print(f'  exception matches:  {exception_matches}')
    print(f'  tokens:             in={totals["in"]:,} out={totals["out"]:,}')
    print(f'  est cost:           ${totals["in"]*COST_IN + totals["out"]*COST_OUT:.3f}')
    print(f'  wall time:          {elapsed:.0f}s')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
