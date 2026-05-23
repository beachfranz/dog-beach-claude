"""reextract_area_first.py — re-extract a (beach, policy_source) pair
using an AREA-FIRST prompt (Franz 2026-05-21).

The original deep-extract prompt was CLAUSE-FIRST: emit one row per
(region, section, rule, temporal) combination. That produced cleaner
EXTRACTION but messy RENDERING: a single area would split into N rows
across the unique-index keys, multi-phrasing would create duplicate
regions, and clauses meant for sibling beaches would attach here too.

This script extracts PER BEACH using the beach's canonical_area_description
so the LLM:
  1. Only emits rules that apply to THIS beach's geographic identity.
  2. Consolidates same-area same-section rows (one row per
     section + rule, with all temporal modifiers as time_windows).
  3. Uses NULL region when the entire beach IS one area (Franz's
     "don't double-count the beach itself as a region" insight).
  4. Uses stable area names when sub-zones genuinely differ in rules.

Usage:
  python scripts/reextract_area_first.py --fid 8560 --ps-id 199
  python scripts/reextract_area_first.py --fid 8992 --ps-id 199
  # --dry-run to preview the LLM output without DB writes
"""
from __future__ import annotations
import sys, os, json, argparse
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
from pathlib import Path
from anthropic import Anthropic

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from extract_and_load_deep import ANCHOR_ALIASES, _norm_anchor  # type: ignore


SYSTEM = """You extract dog-policy rules from a codified source TARGETED
to ONE specific beach.

CRITICAL: this beach has a defined geographic identity (boundaries +
landmarks). You will be told its identity precisely. ONLY emit rules
that apply to areas WITHIN that identity. Skip clauses that describe
sibling beaches outside this beach's area.

OUTPUT PHILOSOPHY: AREA-FIRST, NOT CLAUSE-FIRST.
- Identify the geographic AREAS at THIS beach. Often the entire beach
  is ONE area. Sometimes the source carves the beach into 2-3 named
  sub-zones with genuinely different rules — emit each sub-zone as
  a distinct region.
- For each (area, section, rule) combination, emit ONE row. Multiple
  temporal modifiers for the same (area, section, rule) consolidate
  into MULTIPLE time_windows on that one row.
- If a clause REVERSES the base rule (e.g. default leashed, summer
  no-dogs in zone X) → that's a different rule, so it becomes a
  SEPARATE row in the same area.

REGION NAMING:
- If the entire beach IS one area (only one area in this source
  applies to it), use region = null. The default region carries the
  rules. DO NOT name a region after the beach itself.
- If 2+ sub-zones genuinely differ, use stable concise names:
  "between Powerhouse Park and 29th Street" not multiple phrasings.

AMENITY-CLASS SECTIONS (parking_lot, walkway, restrooms, showers,
restrooms_showers, picnic_area, fire_pits, playground, campground):
emit with region = null. Rules attach to the amenity tile, not a
geographic zone.

REGION NAMES MUST BE STABLE within a single extraction. Do not emit
the same area under two different region names."""


SCHEMA = """OUTPUT SCHEMA — return ONLY this JSON object (no prose):

{
  "source_summary": "1-2 sentence plain-English summary of what this
                     source covers and how it applies to THIS beach",
  "rows": [
    {
      "region": null | "stable short area name (only when this beach
                       has 2+ sub-zones with different rules)",
      "region_anchor": null | "verbatim boundary phrase from the text",
      "section": "sand | water | playground | turf | walkway |
                  restrooms | parking_lot | picnic_area | tide_pools |
                  trails | bluff | dunes | global | <other_snake>",
      "rule": "on_leash | off_leash | off_leash_voice_control |
               on_leash_or_voice | not_allowed | nuisance_restriction |
               waste_pickup_required | collar_tag_required |
               <other_snake>",
      "rule_modifier": null | "leash_max_6ft | service_dogs_excepted |
                              signage_required | <other>",
      "operative_status": "operative | superseded | proposed",
      "temporal": null | {
        "year_round": true | false,
        "season": null | {"start": "MM-DD or named-anchor", "end": "...",
                          "description": "verbatim phrase"},
        "daily":  null | {"start": "HH:MM or 'dawn' or 'dusk'",
                          "end": "...", "description": "verbatim"}
      },
      "evidence_verbatim": "exact passage from the source (max 400 chars,
                            include subsection label like §4.08.020.B.1)",
      "supersedes_baseline": true | false
    }
  ]
}

Named anchors for temporal:
  - 'day-after-Labor-Day' = first Tue after the first Mon of Sep
  - 'Labor-Day'           = first Mon of Sep
  - 'Memorial-Day'        = last Mon of May
  - 'dawn' / 'dusk'       = locally-resolved twilight

START YOUR RESPONSE WITH `{` — no markdown, no preamble."""




def build_user(beach: dict, source: dict) -> str:
    user  = f"BEACH NAME: {beach['name']!r}\n"
    if beach.get('lat') is not None:
        user += f"COORDS: {beach['lat']:.4f}, {beach['lon']:.4f}\n"
    if beach.get('desc'):
        user += f"\nGEOGRAPHIC IDENTITY:\n{beach['desc']}\n"
    user += (f"\nSOURCE: {source['citation']}\n"
             f"SUBTYPE: {source['subtype']}\n"
             f"URL: {source['source_url'] or '(none)'}\n\n"
             f"FULL TEXT:\n{source['full_text']}\n\n"
             "Emit area-first JSON for THIS beach only.")
    return user


def extract(cli: Anthropic, beach: dict, source: dict) -> dict:
    msg = cli.messages.create(
        model='claude-sonnet-4-6', max_tokens=4096,
        system=SYSTEM + '\n\n' + SCHEMA,
        messages=[{'role': 'user', 'content': build_user(beach, source)}],
    )
    raw = msg.content[0].text if msg.content else ''
    text = (raw or '').strip()
    if text.startswith('```'):
        first = text.find('\n', 3); last = text.rfind('```')
        if first >= 0 and last > first: text = text[first+1:last].strip()
    if not text:
        raise ValueError(f'empty response (stop={msg.stop_reason})')
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        import re
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if not m: raise ValueError(f'no JSON: {text[:200]}')
        parsed = json.loads(m.group(0))
    return {'parsed': parsed, 'in_tokens': msg.usage.input_tokens,
            'out_tokens': msg.usage.output_tokens}


def fetch_beach(cur, fid: int) -> dict:
    cur.execute("""SELECT name, lat, lon, canonical_area_description
                     FROM beaches_gold WHERE fid = %s""", (fid,))
    r = cur.fetchone()
    return {'fid': fid, 'name': r[0], 'lat': r[1], 'lon': r[2], 'desc': r[3]}


def fetch_source(cur, ps_id: int) -> dict:
    cur.execute("""SELECT subtype, citation, source_url, full_text
                     FROM policy_source WHERE id = %s""", (ps_id,))
    r = cur.fetchone()
    return {'id': ps_id, 'subtype': r[0], 'citation': r[1],
            'source_url': r[2], 'full_text': r[3]}


def delete_existing(cur, fid: int, ps_id: int) -> tuple[int, int]:
    cur.execute("""DELETE FROM beach_policy_source_temporal
                    WHERE bps_id IN (SELECT id FROM beach_policy_source
                                      WHERE beach_fid = %s AND policy_source_id = %s)""",
                (fid, ps_id))
    t_del = cur.rowcount
    cur.execute("""DELETE FROM beach_policy_source
                    WHERE beach_fid = %s AND policy_source_id = %s""",
                (fid, ps_id))
    return cur.rowcount, t_del


def insert_rows(cur, fid: int, ps_id: int, parsed: dict) -> int:
    """Group LLM rows by (region, section, rule), insert ONE bps row
    per group, attach all temporal modifiers from the group as
    bps_temporal rows under that single bps_id. This avoids the
    unique-index collision when the LLM emits multiple rows with the
    same canonical key but different temporal windows."""
    AMENITY_SECTIONS = {'parking_lot','walkway','restrooms','showers',
                        'restrooms_showers','picnic_area','fire_pits',
                        'playground','campground'}
    # Phase 1: group
    groups: dict[tuple, dict] = {}
    for row in parsed.get('rows') or []:
        section = (row.get('section') or 'global').lower().strip()
        rule    = (row.get('rule')    or 'unknown').lower().strip()
        region  = row.get('region')
        if section in AMENITY_SECTIONS:
            region = None
        key = (region, section, rule)
        g = groups.setdefault(key, {
            'region': region, 'region_anchor': row.get('region_anchor'),
            'section': section, 'rule': rule,
            'rule_modifier': row.get('rule_modifier'),
            'operative_status': row.get('operative_status') or 'operative',
            'evidences': [], 'temporals': [],
        })
        ev = row.get('evidence_verbatim')
        if ev: g['evidences'].append(ev)
        temporal = row.get('temporal') or {}
        for kind, sub in (('seasonal', temporal.get('season')),
                          ('daily',    temporal.get('daily'))):
            if sub: g['temporals'].append({'kind': kind, **sub})

    inserted = 0
    for key, g in groups.items():
        rm = g['rule_modifier']
        rm_json = json.dumps({'modifier': rm}) if rm else None
        op_status = g['operative_status']
        if op_status not in ('operative','non_enforced','superseded_by_lower_tier','inactive'):
            op_status = 'operative'
        # Concatenate evidence quotes with ' / '
        evidence = ' / '.join(dict.fromkeys(g['evidences']))[:1500]
        cur.execute("""
          INSERT INTO beach_policy_source
            (beach_fid, policy_source_id, region_name, region_anchor,
             section, rule, rule_modifier, operative_status,
             evidence_verbatim, extracted_at, last_verified, created_at)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s::operative_status,
                  %s, now(), now(), now())
          ON CONFLICT (beach_fid, policy_source_id, section,
                       (COALESCE(region_name, '__default__')), rule)
            DO NOTHING
          RETURNING id
        """, (fid, ps_id, g['region'], g['region_anchor'],
              g['section'], g['rule'], rm_json, op_status, evidence))
        ret = cur.fetchone()
        if not ret: continue
        bps_id = ret[0]; inserted += 1
        # Dedupe temporal windows within the group (LLM often emits same
        # window across multiple source rows that grouped together).
        seen_tws = set()
        unique_tws = []
        for tw in g['temporals']:
            sig = json.dumps({k: tw.get(k) for k in ('kind','start','end','description')},
                              sort_keys=True)
            if sig in seen_tws: continue
            seen_tws.add(sig); unique_tws.append(tw)
        # Attach unique temporal windows.
        for tw in unique_tws:
            kind = tw['kind']
            start = tw.get('start'); end = tw.get('end')
            label = tw.get('description') or tw.get('label')
            anchor_start = anchor_end = None
            daily_start = daily_end = None
            eff_from = eff_to = None
            def _norm(val):
                """Map LLM-emitted anchor or MM-DD value to canonical."""
                if not val: return (None, None)  # (eff_md, anchor)
                s = str(val).strip()
                if '-' in s and len(s) >= 5 and s[0].isdigit() and s[2] == '-':
                    return (s[:5], None)  # MM-DD
                norm = ANCHOR_ALIASES.get(s.lower())
                if norm: return (None, norm)
                # Fallback: lowercase the raw value (caller may have already canonical)
                return (None, s.lower().replace('-', '_').replace(' ', '_'))

            if kind == 'seasonal':
                eff_from, anchor_start = _norm(start)
                eff_to,   anchor_end   = _norm(end)
                # The injector (_zr_inject_from_policy_sources H3) only
                # surfaces window_kind IN ('daily','seasonal_and_daily').
                # For season-only "all-day during season" windows, encode
                # as seasonal_and_daily with full 00:00-23:59 day so the
                # injector renders it. Per Franz 2026-05-21 patch.
                kind = 'seasonal_and_daily'
                daily_start = '00:00'
                daily_end   = '23:59'
            else:  # daily
                if start in ('dawn', 'dusk'): anchor_start = start
                elif start: daily_start = start if ':' in str(start) else f'{start}:00'
                if end in ('dawn', 'dusk'): anchor_end = end
                elif end: daily_end = end if ':' in str(end) else f'{end}:00'
            cur.execute("SAVEPOINT sp_temporal")
            try:
                cur.execute("""
                  INSERT INTO beach_policy_source_temporal
                    (beach_fid, policy_source_id, section, exception_rule,
                     window_kind, effective_from_md, effective_to_md,
                     anchor_start, anchor_end, daily_start, daily_end,
                     season_label, extracted_via, bps_id, created_at, updated_at)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s::time, %s::time, %s, %s, %s, now(), now())
                """, (fid, ps_id, g['section'], g['rule'], kind, eff_from, eff_to,
                      anchor_start, anchor_end, daily_start, daily_end,
                      label, 'reextract_area_first_v2', bps_id))
                cur.execute("RELEASE SAVEPOINT sp_temporal")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT sp_temporal")
                print(f'      temporal insert fail (bps_id={bps_id}): {e}')
    return inserted


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--fid', type=int, required=True)
    ap.add_argument('--ps-id', type=int, required=True)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    conn = connect(); cur = conn.cursor()
    cli = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])

    beach  = fetch_beach(cur, args.fid)
    source = fetch_source(cur, args.ps_id)
    print(f"BEACH:  fid={beach['fid']} {beach['name']!r}")
    print(f"SOURCE: ps={source['id']} {source['citation'][:80]}...")
    print(f"\nCalling LLM (area-first v2)...")
    out = extract(cli, beach, source)
    parsed = out['parsed']
    print(f"\nLLM summary: {parsed.get('source_summary')!r}")
    print(f"Rows extracted: {len(parsed.get('rows') or [])}")
    for r in parsed.get('rows') or []:
        print(f"  - region={r.get('region')!r:<50} section={r.get('section')} rule={r.get('rule')}")
    print(f"\nTokens: in={out['in_tokens']:,} out={out['out_tokens']:,}"
          f"  est cost=${out['in_tokens']*3e-6 + out['out_tokens']*15e-6:.4f}")

    if args.dry_run:
        print("\n[DRY RUN] no DB changes.")
        return 0

    print(f"\nDeleting existing bps rows for (fid={args.fid}, ps={args.ps_id})...")
    n_bps, n_t = delete_existing(cur, args.fid, args.ps_id)
    print(f"  deleted: {n_bps} bps + {n_t} temporal")
    print(f"\nInserting {len(parsed.get('rows') or [])} new rows...")
    n_ins = insert_rows(cur, args.fid, args.ps_id, parsed)
    print(f"  inserted: {n_ins}")
    print(f"\nRe-cascading fid {args.fid}...")
    cur.execute("SELECT public._promote_zone_rules_for_fid(%s)", (args.fid,))
    conn.commit()
    print("  done")
    return 0


if __name__ == '__main__':
    sys.exit(main())
