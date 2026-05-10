"""extract_beach_section_rules.py — derive per-section dog-policy rules
(sand, water, trails, picnic_area) from operator-level policy summaries
already in operator_dogs_policy. Writes BEP rows with
source='section_research_v1' that the new _zr_inject_sections_from_bep
injector folds into beach_dog_policy.zone_rules.

Per beach:
  1. Find governing operator (PAD-US containment, county, or city PIP).
  2. Pull operator_dogs_policy.summary text.
  3. Send summary + beach context to Haiku with section-mapping prompt.
  4. Upsert one BEP row per beach.
  5. Re-run _promote_zone_rules_for_fid to fold the new sections in.

Cost: ~$0.005/beach (Haiku, no Tavily). 544 OR/WA tier-1 beaches → ~$3.

Usage:
  python scripts/extract_beach_section_rules.py --states OR,WA
  python scripts/extract_beach_section_rules.py --states OR --limit 10
  python scripts/extract_beach_section_rules.py --fids 9711,9712
"""
from __future__ import annotations
import argparse, json, os, sys, time, urllib.parse
from pathlib import Path
import httpx
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')

ANTHROPIC_KEY = os.environ['ANTHROPIC_API_KEY']
MODEL = 'claude-haiku-4-5-20251001'

# Cacheable fixed instructions (system block).
SYSTEM_INSTRUCTIONS = """You map operator-level dog policies onto beach sections.

Output JSON shape per beach (no markdown):
{
  "sand":         { "rule": "off_leash" | "on_leash" | "not_allowed" | "mixed" | null },
  "water":        { "rule": "swim" | "off_leash" | "on_leash" | "not_allowed" | null },
  "trails":       { "rule": "off_leash" | "on_leash" | "not_allowed" | null },
  "picnic_area":  { "rule": "off_leash" | "on_leash" | "not_allowed" | null },
  "evidence":     "one short sentence quoting the policy basis"
}

Rules:
- "sand" = the actual beach surface where dogs walk. If the policy says "dogs allowed on leash" generally, sand=on_leash.
- "water" = ocean/lake water entry. "swim" only if the policy explicitly permits dogs in water.
- "trails" / "picnic_area" = adjacent park amenities. If unspecified, default to the same as sand.
- Use null only if the policy is genuinely silent on that section.
- "off_leash" requires explicit permission (Beach Bill, designated off-leash zone, dog park).
"""

# Per-operator user-block prefix (cacheable when batching beaches under the
# same operator).
OPERATOR_PREFIX = """Operator: {operator_name} ({state})
Operator policy summary:
{summary}
"""

# Single-beach (legacy) prompt — kept for back-compat
PROMPT = SYSTEM_INSTRUCTIONS + "\n\n" + OPERATOR_PREFIX + """
Beach name: {beach_name}

Return ONE flat JSON object as specified."""

# Multi-beach batched prompt — N beaches sharing the operator above
BATCH_INSTRUCTION = """
Below are {n} beaches managed by this operator. Return a single JSON object keyed by the integer index of each beach (1, 2, 3, …) — each value is the per-beach object using the schema above.

Beaches:
{beach_list}

Return:
{{
  "1": {{...per-beach object...}},
  "2": {{...}},
  ...
}}"""


def q(sql, args=None, fetch=True):
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            if fetch and cur.description:
                return cur.fetchall()
            return None


def log(m: str):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def beaches_to_research(states: list[str], limit: int | None, fids: list[int] | None) -> list[dict]:
    """Pick OR/WA tier-1/1b/1c beaches that have a matchable operator policy.
    Two-step: get fid list first, then per-fid find best operator policy."""
    if fids:
        fid_clause = 'g.fid = any(%s)'
        args = (fids,)
    else:
        fid_clause = """g.state = any(%s) and public.beach_location_tier(
          bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text
        ) in ('1_off-leash','2_on-leash')"""
        args = (states,)
    cap = f' limit {int(limit)}' if limit else ''
    fid_rows = q(f"""
      select g.fid, g.name, g.state, g.county_fips, g.geom
        from public.beaches_gold g
        join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
       where g.is_active and {fid_clause}
       order by g.fid{cap}
    """, args)

    out = []
    for fr in fid_rows:
        # Try city first (highest priority), then county, then state via PAD-US
        best = q("""
          (select op.canonical_name op_name, odp.summary, odp.source_url, op.level, 1 priority
             from public.jurisdictions j
             join public.operators op on op.level='city' and op.jurisdiction_id = j.id and op.is_active
             join public.operator_dogs_policy odp on odp.operator_id = op.id
                                                 and odp.policy_found = true and odp.summary is not null
            where st_contains(j.geom, %s::geometry) and j.place_type like 'C%%')
          union all
          (select op.canonical_name, odp.summary, odp.source_url, op.level, 2
             from public.operators op
             join public.operator_dogs_policy odp on odp.operator_id = op.id
                                                 and odp.policy_found = true and odp.summary is not null
            where op.level='county' and op.county_geoid = %s and op.is_active)
          union all
          (select op.canonical_name, odp.summary, odp.source_url, op.level, 3
             from public.pad_us_units pu
             join public.operators op on (op.canonical_name = pu.mng_name or pu.mng_name = any(op.aliases))
                                      and op.is_active
             join public.operator_dogs_policy odp on odp.operator_id = op.id
                                                 and odp.policy_found = true and odp.summary is not null
            where st_contains(pu.geom, %s::geometry))
          order by priority asc limit 1
        """, (fr['geom'], fr['county_fips'], fr['geom']))
        if best:
            b = best[0]
            out.append({
                'fid': fr['fid'], 'name': fr['name'], 'state': fr['state'],
                'op_name': b['op_name'], 'summary': b['summary'],
                'source_url': b['source_url'],
            })
    return out


def call_claude(system_text: str, cached_user_prefix: str,
                 user_specific: str, max_tokens: int = 1200) -> dict | None:
    """Call Haiku with prompt-cache markers on system + operator-prefix block.
    Cache hit when subsequent calls (within ~5 min) reuse the same system
    AND the same operator prefix. Saves ~80% input tokens within an
    operator's beach group."""
    try:
        r = httpx.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': ANTHROPIC_KEY,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': MODEL,
                'max_tokens': max_tokens,
                'system': [{'type': 'text', 'text': system_text,
                            'cache_control': {'type': 'ephemeral'}}],
                'messages': [{'role': 'user', 'content': [
                    {'type': 'text', 'text': cached_user_prefix,
                     'cache_control': {'type': 'ephemeral'}},
                    {'type': 'text', 'text': user_specific},
                ]}],
            },
            timeout=120.0,
        )
        r.raise_for_status()
        body = r.json()
        text = body['content'][0]['text'].strip()
        # Strip code fences if present
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:].strip()
        return {'json': json.loads(text), 'usage': body.get('usage', {})}
    except Exception as e:
        log(f'    LLM error: {e}')
        return None


def recently_section_extracted(fid: int, days: int = 7) -> bool:
    """True if this fid has a section_research_v1 BEP row newer than N days.
    Used to skip re-research on canon re-runs."""
    rows = q(
        """select 1 from public.beach_enrichment_provenance
            where gold_fid = %s
              and source = 'section_research_v1'
              and updated_at > now() - (%s::int || ' days')::interval
            limit 1""",
        (fid, days),
    )
    return bool(rows)


def upsert_bep(fid: int, sections: dict, source_url: str | None):
    """Write a section_research_v1 BEP row."""
    claimed = json.dumps({'sections': sections})
    q("""
      insert into public.beach_enrichment_provenance
        (gold_fid, field_group, source, source_url, claimed_values, confidence,
         is_canonical, extraction_type, cpad_role, updated_at)
      values
        (%s, 'dogs', 'section_research_v1', %s, %s::jsonb, 0.65,
         false, 'derived_url_crawl', 'beach_access', now())
      on conflict (gold_fid, field_group, source) do update
        set claimed_values = excluded.claimed_values,
            source_url     = excluded.source_url,
            updated_at     = now()
    """, (fid, source_url, claimed), fetch=False)


def _process_one_response(t: dict, parsed: dict, ok_counter: list, skipped_counter: list):
    """Common parse/write path for one beach's section JSON."""
    sections = {k: v for k, v in parsed.items() if k in ('sand','water','trails','picnic_area')}
    sections = {k: v for k, v in sections.items() if v and v.get('rule') is not None}
    if not sections:
        skipped_counter[0] += 1
        return False
    if parsed.get('evidence'):
        for k in sections:
            sections[k]['evidence'] = parsed['evidence']
            break
    upsert_bep(t['fid'], sections, t['source_url'])
    q('select public._promote_zone_rules_for_fid(%s)', (t['fid'],), fetch=False)
    ok_counter[0] += 1
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', default='OR,WA')
    ap.add_argument('--limit', type=int)
    ap.add_argument('--fids', help='comma-sep override')
    ap.add_argument('--batch-size', type=int, default=8,
                    help='beaches per Haiku call when grouped under one operator '
                         '(default 8; 1 disables batching)')
    ap.add_argument('--skip-recent', type=int, default=7,
                    help='skip beaches with section_research_v1 BEP newer than '
                         'N days (default 7; 0 to disable)')
    args = ap.parse_args()

    states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    fids = [int(x) for x in args.fids.split(',')] if args.fids else None

    targets = beaches_to_research(states, args.limit, fids)
    initial = len(targets)

    # #3 Skip recently-extracted beaches
    if args.skip_recent > 0:
        targets = [t for t in targets if not recently_section_extracted(t['fid'], args.skip_recent)]
        if initial != len(targets):
            log(f'Skipped {initial - len(targets)} beach(es) with section research <{args.skip_recent}d old')
    log(f'{len(targets)} beach(es) to research')

    if not targets:
        log('Done. Nothing to do.')
        return

    # #2 Group beaches by operator (same op_name + summary share an operator policy)
    groups: dict[tuple, list[dict]] = {}
    for t in targets:
        key = (t['op_name'], (t['summary'] or '')[:2000], t['state'])
        groups.setdefault(key, []).append(t)
    log(f'{len(groups)} operator group(s); avg {len(targets)/max(len(groups),1):.1f} beach/group')

    ok = [0]
    skipped = [0]
    failed = 0
    processed = 0

    for (op_name, summary, state), beaches in groups.items():
        cached_prefix = OPERATOR_PREFIX.format(
            operator_name=op_name, state=state, summary=summary)

        # Process in batches of args.batch_size
        for batch_start in range(0, len(beaches), max(args.batch_size, 1)):
            batch = beaches[batch_start:batch_start + max(args.batch_size, 1)]

            if args.batch_size <= 1 or len(batch) == 1:
                # Single-beach call (back-compat, also used when batch=1)
                user_specific = f"\nBeach name: {batch[0]['name']}\n\nReturn ONE flat JSON object as specified."
                result = call_claude(SYSTEM_INSTRUCTIONS, cached_prefix, user_specific,
                                     max_tokens=600)
                processed += 1
                if not result or 'json' not in result:
                    failed += 1
                    log(f'  [{processed}/{len(targets)}] {batch[0]["name"][:36]:<36} FAILED')
                    continue
                _process_one_response(batch[0], result['json'], ok, skipped)
            else:
                # Multi-beach batched call
                beach_list = "\n".join(f"{i+1}. {b['name']}" for i, b in enumerate(batch))
                user_specific = BATCH_INSTRUCTION.format(n=len(batch), beach_list=beach_list)
                result = call_claude(SYSTEM_INSTRUCTIONS, cached_prefix, user_specific,
                                     max_tokens=400 * len(batch) + 200)
                processed += len(batch)
                if not result or 'json' not in result:
                    failed += len(batch)
                    log(f'  [{processed}/{len(targets)}] batch of {len(batch)} for {op_name[:30]} FAILED')
                    continue
                response = result['json']
                # Parse per-beach: keys are "1", "2", … OR could be misformed
                for i, b in enumerate(batch):
                    key = str(i + 1)
                    parsed = response.get(key)
                    if not isinstance(parsed, dict):
                        failed += 1
                        log(f'  [batch] {b["name"][:36]:<36} missing in response')
                        continue
                    _process_one_response(b, parsed, ok, skipped)
                # Cache stats
                u = result.get('usage', {})
                cache_read = u.get('cache_read_input_tokens', 0)
                cache_create = u.get('cache_creation_input_tokens', 0)
                log(f'  [{processed}/{len(targets)}] batch of {len(batch)} for {op_name[:30]:<30} '
                    f'(cache_read={cache_read} create={cache_create}) ok={ok[0]}')

        # Per-group summary log
        if processed > 0 and processed % 50 == 0:
            log(f'  → {processed}/{len(targets)} processed; ok={ok[0]} skipped={skipped[0]} failed={failed}')

    log(f'Done. ok={ok[0]} skipped={skipped[0]} failed={failed}')


if __name__ == '__main__':
    main()
