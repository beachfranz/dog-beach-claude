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

PROMPT = """You map operator-level dog policies onto beach sections. Output a single FLAT JSON object with section rules.

Operator policy summary:
{summary}

Beach name: {beach_name}
Beach state: {state}
Operator name: {operator_name}

Output exactly this JSON shape (no markdown, no extra keys):
{{
  "sand":         {{ "rule": "off_leash" | "on_leash" | "not_allowed" | "mixed" | null }},
  "water":        {{ "rule": "swim" | "off_leash" | "on_leash" | "not_allowed" | null }},
  "trails":       {{ "rule": "off_leash" | "on_leash" | "not_allowed" | null }},
  "picnic_area":  {{ "rule": "off_leash" | "on_leash" | "not_allowed" | null }},
  "evidence":     "one short sentence quoting the policy basis"
}}

Rules:
- "sand" = the actual beach surface where dogs walk. If the policy says "dogs allowed on leash" generally, sand=on_leash.
- "water" = ocean/lake water entry. Swim only if the policy explicitly permits dogs in water.
- "trails" / "picnic_area" = adjacent park amenities. If unspecified, default to the same as sand.
- Use null only if the policy is genuinely silent on that section.
- "off_leash" requires explicit permission (Beach Bill, designated off-leash zone, dog park).
"""


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


def call_claude(prompt: str) -> dict | None:
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
                'max_tokens': 600,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=60.0,
        )
        r.raise_for_status()
        text = r.json()['content'][0]['text'].strip()
        # Strip code fences if present
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:].strip()
        return json.loads(text)
    except Exception as e:
        log(f'    LLM error: {e}')
        return None


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', default='OR,WA')
    ap.add_argument('--limit', type=int)
    ap.add_argument('--fids', help='comma-sep override')
    args = ap.parse_args()

    states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    fids = [int(x) for x in args.fids.split(',')] if args.fids else None

    targets = beaches_to_research(states, args.limit, fids)
    log(f'{len(targets)} beach(es) to research with operator policy')

    ok, skipped, failed = 0, 0, 0
    for i, t in enumerate(targets, 1):
        prompt = PROMPT.format(
            summary=(t['summary'] or '')[:2000],
            beach_name=t['name'],
            state=t['state'],
            operator_name=t['op_name'],
        )
        out = call_claude(prompt)
        if out is None:
            failed += 1
            log(f'  [{i}/{len(targets)}] {t["name"][:40]:<40} FAILED')
            continue
        sections = {k: v for k, v in out.items() if k in ('sand','water','trails','picnic_area')}
        # Drop sections that are entirely null
        sections = {k: v for k, v in sections.items() if v.get('rule') is not None}
        if not sections:
            skipped += 1
            log(f'  [{i}/{len(targets)}] {t["name"][:40]:<40} skipped (all null)')
            continue
        # Add evidence to one section if provided
        if out.get('evidence'):
            for k in sections:
                sections[k]['evidence'] = out['evidence']
                break
        upsert_bep(t['fid'], sections, t['source_url'])
        # Re-run zone_rules promoter to fold in
        q('select public._promote_zone_rules_for_fid(%s)', (t['fid'],), fetch=False)
        ok += 1
        if i % 25 == 0 or i == len(targets):
            log(f'  [{i}/{len(targets)}] ok={ok} skipped={skipped} failed={failed}')

    log(f'Done. ok={ok} skipped={skipped} failed={failed}')


if __name__ == '__main__':
    main()
