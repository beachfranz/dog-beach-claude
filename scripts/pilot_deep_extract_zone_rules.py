"""pilot_deep_extract_zone_rules.py — DRY-RUN ONLY.

Per Franz 2026-05-20 — scope step 1 of the codify-richness pilot.
Takes one beach + its existing policy_source.full_text and asks the
LLM to emit MULTIPLE structured (region, section, rule, temporal)
rows instead of the current 1-row-per-source pattern.

WRITES NOTHING. Prints the prompt + the JSON output for inspection.

Usage:
  python scripts/one_off/pilot_deep_extract_zone_rules.py --fid 8560
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import truststore                # Win Python 3.14 OS-cert store
truststore.inject_into_ssl()

import argparse, json, os, urllib.parse
from pathlib import Path

import psycopg2, psycopg2.extras
from anthropic import Anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')


# ── Prompt design ──────────────────────────────────────────────────────────
SYSTEM = """You are a precise legal-text extractor for a beach dog-policy
database. Read one codified policy source (municipal code section, federal
regulation, Compendium passage, etc.) and emit a JSON array of STRUCTURED
rules. Be exhaustive: every distinct (geographic-region, section, rule,
temporal-condition) combination becomes a separate row.

Rules are atomic facts about WHO/WHERE/WHEN dogs are allowed (or not).
You are EXTRACTING — never inferring beyond the text. If the text doesn't
say it, you don't emit it.

CRITICAL — ONE ROW PER OPERATIVE RULE, NOT PER MENTION:
- If the same (region, section, rule, temporal) appears multiple times in
  the source with different wording, emit ONE row and collate all the
  supporting language into evidence_verbatim (separate quotes with ' / ').
- If text DEFINES a term (e.g. ORS lists 9 sub-clauses defining 'public
  nuisance'), emit ONE row for the rule. The sub-clauses go into
  evidence_verbatim as a single quote, not 9 rows.
- If two sources cite the same external regulation (e.g. Siuslaw NF and
  Oregon Dunes NRA both reference 36 CFR §261.16(j)), each source still
  emits its own row — but within ONE source, don't duplicate the rule.

PREFER CONTROLLED VOCABULARY (re-use these where they fit):
  rules: on_leash, off_leash, off_leash_voice_control, on_leash_or_voice,
         not_allowed, nuisance_restriction, feces_removal_required,
         waste_pickup_required, collar_tag_required
  sections: sand, water, playground, turf, walkway, restroom, parking_lot,
            picnic_area, tide_pool, dune_restoration, pier, jetty,
            developed_recreation_site, swimming_beach,
            snowy_plover_protection_area, global
If the source genuinely requires a NEW rule or section not in the lists
above, use a new lowercase_snake_case value AND add "_is_new": true on
the row so a reviewer can promote it."""


SCHEMA = """OUTPUT SCHEMA — return ONLY this JSON, no prose, no markdown:

{
  "source_summary": "1-2 sentence plain-English summary of what this source covers",
  "rows": [
    {
      "region": null | "human-readable geographic name (use the text's own
                       landmark phrasing, e.g. 'North of 29th Street extended
                       westward')",
      "region_anchor": null | "the exact boundary phrase from the text that
                              defines this region's geometry",
      "section": "sand | water | playground | turf | walkway | restroom |
                  parking_lot | picnic_area | tide_pool | dune_restoration |
                  pier | jetty | snowy_plover_protection_area | global |
                  <other_lowercase_snake_case>",
      "rule": "on_leash | off_leash_voice_control | off_leash |
               on_leash_or_voice | not_allowed | nuisance_restriction |
               <other_lowercase_snake_case>",
      "rule_modifier": null | "leash_max_6ft | leash_max_10ft |
                              includes_landscape_around_play_equipment |
                              signage_required | <other>",
      "operative_status": "operative | superseded | proposed | seasonal",
      "temporal": null | {
        "season": null | { "start": "MM-DD or named-anchor", "end": "MM-DD or
                           named-anchor", "description": "verbatim phrase" },
        "daily":  null | { "start": "HH:MM or 'dawn' or 'dusk'", "end": "HH:MM
                           or 'dawn' or 'dusk'", "description": "verbatim" },
        "year_round": true | false
      },
      "evidence_verbatim": "the exact passage that supports this row, copy/
                            paste from the source (max 400 chars; include
                            the subsection label)",
      "supersedes_baseline": true | false,  // true if this row OVERRIDES a
                                            // more general rule earlier in
                                            // the same source
      "_is_new": true | false               // OMIT unless rule/section is
                                            // outside the controlled vocab
    }
  ]
}

Named anchors for temporal:
  - 'day-after-Labor-Day' = first Tue after the first Mon of September
  - 'Labor-Day'           = first Mon of September
  - 'Memorial-Day'        = last Mon of May
  - 'dawn' / 'dusk'       = locally-resolved twilight
  - 'year-round'          = no temporal bound

Region anchor guidance:
  - If the text uses a street/landmark/coordinate, copy it verbatim
  - If text says 'beaches northerly of a line being an extension of 29th
    Street', region_anchor = that exact phrase
  - The downstream geometry step will translate anchor → polygon"""


FEW_SHOT = """EXAMPLE INPUT (Ocean Beach SF, 36 CFR §2.15 excerpt):
"§2.15 Pets. (a) The following are prohibited: ... (2) Failing to crate,
cage, restrain on a leash which shall not exceed six feet in length, or
otherwise physically confine a pet at all times. (b) The Superintendent may
designate any other area or trail closed to the use of pets ..."

EXAMPLE OUTPUT:
{
  "source_summary": "Federal regulation requiring all pets in NPS units to
                     be leashed (max 6 ft); authorizes Superintendent to
                     designate exceptions.",
  "rows": [
    {
      "region": null, "region_anchor": null,
      "section": "global",
      "rule": "on_leash",
      "rule_modifier": "leash_max_6ft",
      "operative_status": "operative",
      "temporal": { "year_round": true, "season": null, "daily": null },
      "evidence_verbatim": "§2.15(a)(2): Failing to crate, cage, restrain on
        a leash which shall not exceed six feet in length, or otherwise
        physically confine a pet at all times.",
      "supersedes_baseline": false
    },
    {
      "region": null, "region_anchor": "Superintendent-designated areas",
      "section": "global",
      "rule": "not_allowed",
      "rule_modifier": "superintendent_designation_required",
      "operative_status": "operative",
      "temporal": null,
      "evidence_verbatim": "§2.15(b): The Superintendent may designate any
        other area or trail closed to the use of pets.",
      "supersedes_baseline": false
    }
  ]
}"""


def build_user(beach_name: str, jurisdiction: str, source_subtype: str,
               source_citation: str, source_url: str, full_text: str) -> str:
    return f"""BEACH: {beach_name}
JURISDICTION CONTEXT: {jurisdiction}
SOURCE SUBTYPE: {source_subtype}
SOURCE CITATION: {source_citation}
SOURCE URL: {source_url}

SOURCE TEXT (extract from this only):
---
{full_text}
---

Emit the JSON now. No prose, no markdown fence."""


# ── Runner ─────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--fid', type=int, required=True)
    ap.add_argument('--model', default='claude-sonnet-4-6')
    args = ap.parse_args()

    pooler = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    conn = psycopg2.connect(host=pp.hostname, port=pp.port or 5432,
                            user=pp.username,
                            password=os.environ['SUPABASE_DB_PASSWORD'],
                            dbname=(pp.path or '/postgres').lstrip('/'),
                            sslmode='require')
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""SELECT g.name AS beach_name, g.state, g.county_name,
                          bps.policy_source_id, ps.subtype, ps.citation,
                          ps.source_url, ps.full_text
                   FROM beach_policy_source bps
                   JOIN beaches_gold g ON g.fid = bps.beach_fid
                   JOIN policy_source ps ON ps.id = bps.policy_source_id
                   WHERE bps.beach_fid = %s
                     AND ps.full_text IS NOT NULL
                     AND length(ps.full_text) > 100""", (args.fid,))
    rows = cur.fetchall()
    if not rows:
        print(f'fid {args.fid}: no policy_source rows with full_text')
        return 1
    print(f'fid {args.fid}: {len(rows)} source(s) with full_text\n')

    cli = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
    total_in = total_out = 0
    aggregated = []
    for i, r in enumerate(rows, 1):
        user = build_user(
            beach_name=r['beach_name'],
            jurisdiction=f"{r['county_name']}, {r['state']}",
            source_subtype=r['subtype'],
            source_citation=r['citation'],
            source_url=r['source_url'] or '(none)',
            full_text=r['full_text'],
        )
        print(f'─── source {i}: {r["citation"][:80]} ───')
        msg = cli.messages.create(
            model=args.model, max_tokens=4096,
            system=SYSTEM + '\n\n' + SCHEMA + '\n\n' + FEW_SHOT,
            messages=[{'role': 'user', 'content': user}],
        )
        total_in  += msg.usage.input_tokens
        total_out += msg.usage.output_tokens
        text = msg.content[0].text.strip()
        if text.startswith('```'):  # strip fence if model added one
            text = text.split('```', 2)[1]
            if text.startswith('json'): text = text[4:]
            text = text.rsplit('```', 1)[0].strip()
        try:
            parsed = json.loads(text)
            aggregated.append({
                'policy_source_id': r['policy_source_id'],
                'citation': r['citation'],
                'subtype': r['subtype'],
                'extracted': parsed,
            })
            print(f'  summary: {parsed.get("source_summary","")[:120]}')
            print(f'  rows emitted: {len(parsed.get("rows", []))}')
            for j, row in enumerate(parsed.get('rows', []), 1):
                temporal_str = ''
                t = row.get('temporal') or {}
                if t.get('season'): temporal_str += f" season={t['season'].get('start')}→{t['season'].get('end')}"
                if t.get('daily'):  temporal_str += f" daily={t['daily'].get('start')}→{t['daily'].get('end')}"
                if t.get('year_round'): temporal_str += ' year-round'
                region = row.get('region') or '(default)'
                print(f"    [{j}] region={region[:35]!r:<37} sec={row.get('section','?'):<25} "
                      f"rule={row.get('rule','?'):<24}{temporal_str}")
        except Exception as e:
            print(f'  PARSE ERROR: {e}')
            print(f'  raw output (first 500 chars):\n{text[:500]}')
    cost = total_in * 3e-6 + total_out * 15e-6  # Sonnet pricing
    print(f'\nTokens: in={total_in:,} out={total_out:,}  est cost=${cost:.3f}')
    out_path = ROOT / 'scripts' / 'output' / f'_pilot_deep_extract_fid{args.fid}.json'
    out_path.write_text(json.dumps(aggregated, indent=2))
    print(f'Full JSON written to {out_path}')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
