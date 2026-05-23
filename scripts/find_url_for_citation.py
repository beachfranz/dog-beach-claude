"""find_url_for_citation.py — resolve missing source_url for policy_source rows.

Per Franz 2026-05-20 — task #94, half B.

Takes policy_source rows where source_url IS NULL and uses Sonnet to
propose the canonical URL based on the citation text + subtype. Verifies
with a HEAD request before persisting.

Usage:
  python scripts/find_url_for_citation.py --ps-id 7              # one row
  python scripts/find_url_for_citation.py --all                  # all url-less
  python scripts/find_url_for_citation.py --all --dry-run        # propose only
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]

import argparse, json, os, time, urllib.request

import psycopg2.extras
from anthropic import Anthropic

from scripts.common.db import connect

SYSTEM = """You resolve the canonical, fetchable URL for a codified policy
source given its citation text. Return ONLY a JSON object:
  {"url": "https://...", "confidence": "high"|"medium"|"low", "note": "..."}
or {"url": null, "confidence": "low", "note": "why not found"}.

URL hints by subtype:
  - municipal_code:    library.municode.com/<state>/<city>/codes/municipal_code?nodeId=...
                       or codepublishing.com / qcode / ecode360 (varies by city)
  - federal_regulation: ecfr.gov/current/title-N/...
  - federal_statute:    govinfo.gov/...
  - state_regulation/state_statute: jurisdiction-specific (e.g.
                                    oregonlaws.org, secure.sos.state.or.us/oard,
                                    leginfo.legislature.ca.gov)
  - superintendents_compendium: nps.gov/<park>/learn/management/compendium.htm
  - operator_posted_policy: the operator's own website (use site name in citation)
  - mou / lease_agreement: rarely have public URLs — return null
"""




def verify_url(url: str, timeout: int = 8) -> tuple[bool, int | None]:
    """HEAD (or GET first chunk if HEAD not allowed) the URL. Returns
    (reachable, http_status). 200/301/302 = good."""
    try:
        req = urllib.request.Request(url, method='HEAD',
            headers={'User-Agent': 'Mozilla/5.0 (codify-bot)'})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return (200 <= r.status < 400, r.status)
    except urllib.error.HTTPError as e:
        if e.code == 405:  # method not allowed → try GET
            try:
                req = urllib.request.Request(url, method='GET',
                    headers={'User-Agent': 'Mozilla/5.0 (codify-bot)'})
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    r.read(1024)
                    return (200 <= r.status < 400, r.status)
            except Exception:
                return (False, e.code)
        return (False, e.code)
    except Exception:
        return (False, None)


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument('--ps-id', type=int)
    grp.add_argument('--all', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if args.ps_id:
        cur.execute("SELECT id, subtype, citation FROM policy_source WHERE id = %s", (args.ps_id,))
    else:
        cur.execute("""SELECT id, subtype, citation FROM policy_source
                       WHERE source_url IS NULL ORDER BY id""")
    rows = cur.fetchall()
    if not rows:
        print('no rows'); return 1
    print(f'Targets: {len(rows)}')

    cli = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
    totals = {'found': 0, 'verified': 0, 'persisted': 0, 'in': 0, 'out': 0}

    for i, r in enumerate(rows, 1):
        user = f"""SUBTYPE: {r['subtype']}
CITATION: {r['citation']}

Propose the URL where the full text of this citation can be fetched."""
        try:
            msg = cli.messages.create(
                model='claude-sonnet-4-6', max_tokens=512,
                system=SYSTEM,
                messages=[{'role': 'user', 'content': user}],
            )
        except Exception as e:
            print(f'  [{i}/{len(rows)}] ps={r["id"]} LLM FAIL: {e}'); continue
        totals['in']  += msg.usage.input_tokens
        totals['out'] += msg.usage.output_tokens
        text = msg.content[0].text.strip()
        if text.startswith('```'):
            text = text.split('```',2)[1]
            if text.startswith('json'): text = text[4:]
            text = text.rsplit('```',1)[0].strip()
        try:
            parsed = json.loads(text)
        except Exception:
            print(f'  [{i}/{len(rows)}] ps={r["id"]} PARSE FAIL: {text[:120]}'); continue

        url = parsed.get('url')
        conf = parsed.get('confidence', 'low')
        note = parsed.get('note', '')[:120]

        if not url:
            print(f'  [{i}/{len(rows)}] ps={r["id"]} {r["subtype"]:<26} NO URL ({conf}): {note}')
            continue
        totals['found'] += 1

        ok, status = verify_url(url)
        if not ok:
            print(f'  [{i}/{len(rows)}] ps={r["id"]} {r["subtype"]:<26} PROPOSED ✗ ({conf}, http {status}): {url[:90]}')
            continue
        totals['verified'] += 1

        if args.dry_run:
            print(f'  [{i}/{len(rows)}] ps={r["id"]} {r["subtype"]:<26} VERIFIED ({conf}, http {status}): {url[:90]}  [dry-run]')
        else:
            cur.execute("UPDATE policy_source SET source_url = %s WHERE id = %s", (url, r['id']))
            conn.commit()
            totals['persisted'] += 1
            print(f'  [{i}/{len(rows)}] ps={r["id"]} {r["subtype"]:<26} PERSISTED ({conf}, http {status}): {url[:90]}')
        time.sleep(0.5)  # politeness

    cost = totals['in']*3e-6 + totals['out']*15e-6
    print()
    print(f'=== SUMMARY ===')
    print(f'  proposed:   {totals["found"]}/{len(rows)}')
    print(f'  verified:   {totals["verified"]}/{len(rows)}')
    print(f'  persisted:  {totals["persisted"]}/{len(rows)} (dry-run was {"on" if args.dry_run else "off"})')
    print(f'  tokens:     in={totals["in"]:,} out={totals["out"]:,}  est cost=${cost:.3f}')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
