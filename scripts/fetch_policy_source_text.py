"""fetch_policy_source_text.py — fetch + extract text from source_url.

Per Franz 2026-05-20 — task #94, half A.

For each policy_source row where full_text IS NULL AND source_url IS NOT
NULL, fetch the URL and persist the extracted text. Handles:
  - PDF (pdfplumber)
  - HTML (urllib + BeautifulSoup; Playwright fallback flagged for manual)
  - Municode SPA → flagged; reuse derive_policy_source_for_jurisdiction
    Playwright if needed

Usage:
  python scripts/fetch_policy_source_text.py --ps-id 7
  python scripts/fetch_policy_source_text.py --all
  python scripts/fetch_policy_source_text.py --all --dry-run
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]

import argparse, io, time, urllib.parse, urllib.request
from pathlib import Path

import psycopg2.extras
import pdfplumber
from bs4 import BeautifulSoup

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts' / 'fetch'))
try:
    from fetch_html import fetch as playwright_fetch  # type: ignore
except Exception:
    playwright_fetch = None  # type: ignore

MIN_TEXT_LEN = 200       # below this we don't trust the extraction
MAX_TEXT_LEN = 50_000    # cap (typical municipal codes < 5k; cap protects DB)
UA = 'Mozilla/5.0 (compatible; dog-beach-codify-bot)'


def fetch_pdf(url: str, timeout: int = 30) -> tuple[str | None, str | None]:
    """Returns (text, error). text is None on failure."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            parts = []
            for page in pdf.pages[:20]:  # cap at 20 pages
                t = page.extract_text() or ''
                if t.strip(): parts.append(t)
        text = '\n\n'.join(parts).strip()
        return (text or None, None if text else 'empty PDF text')
    except Exception as e:
        return (None, f'PDF fetch/parse: {type(e).__name__}: {str(e)[:120]}')


def fetch_html(url: str, timeout: int = 30) -> tuple[str | None, str | None]:
    """Returns (text, error). Strips nav/script/style; takes main content."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            html = r.read().decode('utf-8', errors='replace')
        soup = BeautifulSoup(html, 'html.parser')
        for tag in soup(['script','style','nav','footer','header','aside','form']):
            tag.decompose()
        main = (soup.find('main') or soup.find(id='content')
                or soup.find(class_='content') or soup.find(class_='main-content')
                or soup.find('article') or soup.body or soup)
        text = main.get_text(separator='\n', strip=True)
        text = '\n'.join(line for line in text.split('\n') if line.strip())
        if len(text) < MIN_TEXT_LEN:
            return (None, f'extracted text too short ({len(text)} chars) — likely SPA')
        return (text[:MAX_TEXT_LEN], None)
    except Exception as e:
        return (None, f'HTML fetch/parse: {type(e).__name__}: {str(e)[:120]}')


# Site-specific Playwright selectors per docs/jurisdiction_policy_source_playbook.md
# + feedback_municode_fetchable.md memory pin.
PLAYWRIGHT_SELECTORS: dict[str, tuple[str, float]] = {
    'library.municode.com': ('.codes-chunks-pg', 20.0),
    'qcode.us':             ('main', 8.0),
    'ecode360.com':         ('main', 8.0),
    'codepublishing.com':   ('#document-text', 8.0),
    'ecfr.gov':             ('main', 4.0),
}


def fetch_html_playwright(url: str) -> tuple[str | None, str | None]:
    """Browser-render fallback for SPA / bot-protected pages. Returns
    (text, error). Uses site-specific selectors when known."""
    if playwright_fetch is None:
        return (None, 'playwright unavailable')
    host = urllib.parse.urlparse(url).netloc.lower()
    selector, wait = (None, 2.0)
    for k, (sel, w) in PLAYWRIGHT_SELECTORS.items():
        if k in host:
            selector, wait = sel, w
            break
    try:
        text = playwright_fetch(url, selector=selector, wait_seconds=wait,
                                 timeout_ms=45000)
        text = '\n'.join(line for line in (text or '').split('\n') if line.strip())
        if len(text) < MIN_TEXT_LEN:
            return (None, f'playwright returned only {len(text)} chars')
        return (text[:MAX_TEXT_LEN], None)
    except Exception as e:
        return (None, f'playwright: {type(e).__name__}: {str(e)[:120]}')


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument('--ps-id', type=int)
    grp.add_argument('--ps-ids', type=str,
                     help='Comma-separated policy_source IDs')
    grp.add_argument('--all', action='store_true')
    grp.add_argument('--refresh-short', type=int, default=None,
                     help='Refresh ps rows where length(full_text) < N. '
                          'Use to recover captcha-blocked / partially-loaded rows.')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--force-playwright', action='store_true',
                    help='Skip urllib; go straight to Playwright. Useful for '
                         'hosts that return captcha HTML via urllib (eCFR, '
                         'Cloudflare-protected .gov, etc.).')
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if args.ps_id:
        cur.execute("SELECT id, subtype, citation, source_url FROM policy_source WHERE id=%s", (args.ps_id,))
    elif args.ps_ids:
        ids = [int(x) for x in args.ps_ids.split(',') if x.strip()]
        cur.execute("SELECT id, subtype, citation, source_url FROM policy_source WHERE id = ANY(%s) ORDER BY id", (ids,))
    elif args.refresh_short is not None:
        # Refresh short/blocked rows; exclude per-beach stubs (operator_posted_policy
        # rows that the per_beach extractor framework intentionally stubs to ~60 chars)
        # and rows without source_url. Skip rows the loader already failed on
        # (source_url='[web_search]' sentinel).
        cur.execute("""SELECT id, subtype, citation, source_url FROM policy_source
                       WHERE length(coalesce(full_text,'')) < %s
                         AND source_url IS NOT NULL
                         AND source_url <> '[web_search]'
                         AND NOT (subtype = 'operator_posted_policy'
                                  AND citation LIKE '%% per-beach policy (v1)%%')
                         AND EXISTS (SELECT 1 FROM beach_policy_source bps
                                      WHERE bps.policy_source_id = policy_source.id)
                       ORDER BY length(coalesce(full_text,'')) ASC, id""",
                    (args.refresh_short,))
    else:
        cur.execute("""SELECT id, subtype, citation, source_url FROM policy_source
                       WHERE full_text IS NULL AND source_url IS NOT NULL
                       ORDER BY id""")
    rows = cur.fetchall()
    if not rows:
        print('no rows'); return 1
    print(f'Targets: {len(rows)}')

    totals = {'fetched': 0, 'persisted': 0, 'failed': 0,
              'pdf': 0, 'html': 0, 'playwright': 0}
    for i, r in enumerate(rows, 1):
        url = r['source_url']
        is_pdf = url.lower().endswith('.pdf')
        if args.force_playwright and not is_pdf and playwright_fetch is not None:
            # Skip urllib; go straight to Playwright. For hosts that return
            # captcha HTML via urllib (urllib succeeds but with garbage),
            # the Playwright fallback below never triggers, so force is
            # the only way through.
            text, err = fetch_html_playwright(url)
            via = 'playwright' if text else 'playwright_failed'
        else:
            text, err = (fetch_pdf if is_pdf else fetch_html)(url)
            via = 'pdf' if is_pdf else 'html'
            # Playwright fallback when urllib HTML extraction returned too
            # little (SPAs render via JS; the static HTML has empty body).
            if (not text) and (not is_pdf) and playwright_fetch is not None:
                text, err = fetch_html_playwright(url)
                via = 'playwright' if text else via
        if not text:
            print(f'  [{i}/{len(rows)}] ps={r["id"]} {r["subtype"]:<26} FAIL: {err}')
            totals['failed'] += 1
            continue
        totals['fetched'] += 1
        totals[via] += 1

        if args.dry_run:
            print(f'  [{i}/{len(rows)}] ps={r["id"]} {r["subtype"]:<26} fetched {len(text)} chars [dry-run]')
        else:
            # Supavisor closes idle/long-lived connections after ~15min.
            # Retry-with-reconnect so long runs survive both the per-conn
            # drop AND short network blips (refetch + eastern bulk-cache
            # both died this way on 2026-06-14). 5 attempts × exp backoff
            # capped at 30s = ~75s total tolerance before giving up.
            for attempt in range(5):
                try:
                    cur.execute("UPDATE policy_source SET full_text = %s WHERE id = %s",
                                (text, r['id']))
                    conn.commit()
                    break
                except psycopg2.OperationalError as e:
                    if attempt == 4:
                        raise
                    delay = min(2 ** attempt, 30)
                    print(f'  [{i}/{len(rows)}] ps={r["id"]} reconnecting in {delay}s after pooler drop: {str(e)[:80]}', flush=True)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    time.sleep(delay)
                    try:
                        conn = connect()
                        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    except Exception as e2:
                        print(f'  [{i}/{len(rows)}] ps={r["id"]} reconnect failed: {str(e2)[:80]}', flush=True)
            totals['persisted'] += 1
            print(f'  [{i}/{len(rows)}] ps={r["id"]} {r["subtype"]:<26} PERSISTED {len(text)} chars')
        time.sleep(0.5)

    print()
    print(f'=== SUMMARY ===')
    print(f'  fetched:   {totals["fetched"]}/{len(rows)} ({totals["pdf"]} pdf, {totals["html"]} html, {totals["playwright"]} playwright)')
    print(f'  persisted: {totals["persisted"]}/{len(rows)}')
    print(f'  failed:    {totals["failed"]}/{len(rows)}')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
