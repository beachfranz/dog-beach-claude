"""
California state law fetcher — wraps fetch_html.py with the right
URL pattern and CSS selector for leginfo.legislature.ca.gov.

leginfo is server-rendered JSF (not a SPA), so basic Playwright with
the `#codeLawSectionNoHead` selector returns clean section text.

Usage:
    python scripts/fetch/fetch_leginfo.py PRC 6001       # PRC §6001
    python scripts/fetch/fetch_leginfo.py PRC 6001.5     # decimal sections OK
    python scripts/fetch/fetch_leginfo.py HSC 115880     # AB 411 (water quality)
    python scripts/fetch/fetch_leginfo.py FGC 1583       # Fish & Game

Common law codes:
    PRC   Public Resources Code (state lands, parks, coastal access)
    HSC   Health & Safety Code (water quality, beach posting)
    FGC   Fish & Game Code (wildlife protection, MPAs)
    PEN   Penal Code (animal cruelty, leash law penalties)
    VEH   Vehicle Code
    GOV   Government Code
    CIV   Civil Code
    FAC   Food & Agricultural Code
    BPC   Business & Professions Code

Honest limits:
- leginfo URL fragments are case-sensitive on lawCode.
- Sections with trailing periods (e.g., `6001.`) work; without periods
  they sometimes 404. This script appends a period if missing.
- For a specific text revision (vs current), use leginfo's "history"
  feature manually — no public API.
"""
from __future__ import annotations

import argparse
import sys
import urllib.parse

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from fetch_html import fetch as fetch_html_inner  # type: ignore
from playwright.sync_api import TimeoutError as PWTimeout


LEGINFO_BASE = "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml"
SECTION_SELECTOR = "#codeLawSectionNoHead"


def build_url(law_code: str, section: str) -> str:
    if not section.endswith("."):
        section = section + "."
    qs = urllib.parse.urlencode({"lawCode": law_code.upper(), "sectionNum": section})
    return f"{LEGINFO_BASE}?{qs}"


def fetch_section(law_code: str, section: str, wait_seconds: float = 3.0) -> str:
    url = build_url(law_code, section)
    return fetch_html_inner(
        url,
        selector=SECTION_SELECTOR,
        wait_seconds=wait_seconds,
        timeout_ms=30000,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="leginfo.legislature.ca.gov fetcher")
    ap.add_argument("law_code", help="Law code (e.g. PRC, HSC, FGC, PEN)")
    ap.add_argument("section", help="Section number (e.g. 6001 or 6001.5)")
    ap.add_argument(
        "--wait",
        type=float,
        default=3.0,
        help="Settle time after DOMContentLoaded (default 3.0s)",
    )
    ap.add_argument(
        "--url-only",
        action="store_true",
        help="Print the constructed URL and exit (no fetch)",
    )
    args = ap.parse_args()

    if args.url_only:
        print(build_url(args.law_code, args.section))
        return 0

    try:
        text = fetch_section(args.law_code, args.section, wait_seconds=args.wait)
    except PWTimeout as e:
        print(f"TIMEOUT: {e}", file=sys.stderr)
        return 4
    except Exception as e:
        print(f"FETCH ERROR: {e}", file=sys.stderr)
        return 3

    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
