"""
Municode (library.municode.com) helper — honestly, mostly a
URL-builder. Municode's SPA is bot-protected: even non-headless
Playwright returns "The requested content cannot be found or you
are not authorized to view it."

This script will:
  1. Build the canonical deep-link URL for a (jurisdiction, nodeId) pair
  2. Attempt a Playwright fetch (often fails — see honest limits below)
  3. If the SPA returns the auth/not-found page, fall back to a
     manual-paste workflow message.

Use this when you already have a nodeId. Finding the nodeId requires
manual TOC navigation in a real browser (the SPA's URL bar updates
as you click into chapters).

Usage:
    # Build URL only (always works, doesn't hit network)
    python scripts/fetch/fetch_municode.py long_beach \\
        TIT6PUWE_CH6.16AN_S6.16.205DOBEZO --url-only

    # Try to fetch (likely fails for Long Beach; succeeds for some
    # jurisdictions that have looser bot detection)
    python scripts/fetch/fetch_municode.py long_beach \\
        TIT6PUWE_CH6.16AN_S6.16.205DOBEZO

Honest limits:
- **Municode actively blocks Playwright** for deep-section URLs as
  of 2026-05-16. The SPA falls back to its "auth or not found" page.
- The platform doesn't expose a public API, so we can't bypass via
  JSON either. CPRA request or browser-paste is the working fallback.
- Some Municode-hosted municipalities serve a non-SPA legacy view at
  `/codes/code_of_ordinances/?...` that does render to Playwright;
  the working/not-working split appears to be per-city.

Workflow when this fails:
    1. Open the URL in a regular browser
    2. Copy the section text
    3. Paste into the relevant walkthrough doc's evidence_verbatim
       (manual capture, same as `external_sources.py` curated rows)

CA cities + counties + special districts known to be on Municode
(non-exhaustive, from CPAD + walkthrough discovery):
  long_beach, los_angeles_county, oakland, san_diego_county,
  east_bay_regional_park_district, ...
"""
from __future__ import annotations

import argparse
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from fetch_html import fetch as fetch_html_inner  # type: ignore
from playwright.sync_api import TimeoutError as PWTimeout


MUNICODE_BASE = "https://library.municode.com/ca"


def build_url(jurisdiction_slug: str, node_id: str | None = None) -> str:
    """Construct a Municode deep-link URL.

    Examples:
        build_url("long_beach") →
            https://library.municode.com/ca/long_beach/codes/code_of_ordinances
        build_url("long_beach", "TIT6PUWE_CH6.16AN_S6.16.205DOBEZO") →
            https://library.municode.com/ca/long_beach/codes/code_of_ordinances?nodeId=TIT6PUWE_CH6.16AN_S6.16.205DOBEZO
    """
    base = f"{MUNICODE_BASE}/{jurisdiction_slug}/codes/code_of_ordinances"
    if node_id:
        return f"{base}?nodeId={node_id}"
    return base


_FAILURE_MARKERS = (
    "The requested content cannot be found",
    "or you are not authorized to view it",
    "Loading complete",
)


def fetch_node(
    jurisdiction_slug: str,
    node_id: str,
    wait_seconds: float = 12.0,
) -> tuple[str, bool]:
    """Returns (text, success_flag). success=False if bot-detection page detected."""
    url = build_url(jurisdiction_slug, node_id)
    text = fetch_html_inner(
        url,
        selector="#content",
        wait_seconds=wait_seconds,
        timeout_ms=60000,
    )
    success = not any(marker in text for marker in _FAILURE_MARKERS)
    return text, success


def main() -> int:
    ap = argparse.ArgumentParser(description="Municode helper (often fails — see docstring)")
    ap.add_argument("jurisdiction", help="Slug, e.g. 'long_beach', 'los_angeles_county'")
    ap.add_argument(
        "node_id",
        nargs="?",
        help="Municode nodeId (find via manual browser navigation)",
    )
    ap.add_argument("--url-only", action="store_true", help="Print URL and exit")
    ap.add_argument("--wait", type=float, default=12.0)
    args = ap.parse_args()

    url = build_url(args.jurisdiction, args.node_id)

    if args.url_only or not args.node_id:
        print(url)
        return 0

    try:
        text, success = fetch_node(args.jurisdiction, args.node_id, wait_seconds=args.wait)
    except PWTimeout as e:
        print(f"TIMEOUT: {e}", file=sys.stderr)
        return 4
    except Exception as e:
        print(f"FETCH ERROR: {e}", file=sys.stderr)
        return 3

    print(text)
    if not success:
        print(
            f"\n[!] Municode bot-protection detected for {url}\n"
            "[!] Open the URL in a regular browser and paste the section "
            "into the walkthrough doc evidence_verbatim. CPRA request also "
            "an option for high-value targets.",
            file=sys.stderr,
        )
        return 5  # custom: bot-detected
    return 0


if __name__ == "__main__":
    sys.exit(main())
