"""
Municode (library.municode.com) helper — works after all.

CORRECTION 2026-05-16: my earlier conclusion that Municode is
bot-protected was wrong. The SPA returns "not authorized" only when
the URL slug is incorrect for that jurisdiction (e.g. using
`code_of_ordinances` when the jurisdiction's slug is `municipal_code`).
With the correct slug and the `.codes-chunks-pg` selector, the full
chapter text renders cleanly in 10-15s.

Usage:
    # Auto-detect jurisdiction's document slug
    python scripts/fetch/fetch_municode.py long_beach \\
        TIT6AN_CH6.16ANRE_6.16.310DOEXARDE

    # Explicit slug if auto-detect fails
    python scripts/fetch/fetch_municode.py long_beach \\
        TIT6AN_CH6.16ANRE_6.16.310DOEXARDE --doc municipal_code

    # URL build only
    python scripts/fetch/fetch_municode.py long_beach \\
        TIT6AN_CH6.16ANRE_6.16.310DOEXARDE --url-only

The output is the CHAPTER scope around the section, not just the one
section — Municode renders sibling sections in the same chunks-pg
container. Use `sed -n '/^<sec_num> -/,/^<next_num> -/p'` to extract
a single section.

Finding nodeIds: open the code in a regular browser, navigate to the
section, the URL bar shows the nodeId.

Known document slugs per jurisdiction (extend as discovered):
  long_beach                      → municipal_code
  east_bay_regional_park_district → code_of_ordinances (verify)
  los_angeles_county              → code  (verify)

Honest limits:
- The chunks-pg selector returns the chapter, not the single section.
  Grep / sed to scope.
- Some jurisdictions use non-standard slugs; pass --doc explicitly.
- Cookie banners may add chrome to the head of the output.
"""
from __future__ import annotations

import argparse
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from fetch_html import fetch as fetch_html_inner  # type: ignore
from playwright.sync_api import TimeoutError as PWTimeout


MUNICODE_BASE = "https://library.municode.com/ca"

# Known document slugs per jurisdiction. Extend as discovered.
# The default `code_of_ordinances` matches most jurisdictions; this
# table records the ones that need a different slug.
JURISDICTION_DOC_SLUG = {
    "long_beach": "municipal_code",
}
DEFAULT_DOC_SLUG = "code_of_ordinances"

# These markers indicate the SPA didn't resolve the nodeId — usually a
# slug mismatch or an invalid node. Not bot detection.
_FAILURE_MARKERS = (
    "The requested content cannot be found",
)


def build_url(
    jurisdiction_slug: str,
    node_id: str | None = None,
    doc_slug: str | None = None,
) -> str:
    """Construct a Municode deep-link URL.

    `doc_slug` defaults to JURISDICTION_DOC_SLUG[jurisdiction] or
    DEFAULT_DOC_SLUG.
    """
    if doc_slug is None:
        doc_slug = JURISDICTION_DOC_SLUG.get(jurisdiction_slug, DEFAULT_DOC_SLUG)
    base = f"{MUNICODE_BASE}/{jurisdiction_slug}/codes/{doc_slug}"
    if node_id:
        return f"{base}?nodeId={node_id}"
    return base


def fetch_node(
    jurisdiction_slug: str,
    node_id: str,
    wait_seconds: float = 12.0,
    doc_slug: str | None = None,
    selector: str = ".codes-chunks-pg",
) -> tuple[str, bool]:
    """Returns (text, success_flag). success=False if SPA returned 'not found' page."""
    url = build_url(jurisdiction_slug, node_id, doc_slug=doc_slug)
    text = fetch_html_inner(
        url,
        selector=selector,
        wait_seconds=wait_seconds,
        timeout_ms=60000,
    )
    success = not any(marker in text for marker in _FAILURE_MARKERS)
    return text, success


def main() -> int:
    ap = argparse.ArgumentParser(description="Municode municipal-code fetcher")
    ap.add_argument("jurisdiction", help="Slug, e.g. 'long_beach', 'los_angeles_county'")
    ap.add_argument(
        "node_id",
        nargs="?",
        help="Municode nodeId (find via manual browser navigation)",
    )
    ap.add_argument("--url-only", action="store_true", help="Print URL and exit")
    ap.add_argument("--wait", type=float, default=12.0)
    ap.add_argument(
        "--doc",
        help="Override document slug (default: per JURISDICTION_DOC_SLUG, else code_of_ordinances)",
    )
    ap.add_argument(
        "--selector",
        default=".codes-chunks-pg",
        help="Content selector (default .codes-chunks-pg — chapter scope)",
    )
    args = ap.parse_args()

    url = build_url(args.jurisdiction, args.node_id, doc_slug=args.doc)

    if args.url_only or not args.node_id:
        print(url)
        return 0

    try:
        text, success = fetch_node(
            args.jurisdiction,
            args.node_id,
            wait_seconds=args.wait,
            doc_slug=args.doc,
            selector=args.selector,
        )
    except PWTimeout as e:
        print(f"TIMEOUT: {e}", file=sys.stderr)
        return 4
    except Exception as e:
        print(f"FETCH ERROR: {e}", file=sys.stderr)
        return 3

    print(text)
    if not success:
        print(
            f"\n[!] Municode SPA didn't resolve nodeId for {url}\n"
            "[!] Common causes: wrong --doc slug for this jurisdiction, "
            "or invalid/expired nodeId. Verify in a browser.",
            file=sys.stderr,
        )
        return 5
    return 0


if __name__ == "__main__":
    sys.exit(main())
