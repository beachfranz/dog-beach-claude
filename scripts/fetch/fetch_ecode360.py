"""
ecode360.com fetcher — Material-UI SPA-aware Playwright fetcher for
the General Code Publishers' municipal code platform.

ecode360 is the platform that hosts e.g. Huntington Beach (HBMC).
Each section / chapter / article has a numeric nodeId in the URL:
`https://ecode360.com/<nodeId>`. The body lives in `.article_content`
after SPA hydration.

Usage:
    python scripts/fetch/fetch_ecode360.py 43799422       # HBMC chapter 13.08
    python scripts/fetch/fetch_ecode360.py 43799422 --wait 8

Finding the right nodeId:
    1. Open https://ecode360.com in a regular browser
    2. Pick the jurisdiction → navigate the TOC to the target section
    3. The URL bar shows /<nodeId> when you're on the right node
    4. Run this script with that nodeId

Common pattern: chapter-level nodes show the chapter contents; section
nodes show just one section. Going up a level (chapter) gives the
fuller context useful for evidence_verbatim.

Honest limits:
- Some nodes return only the title note (e.g. "see §§ X, Y, Z") and not
  body text; that means the section is at a sibling node — go up a
  level and retry.
- Cookie banners + login prompts may pad the output; the `.article_content`
  selector keeps things scoped but tree-nav chrome occasionally leaks.
- Login-walled jurisdictions exist (rare for CA cities); no scripted
  bypass.
"""
from __future__ import annotations

import argparse
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from fetch_html import fetch as fetch_html_inner  # type: ignore
from playwright.sync_api import TimeoutError as PWTimeout


def fetch_node(node_id: str, wait_seconds: float = 6.0, selector: str = ".article_content") -> str:
    url = f"https://ecode360.com/{node_id}"
    return fetch_html_inner(
        url,
        selector=selector,
        wait_seconds=wait_seconds,
        timeout_ms=45000,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="ecode360 municipal code fetcher")
    ap.add_argument("node_id", help="ecode360 numeric node id (e.g. 43799422)")
    ap.add_argument(
        "--wait",
        type=float,
        default=6.0,
        help="SPA settle time (default 6.0s). Increase if content_area is empty.",
    )
    ap.add_argument(
        "--selector",
        default=".article_content",
        help="CSS selector (default .article_content). Try .content-area for fuller chapter view.",
    )
    args = ap.parse_args()

    try:
        text = fetch_node(args.node_id, wait_seconds=args.wait, selector=args.selector)
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
