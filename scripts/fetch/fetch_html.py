"""
Playwright-backed HTML fetcher for 403-blocked sites.

Runs headless Chromium with a realistic User-Agent + viewport so sites
that block default WebFetch (ecode360, ebparks.org, OC Register,
ecfr.gov HTML, Federal Register HTML, etc.) generally let us through.

Usage:
    python scripts/fetch/fetch_html.py <URL>                 # full page text
    python scripts/fetch/fetch_html.py <URL> --selector main # CSS-scoped
    python scripts/fetch/fetch_html.py <URL> --html          # raw HTML
    python scripts/fetch/fetch_html.py <URL> --wait 2.0      # extra settle time

Exit codes:
    0 success / 2 bad args / 3 fetch error / 4 timeout

Honest limits:
- Sites using Cloudflare Turnstile or full bot-management (e.g. some
  Federal Register paths) may still block. If a fetch fails, fall back
  to user-paste.
- ~5-15 seconds per page (browser startup + render).
"""
from __future__ import annotations

import argparse
import sys
from typing import Optional

# Windows default stdout is cp1252, which chokes on Unicode (FontAwesome
# private-use glyphs, em-dashes, etc.). Force UTF-8 so the fetched text
# pipes cleanly through tee/redirect/clip.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)


def fetch(
    url: str,
    selector: Optional[str] = None,
    raw_html: bool = False,
    wait_seconds: float = 1.0,
    timeout_ms: int = 30000,
) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            ctx = browser.new_context(
                user_agent=UA,
                viewport={"width": 1366, "height": 900},
                locale="en-US",
            )
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            # Extra settle time for JS-rendered content (Cloudflare challenges,
            # late-loaded text). 1s default; bump with --wait for stubborn pages.
            if wait_seconds > 0:
                page.wait_for_timeout(int(wait_seconds * 1000))
            if selector:
                el = page.query_selector(selector)
                if el is None:
                    raise RuntimeError(f"selector {selector!r} not found")
                return el.inner_html() if raw_html else el.inner_text()
            if raw_html:
                return page.content()
            body = page.query_selector("body")
            return body.inner_text() if body else ""
        finally:
            browser.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Headless Chromium fetcher")
    ap.add_argument("url")
    ap.add_argument(
        "--selector",
        help="CSS selector to scope output (default: body)",
    )
    ap.add_argument(
        "--html",
        action="store_true",
        help="Return raw HTML instead of inner-text",
    )
    ap.add_argument(
        "--wait",
        type=float,
        default=1.0,
        help="Extra settle-time after DOMContentLoaded (seconds). "
        "Bump for sites with late-rendered JS content.",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=30000,
        help="Navigation timeout in ms (default 30000)",
    )
    args = ap.parse_args()
    try:
        out = fetch(
            args.url,
            selector=args.selector,
            raw_html=args.html,
            wait_seconds=args.wait,
            timeout_ms=args.timeout,
        )
    except PWTimeout as e:
        print(f"TIMEOUT: {e}", file=sys.stderr)
        return 4
    except Exception as e:
        print(f"FETCH ERROR: {e}", file=sys.stderr)
        return 3
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
