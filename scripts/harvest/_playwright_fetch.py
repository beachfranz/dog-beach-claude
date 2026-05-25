"""Playwright-backed HTML fetcher for the harvest framework.

Wraps the same pattern as scripts/fetch/fetch_html.py (codify's wrapper)
but uses `channel='chrome'` so we use the system-installed Chrome
instead of the bundled Playwright Chromium binary. This bypasses the
download failure that AVG Antivirus's HTTPS-MITM causes against
Playwright's CDN.

When to use:
  - operator_location_sources.shape = 'js_rendered' → use this directly
  - requests-based fetch returns 403 → fall through to this
    (per [[403-means-playwright-skip-ua-tricks]])

Cost: ~5-10s per page (browser startup + render). Cache or batch where
possible.

If you need this elsewhere in the codebase, consider promoting to
scripts/common/. Today it lives in scripts/harvest/ because that's
where it's used.
"""
from __future__ import annotations

from playwright.sync_api import sync_playwright

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)


def fetch_via_playwright(url: str, *, wait_seconds: float = 2.0,
                         timeout_ms: int = 45000) -> tuple[str | None, str | None]:
    """Fetch a URL via headless Chrome (system install).

    Returns (text, error). text is None on failure; error is None on success.
    Matches the shape of scripts.harvest.extract_locations.fetch_html() so
    they're interchangeable from a caller's perspective.

    `channel='chrome'` is critical here — we want the system Chrome, not
    the bundled chromium binary (which fails to download under AVG MITM).
    """
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(channel="chrome", headless=True)
            try:
                ctx = browser.new_context(
                    user_agent=UA,
                    viewport={"width": 1366, "height": 900},
                    locale="en-US",
                )
                page = ctx.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                if wait_seconds > 0:
                    page.wait_for_timeout(int(wait_seconds * 1000))
                body = page.query_selector("body")
                text = body.inner_text() if body else ""
                return text, None
            finally:
                browser.close()
    except Exception as e:
        return None, f"playwright_error: {e}"
