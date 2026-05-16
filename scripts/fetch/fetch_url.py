"""
Auto-routing URL fetcher for the law-as-primary-source-ca initiative.

Routes requests to the right backend based on the URL host:
- ecfr.gov / federalregister.gov   -> eCFR API
- leginfo.legislature.ca.gov       -> fetch_leginfo (Playwright + selector)
- ecode360.com                     -> fetch_ecode360 (Playwright + selector)
- library.municode.com             -> fetch_municode (often fails — see module)
- Everything else                  -> Headless Chromium (fetch_html)

Usage:
    python scripts/fetch/fetch_url.py <URL>                 # text output
    python scripts/fetch/fetch_url.py <URL> --selector main # CSS-scoped
    python scripts/fetch/fetch_url.py <URL> --html          # raw HTML/XML

For eCFR URLs of the form
  https://www.ecfr.gov/current/title-36/chapter-I/part-7/section-7.97
this parses out (title, part, section) and calls the API.

Sites that the WebFetch default fails on (ebparks.org, ecode360,
parks.ca.gov in some PDF cases, OC Register, etc.) route to the
Playwright backend and generally render successfully.

Honest limits:
- Cloudflare Turnstile / advanced bot-management can still block.
  If you get a captcha or interstitial page in the output, fall back
  to user-paste.
- The eCFR URL-to-API mapping handles section-level URLs cleanly;
  appendix or non-section paths may need direct fetch_ecfr usage.
"""
from __future__ import annotations

import argparse
import re
import sys
from urllib.parse import urlparse

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Local imports — these are sibling modules in scripts/fetch/
import fetch_html
import fetch_ecfr
import fetch_leginfo
import fetch_ecode360
import fetch_municode


def is_ecfr(url: str) -> bool:
    host = urlparse(url).hostname or ""
    return host.endswith("ecfr.gov")


def is_leginfo(url: str) -> bool:
    host = urlparse(url).hostname or ""
    return host.endswith("leginfo.legislature.ca.gov")


def is_ecode360(url: str) -> bool:
    host = urlparse(url).hostname or ""
    return host.endswith("ecode360.com")


def is_municode(url: str) -> bool:
    host = urlparse(url).hostname or ""
    return "municode.com" in host


def parse_leginfo_url(url: str):
    """Extract (lawCode, sectionNum) from a leginfo URL."""
    from urllib.parse import parse_qs
    qs = parse_qs(urlparse(url).query)
    law = (qs.get("lawCode", [None])[0] or "").upper()
    section = qs.get("sectionNum", [None])[0]
    if law and section:
        return law, section
    return None


def parse_ecode360_url(url: str):
    """Extract nodeId from an ecode360 URL: /<nodeId> or /<nodeId>#..."""
    path = urlparse(url).path.strip("/")
    if path and path.split("/")[0].isdigit():
        return path.split("/")[0]
    return None


def parse_ecfr_section_url(url: str):
    """
    Match patterns like:
      /current/title-36/chapter-I/part-7/section-7.97
      /current/title-36/section-2.15
    Returns (title:int, part:int, section:str) or None.
    """
    path = urlparse(url).path
    m_title = re.search(r"/title-(\d+)", path)
    m_part = re.search(r"/part-(\d+)", path)
    m_sec = re.search(r"/section-([\d.]+)", path)
    if not (m_title and m_sec):
        return None
    title = int(m_title.group(1))
    section = m_sec.group(1)
    # Infer part from section number (e.g. 7.97 -> part 7) if not explicit.
    if m_part:
        part = int(m_part.group(1))
    else:
        part_match = re.match(r"^(\d+)\.", section)
        if not part_match:
            return None
        part = int(part_match.group(1))
    return title, part, section


def main() -> int:
    ap = argparse.ArgumentParser(description="Auto-routing fetcher")
    ap.add_argument("url")
    ap.add_argument("--selector", help="CSS selector (Playwright path only)")
    ap.add_argument(
        "--html",
        action="store_true",
        help="Return raw HTML/XML instead of inner-text",
    )
    ap.add_argument("--wait", type=float, default=1.0)
    ap.add_argument(
        "--date",
        help="As-of date for eCFR (YYYY-MM-DD); ignored for Playwright path",
    )
    args = ap.parse_args()

    if is_ecfr(args.url):
        parsed = parse_ecfr_section_url(args.url)
        if not parsed:
            print(
                "WARNING: ecfr.gov URL didn't match a section pattern; "
                "falling back to Playwright (may still 403).",
                file=sys.stderr,
            )
        else:
            title, part, section = parsed
            try:
                xml_str = fetch_ecfr.fetch_xml(title, part, section, as_of=args.date)
            except Exception as e:
                print(f"eCFR API ERROR: {e}", file=sys.stderr)
                return 3
            if args.html:
                print(xml_str)
            else:
                print(fetch_ecfr.xml_to_text(xml_str))
            return 0

    if is_leginfo(args.url):
        parsed = parse_leginfo_url(args.url)
        if parsed:
            law, section = parsed
            try:
                print(fetch_leginfo.fetch_section(law, section, wait_seconds=max(args.wait, 3.0)))
            except Exception as e:
                print(f"leginfo ERROR: {e}", file=sys.stderr)
                return 3
            return 0
        # Fall through to default Playwright for non-section leginfo paths

    if is_ecode360(args.url):
        node_id = parse_ecode360_url(args.url)
        if node_id:
            try:
                print(fetch_ecode360.fetch_node(node_id, wait_seconds=max(args.wait, 6.0)))
            except Exception as e:
                print(f"ecode360 ERROR: {e}", file=sys.stderr)
                return 3
            return 0

    if is_municode(args.url):
        # Municode is bot-protected; try and warn on failure.
        try:
            out = fetch_html.fetch(
                args.url,
                selector="#content",
                raw_html=args.html,
                wait_seconds=max(args.wait, 12.0),
                timeout_ms=60000,
            )
        except Exception as e:
            print(f"municode ERROR: {e}", file=sys.stderr)
            return 3
        print(out)
        if "requested content cannot be found" in out:
            print(
                "[!] Municode bot-protection — try CPRA or browser-paste",
                file=sys.stderr,
            )
            return 5
        return 0

    # Default: Playwright
    try:
        out = fetch_html.fetch(
            args.url,
            selector=args.selector,
            raw_html=args.html,
            wait_seconds=args.wait,
        )
    except Exception as e:
        print(f"FETCH ERROR: {e}", file=sys.stderr)
        return 3
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
