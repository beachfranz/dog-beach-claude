"""
eCFR API fetcher — pulls federal-regulation text directly via the
public eCFR developer API (no bot-detection, free, no key required).

The eCFR HTML pages are blocked from automated fetch, but the API
at ecfr.gov/api isn't. Documentation:
https://www.ecfr.gov/developers/documentation/api/v1

Usage:
    python scripts/fetch/fetch_ecfr.py 36 7 7.97           # title=36, part=7, section=7.97
    python scripts/fetch/fetch_ecfr.py 36 2 2.15
    python scripts/fetch/fetch_ecfr.py 36 7 7.97 --date 2025-01-01
    python scripts/fetch/fetch_ecfr.py 36 7 7.97 --xml     # raw XML

Outputs plain text by default (XML structure flattened). Use --xml for
the raw API response.

Honest limits:
- The eCFR API returns the *current* (or as-of-date) text. For
  historical CFR versions, the GovInfo bulk packages are authoritative.
- Some structural variations (subparts, etc.) require navigating the
  hierarchy fields the API returns.
"""
from __future__ import annotations

import argparse
import ssl
import sys
import urllib.request
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import date as date_cls
from typing import Optional

# Force UTF-8 stdout — CFR XML contains Unicode (§ symbols etc.).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Windows: Python's bundled OpenSSL trust store mis-verifies certs that
# Windows itself trusts (same issue that forces `-c http.sslBackend=schannel`
# on git push). Use `truststore` to validate against the Windows native
# cert store via SChannel — same trust chain the OS uses. Falls back to
# certifi's Mozilla bundle if truststore isn't available, then to the
# OpenSSL default.
try:
    import truststore  # type: ignore

    truststore.inject_into_ssl()
    _SSL_CTX = ssl.create_default_context()
except ImportError:
    try:
        import certifi  # type: ignore

        _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        _SSL_CTX = ssl.create_default_context()


def _latest_date_for_title(title: int) -> str:
    """
    eCFR's `up_to_date_as_of` for the given title. The versioner /full/
    endpoint 404s on dates past this, so calling with today's date breaks
    once the system clock advances past the upstream's latest update.
    """
    url = "https://www.ecfr.gov/api/versioner/v1/titles"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "dog-beach-claude/law-as-primary-source-ca"},
    )
    with urllib.request.urlopen(req, timeout=15, context=_SSL_CTX) as resp:
        import json

        data = json.loads(resp.read())
    for t in data.get("titles", []):
        if t.get("number") == title:
            return t.get("up_to_date_as_of") or t.get("latest_issue_date") or date_cls.today().isoformat()
    return date_cls.today().isoformat()


def fetch_xml(
    title: int,
    part: int,
    section: str,
    as_of: Optional[str] = None,
    chapter: str = "I",
) -> str:
    """
    Fetch a CFR section via the eCFR versioner full XML endpoint.

    Endpoint pattern:
        /api/versioner/v1/full/{date}/title-{title}.xml?chapter={chapter}&part={part}&section={section}

    Defaults chapter to "I" which covers the vast majority of our
    targets (Title 36 Chapter I = NPS, Title 50 Chapter I = USFWS).
    Override via `chapter` parameter for less common titles.

    `as_of` defaults to the title's `up_to_date_as_of` date from the
    titles endpoint — using today's literal date 404s when the system
    clock has advanced past upstream's latest update.

    Returns the raw XML string for the requested section.
    """
    when = as_of or _latest_date_for_title(title)
    qs = urllib.parse.urlencode(
        {"chapter": chapter, "part": str(part), "section": str(section)}
    )
    url = f"https://www.ecfr.gov/api/versioner/v1/full/{when}/title-{title}.xml?{qs}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "dog-beach-claude/law-as-primary-source-ca research script",
            "Accept": "application/xml, text/xml",
        },
    )
    with urllib.request.urlopen(req, timeout=30, context=_SSL_CTX) as resp:
        return resp.read().decode("utf-8")


def xml_to_text(xml_str: str) -> str:
    """Flatten the CFR XML to readable text. Preserves paragraph breaks."""
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError as e:
        return f"<XML parse error: {e}>\n\n{xml_str[:500]}"

    out = []

    def walk(el, depth=0):
        # Capture section/subsection headers, then body text.
        tag = el.tag.lower()
        if tag in {"head", "subject", "title"}:
            text = (el.text or "").strip()
            if text:
                out.append(text)
                out.append("")
        elif tag == "p":
            # Concatenate <P> with any nested element text in order.
            pieces = []
            if el.text:
                pieces.append(el.text)
            for child in el:
                pieces.append(_inner_text(child))
                if child.tail:
                    pieces.append(child.tail)
            line = " ".join(p for p in pieces if p).strip()
            if line:
                out.append(line)
                out.append("")
            return  # don't recurse — children already consumed
        for child in el:
            walk(child, depth + 1)

    def _inner_text(el):
        parts = []
        if el.text:
            parts.append(el.text)
        for c in el:
            parts.append(_inner_text(c))
            if c.tail:
                parts.append(c.tail)
        return "".join(parts)

    walk(root)
    return "\n".join(out).strip()


def main() -> int:
    ap = argparse.ArgumentParser(description="eCFR API fetcher")
    ap.add_argument("title", type=int, help="CFR title number, e.g. 36")
    ap.add_argument("part", type=int, help="Part number, e.g. 7")
    ap.add_argument(
        "section",
        help="Section number (string, e.g. '7.97' or '2.15')",
    )
    ap.add_argument(
        "--date",
        help="As-of date YYYY-MM-DD (default: today)",
    )
    ap.add_argument(
        "--chapter",
        default="I",
        help='Chapter letter (default: "I" — covers most NPS / USFWS regs)',
    )
    ap.add_argument(
        "--xml",
        action="store_true",
        help="Return raw XML response (default: flatten to text)",
    )
    args = ap.parse_args()

    try:
        xml_str = fetch_xml(
            args.title,
            args.part,
            args.section,
            as_of=args.date,
            chapter=args.chapter,
        )
    except urllib.error.HTTPError as e:
        print(f"HTTP ERROR {e.code}: {e.reason}", file=sys.stderr)
        return 3
    except urllib.error.URLError as e:
        print(f"NETWORK ERROR: {e.reason}", file=sys.stderr)
        return 3

    if args.xml:
        print(xml_str)
    else:
        print(xml_to_text(xml_str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
