---
name: cloudflare-fallback
description: Use this skill when a Python script using urllib / requests is hitting 403 Forbidden or 503 (often Cloudflare bot-detection) and you need to add a Playwright fallback. Triggers include "site returns 403", "Cloudflare is blocking", "amlegal.com / codepublishing / mass.gov / michigan.gov 403", "add Playwright fallback to <script>", "smart_fetch pattern", "this fetch keeps failing", or any new script that fetches from US municipal / state .gov URLs likely to be Cloudflare-protected. Owns the canonical pattern + the known-blocker host list. DO NOT use for: WebFetch (the Claude tool) returning errors — that's an environment limitation, not a code path you can patch; or for legitimate non-403 fetch failures (timeouts, DNS, malformed URLs).
---

# cloudflare-fallback — urllib → Playwright on 403/503

Whenever a Python script in this repo fetches from a `.gov` / `municode` / `amlegal` / `ecode360` URL, expect Cloudflare or a similar bot-detection layer to return **403 Forbidden** to default `urllib.request` / `requests` calls. The fix is **always Playwright**, never UA / Accept-Language / Referer tricks. Encoded as HARD rule `[[403-means-playwright-skip-ua-tricks]]`.

Two pieces in this repo:

| File | Purpose |
|---|---|
| `scripts/fetch/fetch_html.py` | Canonical Playwright fetcher. Exports `fetch(url, selector=None, raw_html=False, wait_seconds=1.0, timeout_ms=30000) -> str`. Uses bundled Chromium headless. |
| `scripts/extract_per_beach_offleash_v2.py:181` | Canonical `smart_fetch()` wrapper — urllib first, auto-escalates to Playwright on 403/503. Copy this pattern. |

There's also `scripts/harvest/_playwright_fetch.py` which uses `channel='chrome'` (system Chrome) for the AVG-MITM workaround per `[[avg-antivirus-https-mitm]]`. Use it if the bundled chromium can't launch — but the bundled chromium normally works in `.venv-pipeline`.

## When to apply

Add the smart_fetch pattern to any **new** script that fetches HTML from external URLs. Don't wait for a 403 in the wild — front-load it.

For **existing** scripts hitting 403s in production:
1. Check the host against the known-blocker list below
2. Either add it to the script's PLAYWRIGHT_HOSTS set (go straight to Playwright, skip urllib) OR rely on the auto-escalate path
3. Don't fiddle with User-Agent / headers — that won't help

## Canonical smart_fetch — drop into your script

```python
import urllib.request
import urllib.error
import urllib.parse

# Playwright fallback. Pin: [[403-means-playwright-skip-ua-tricks]].
try:
    from scripts.fetch.fetch_html import fetch as playwright_fetch  # type: ignore
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    playwright_fetch = None  # type: ignore
    PLAYWRIGHT_AVAILABLE = False

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# Known Cloudflare/JS-blocker hosts — go straight to Playwright, skip the
# urllib round-trip. Extend per the canonical list in cloudflare-fallback skill.
PLAYWRIGHT_HOSTS = {
    "mass.gov", "michigan.gov",
    # ... add per your script's domain
}


def smart_fetch(url: str, timeout: int = 30) -> str:
    """urllib first; auto-escalate to Playwright on 403/503 or known-block host."""
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    use_playwright = any(host.endswith(h.removeprefix("www.")) for h in PLAYWRIGHT_HOSTS)

    if use_playwright and PLAYWRIGHT_AVAILABLE:
        return playwright_fetch(url, raw_html=True, wait_seconds=2.0, timeout_ms=45000) or ""

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except urllib.error.HTTPError as e:
        if e.code in (403, 503) and PLAYWRIGHT_AVAILABLE:
            return playwright_fetch(url, raw_html=True, wait_seconds=4.0, timeout_ms=45000) or ""
        raise
```

For shapes that need the `(text, why)` tuple instead of raising, see `extract_per_beach_offleash_v2.py:181` — same logic, different return contract.

## Known-blocker hosts (Cloudflare or similar)

State `.gov` directories that 403 default urllib:

- `mass.gov` / `www.mass.gov` — confirmed 2026-06-08 on `/info-details/accessible-beaches`
- `michigan.gov` / `www.michigan.gov` — confirmed 2026-06-08 on `/dnr/about/accessibility/track-chairs`
- `parks.ca.gov` — confirmed in `extract_per_beach_offleash_v2.py`
- `sanjoseca.gov` — confirmed
- `cityofdavis.org` — confirmed
- `newportbeachca.gov` — confirmed

Municipal-code platforms:

- `amlegal.com` — confirmed across CA/OR/WA/MI/OH in operator-URL extractor 2026-06-08
- `codepublishing.com` — confirmed CA/OR/WA in operator-URL extractor 2026-06-08
- `municode.com` (sometimes works, sometimes 403; route to Playwright if reliability matters)
- `lacey.municipal.codes` — confirmed

JS-rendered SPAs that body-text returns empty without render:

- `sfrecpark.org` — JS SPA, needs `wait_seconds=6.0`
- `civicplus.com` (many cities use this) — needs `wait_seconds=6.0`
- `oaklandca.gov`, `alamedaca.gov`, `santaclaraca.gov`, `cityofsacramento.gov`, `elsegundorecparks.org`, `whittierprcs.org`, `southpasadenaca.gov`, `lagunabeachcity.net`, `redwoodcity.org`, `sanramon.ca.gov`, `downeyca.org`

Misc:

- `cityofcapitola.org`, `malibucity.org`, `beaches.lacounty.gov` — sometimes (AUTH_DOMAINS table in extract_per_beach_offleash_v2.py routes these)
- `cannon-beach.or.us` — confirmed in operator-URL extractor

## What NOT to do

Per `[[403-means-playwright-skip-ua-tricks]]`:

- **Don't iterate on User-Agent strings.** Cloudflare fingerprints TLS handshake + HTTP/2 behavior; UA doesn't matter.
- **Don't add `Accept-Language` / `Referer` / cookie tricks.** Same reason.
- **Don't try multiple `requests` retries with delays.** Wastes wall time; Cloudflare doesn't soften after N tries.
- **Don't conclude "this site is permanently blocked"** without trying Playwright first.
- **Don't add Playwright to** `extract_for_orphans.py` / `extract_operator.py` style scripts that already use the codify substrate — they delegate to `derive_policy_source_for_jurisdiction.py:_smart_fetch` which is platform-driven.

## Cost / latency

- Playwright fetch: 5-15s per page (browser startup + DOMContentLoaded + wait_seconds)
- Bundled chromium first launch in a fresh venv: extra ~10s on first call
- Use `wait_seconds=2.0` default; bump to 4-6s for stubborn SPAs (civicplus, sfrecpark)
- Bundle multiple fetches in one Python process to amortize browser startup if possible

## AVG / antivirus MITM caveat

If `playwright_fetch` fails to launch with cert errors on Franz's machine, swap to `scripts/harvest/_playwright_fetch.py:fetch_via_playwright()` which uses `channel='chrome'` (system Chrome) instead of the bundled Chromium. The bundled Chromium download path runs through AVG's HTTPS MITM and fails verification per `[[avg-antivirus-https-mitm]]`. Bundled chromium that's ALREADY installed in `.venv-pipeline` is fine — the MITM problem is at download/install time, not runtime.

## Cross-references

- `[[403-means-playwright-skip-ua-tricks]]` — HARD rule, parent of this skill
- `[[avg-antivirus-https-mitm]]` — AVG MITM detail
- `extract_per_beach_offleash_v2.py:181` — canonical smart_fetch implementation
- `scripts/derive_policy_source_for_jurisdiction.py:874` — platform-driven `_smart_fetch` for codify pipeline
- `scripts/extract_accessibility_from_state_directory.py` — most recent adopter (2026-06-08; mass.gov + michigan.gov)
