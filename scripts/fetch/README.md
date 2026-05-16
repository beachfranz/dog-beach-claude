# `scripts/fetch/` — 403-defeat fetchers for law-as-primary-source research

Three scripts for pulling primary-source text from sites that block
WebFetch (city code platforms, EBRPD, OC Register, ecfr.gov HTML, etc.).
Built for the [[law-as-primary-source-ca]] initiative — see
`docs/walkthrough_*.md` for what these fetch.

## Quickstart

```bash
# Federal regulations (uses eCFR public API — fast, clean XML)
python scripts/fetch/fetch_ecfr.py 36 7 7.97       # 36 CFR §7.97 (GGNRA)
python scripts/fetch/fetch_ecfr.py 36 2 2.15       # 36 CFR §2.15 (NPS pets)

# Any other URL (city codes, district sites, news articles)
python scripts/fetch/fetch_html.py https://www.ebparks.org/parks/crown-beach

# Auto-routes: ecfr.gov URLs → API, everything else → Playwright
python scripts/fetch/fetch_url.py "https://www.ecfr.gov/current/title-36/chapter-I/part-7/section-7.97"
python scripts/fetch/fetch_url.py https://www.ebparks.org/parks/crown-beach
```

## Why two backends

| Backend | When to use | Speed | Reliability |
|---|---|---|---|
| **eCFR API** (`fetch_ecfr.py`) | Federal CFR sections | ~1s | High — government API, no bot detection |
| **Playwright** (`fetch_html.py`) | Everything else | 5-15s | High for most sites; some Cloudflare-Enterprise-protected sites still block |

WebFetch (the AI-tool default) gets 403'd on a long list of sites we need
to read for this project — ecode360, municode, qcode, amlegal, ebparks,
parks.ca.gov PDFs, Federal Register HTML, ecfr.gov HTML, OC Register, LA
Times. These scripts route around that.

## Dependencies

Already installed in this environment (see also `requirements.txt` if
present):

- `playwright` (browser automation, includes a Chromium binary)
- `truststore` (Windows: uses the OS native cert store to avoid
  OpenSSL/SChannel trust mismatches — same fix as `git -c
  http.sslBackend=schannel`)
- `certifi` (fallback trust bundle when truststore isn't available)

If setting up fresh:

```bash
pip install playwright truststore certifi
playwright install chromium
```

## Known limits — when to fall back to user-paste

- **Cloudflare Turnstile** challenges — some Federal Register paths and
  a few city-code-platform deep links use Turnstile JS challenges that
  Playwright doesn't solve out of the box. If you see an interstitial
  page in the output, the user pastes the verbatim text instead.
- **Authenticated sources** — court PACER records, Westlaw/Lexis. No
  scripted access; manual extraction.
- **Government PDFs with binary streams** — some (like NPS Compendium
  PDFs and CFR-edition PDFs from GovInfo) don't render to text cleanly
  through any HTTP fetch. Read the file locally with a PDF tool or get
  the equivalent HTML / API representation.
- **Municode-hosted municipal codes** — Municode pages are JS-heavy
  single-page apps. Basic Playwright navigation lands on the shell
  page but the actual ordinance article body is rendered after a TOC
  click. Discovered 2026-05-16 while trying to extract EBRPD Ordinance
  38 from `library.municode.com/ca/east_bay_regional_park_district`.
  For Wave 3 (many CA cities on Municode), this likely needs a
  Municode-specific helper in `fetch_html.py` that waits for the TOC,
  clicks the target chapter, and extracts the rendered article.
  Pattern to implement: locate the chapter link in the TOC, click,
  wait for the article body selector, extract.

## Useful patterns discovered

- **Date-stamped filenames as metadata.** EBRPD's PDF ordinances
  encode the effective date in the filename (e.g.,
  `Ord38-09052023FINAL.pdf` = Sept 5, 2023). Exploiting filenames
  this way gives `last_amended_date` without opening the PDF.
  Worth checking for similar patterns on other government sites
  (city clerks, CDFW PDFs, NPS Compendiums) before parsing PDF
  text.
- **Site-internal links are reliable.** If a page mentions
  "VIEW ONLINE" / "Ordinance 38" / etc. but the surface text
  doesn't include the source URL, the HTML grep
  (`fetch_html.py --html | grep -oE 'href="[^"]+"'`) usually
  surfaces the canonical link cheaply.

## Auto-router URL detection

`fetch_url.py` routes based on hostname:

- `*.ecfr.gov` → API path, parses title/part/section from the URL
- everything else → Playwright

For eCFR URLs that don't match a section pattern (table-of-contents
pages, appendix paths), it falls back to Playwright with a stderr
warning.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 2 | Bad args |
| 3 | Fetch error (network, HTTP error, missing selector) |
| 4 | Timeout |

## Related

- `docs/walkthrough_hbdb.md`, `walkthrough_crystal_cove.md`,
  `walkthrough_fort_funston.md`, `walkthrough_crown_memorial.md` —
  beaches these scripts have been used against.
- Memory pin: [[law-as-primary-source-ca]] — strategy + CPRA-vs-scrape
  process notes.
