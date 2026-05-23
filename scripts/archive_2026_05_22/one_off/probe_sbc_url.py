"""Probe SBC's Off-Leash-Dog-Areas URL with extended Playwright settings.
Reports what content actually loads after various wait strategies."""
import re, sys
sys.stdout.reconfigure(encoding="utf-8")
from playwright.sync_api import sync_playwright

URL = "https://www.countyofsb.org/887/Off-Leash-Dog-Areas"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

def probe():
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(user_agent=UA, viewport={"width":1280,"height":1600})
        page = ctx.new_page()
        page.route("**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,otf,mp4,webm}",
                   lambda r: r.abort())
        page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception as e:
            print(f"networkidle timeout: {e}")
        # Extra wait
        page.wait_for_timeout(3000)
        text = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
        title = page.title()
        # Search for dog/leash/pet
        kw_count = len(re.findall(r"\b(dog|leash|pet)s?\b", text, re.I))
        # Print first 2000 chars
        print("=" * 60)
        print(f"TITLE: {title}")
        print(f"CHARS: {len(text)}  KEYWORD HITS: {kw_count}")
        print("=" * 60)
        print(text[:3000])
        print("=" * 60)
        # If keyword found, also dump the section
        m = re.search(r".{200}\b(dog|leash|pet)s?\b.{500}", text, re.I | re.S)
        if m:
            print("KEYWORD CONTEXT:")
            print(m.group(0))
        browser.close()

if __name__ == "__main__":
    probe()
