"""
main.py -- Microsoft Learn cert page -> per-module + per-LP PDFs.

Scrapes Microsoft Learn modules into self-contained PDFs for offline study.

Output layout:
    output/DP-700-fabric-data-engineer-associate/
    ├── DP700-D1-Ingest data with Microsoft Fabric.pdf       (combined LP)
    ├── DP700-D2-Implement a Lakehouse with Microsoft Fabric.pdf
    ├── ...
    ├── D1-Ingest data with Microsoft Fabric/
    │   ├── D1M1-Ingest Data with Dataflows Gen2.pdf         (per module)
    │   ├── D1M2-Orchestrate processes and data movement.pdf
    │   └── ...
    ├── D2-Implement a Lakehouse with Microsoft Fabric/      (7 modules)
    └── How To Certyze.pdf                                   (study guide)

Setup:
    pip install -r requirements.txt
    playwright install chromium

Usage:
    python main.py
    python main.py "https://learn.microsoft.com/en-us/credentials/certifications/<cert-slug>/"
"""
from __future__ import annotations
import argparse, asyncio, base64, re, shutil, sys, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Comment
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright


# ============================================================================
# Config
# ============================================================================
SCRIPT_DIR = Path(__file__).resolve().parent
OUT_BASE   = SCRIPT_DIR / "output"

LEARN_BASE = "https://learn.microsoft.com/en-us/training/modules"

CERT_URL_RE = re.compile(
    r"^https?://learn\.microsoft\.com/[^/]+/credentials/certifications/([a-z0-9-]+)/?",
    re.IGNORECASE,
)
EXAM_CODE_RE = re.compile(r"\b([A-Z]{2,4}-\d{3,4})\b")

RENDER_CONCURRENCY     = 4
PAGE_LOAD_TIMEOUT_MS   = 60_000
REQUEST_DELAY_S        = 0.5   # polite delay between unit-page fetches

SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# urllib3 backoff schedule: factor * (2 ** (n-1)) -> 1s, 2s, 4s, 8s, 16s.
# ~31s worst case across 5 attempts. Retried statuses cover transient WAF
# pushback (429) and upstream gateway errors; Retry-After is honored.
RETRY_TOTAL           = 5
RETRY_BACKOFF_FACTOR  = 1.0
RETRY_STATUS_CODES    = (408, 429, 500, 502, 503, 504)
RETRY_ALLOWED_METHODS = frozenset(["GET", "HEAD"])


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(SCRAPE_HEADERS)
    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=RETRY_ALLOWED_METHODS,
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


_SESSION = _build_session()


# Pre-fetch each <img> and inline it as a base64 data URI before Chromium
# renders the PDF. Eliminates the file:// + remote-fetch race that produced
# silent visual holes when CDN images were still in flight at render time.
INLINE_IMAGE_MAX_BYTES = 16 * 1024 * 1024  # MS Learn diagrams routinely 5-10 MB
INLINE_IMAGE_WORKERS   = 8
INLINE_IMAGE_TIMEOUT_S = 30

# Process-scoped URL -> data-URI cache. None = permanent failure (don't retry).
# Best-effort dedupe: concurrent fetches of the same novel URL may both run.
_IMAGE_CACHE: dict[str, str | None] = {}

# Image-inline warnings get queued here instead of printed mid-LP, since the
# render loop holds an open progress line ([N/M] LP title ... ). The LP loop
# drains this list after each LP completes, so warnings stay grouped under
# the LP that triggered them. list.append is GIL-safe for the 8-thread pool.
_PENDING_IMG_WARNINGS: list[str] = []

# End-of-run sanity check. One tiny PDF is fine (some intros are short);
# more than SUSPICIOUS_PDF_LIMIT is a systemic regression worth flagging.
SUSPICIOUS_PDF_BYTES = 30 * 1024
SUSPICIOUS_PDF_LIMIT = 3


# ============================================================================
# Phase 1: Cert URL -> learning paths -> modules (Playwright scrape)
# ============================================================================
def parse_cert_url(url: str) -> str:
    """Return the cert slug from a Learn certification URL. Exits on bad input."""
    m = CERT_URL_RE.match(url.strip())
    if not m:
        raise SystemExit(
            f"\n[!] Input must be a Microsoft Learn certification URL like:\n"
            f"      https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/\n"
            f"    Got: {url!r}"
        )
    return m.group(1).lower()


def slugs_from_anchors(page, kind: str) -> list[str]:
    """Collect ordered, de-duplicated slugs from /training/<kind>/ anchors on `page`."""
    hrefs = page.eval_on_selector_all(
        f'a[href*="/training/{kind}/"]',
        "els => els.map(e => e.getAttribute('href'))",
    )
    seen, ordered = set(), []
    for href in hrefs or []:
        if not href:
            continue
        parts = urlparse(href).path.strip("/").split("/")
        for i, p in enumerate(parts):
            if p == kind and i + 1 < len(parts):
                slug = parts[i + 1]
                if slug not in seen:
                    seen.add(slug)
                    ordered.append(slug)
                break
    return ordered


def _safe_text(page, selector: str) -> str:
    try:
        return page.eval_on_selector(selector, "el => el.innerText") or ""
    except Exception:
        return ""


def scrape_cert_page(cert_url: str) -> dict:
    """Walk the cert page and each linked LP via Playwright.
    Returns {title, exam_code, lps: [{slug, title, modules: [slug, ...]}, ...]}."""
    print(f"[+] Scraping {cert_url}")
    out: dict = {"title": "", "exam_code": None, "lps": []}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            page = browser.new_context().new_page()
            page.goto(cert_url, wait_until="domcontentloaded")
            try:
                page.wait_for_selector('a[href*="/training/paths/"]', timeout=15_000)
            except Exception:
                raise SystemExit(f"\n[!] No learning paths found on {cert_url}")

            cert_title = _safe_text(page, "h1").strip() \
                or (page.title() or "").split(" | ")[0].strip()
            out["title"] = cert_title

            for sel in ("h1", "main", "body"):
                m = EXAM_CODE_RE.search(_safe_text(page, sel))
                if m:
                    out["exam_code"] = m.group(1)
                    break

            lp_slugs = slugs_from_anchors(page, "paths")
            cert_module_slugs = slugs_from_anchors(page, "modules")

            for lp_slug in lp_slugs:
                page.goto(f"https://learn.microsoft.com/en-us/training/paths/{lp_slug}/",
                          wait_until="domcontentloaded")
                try:
                    page.wait_for_selector('a[href*="/training/modules/"]', timeout=10_000)
                    mods = slugs_from_anchors(page, "modules")
                except Exception:
                    print(f"    [!] No modules on {lp_slug}")
                    mods = []
                lp_title = _safe_text(page, "h1").strip() or lp_slug
                # Strip the redundant cert-code prefix MS Learn often emits in
                # LP h1s ("AZ-104: Configure...") -- the cert folder already
                # carries the exam code, repeating it here just bloats paths.
                lp_title = re.sub(r'^[A-Z]{2,4}-\d{3,4}\s*:?\s*', '', lp_title)
                out["lps"].append({"slug": lp_slug, "title": lp_title, "modules": mods})
                print(f"    {lp_title} ({len(mods)} modules)")

            covered = {m for lp in out["lps"] for m in lp["modules"]}
            standalone = [m for m in cert_module_slugs if m not in covered]
            if standalone:
                print(f"    {len(standalone)} standalone module(s)")
                out["lps"].append({"slug": "_standalone", "title": "Standalone modules",
                                   "modules": standalone})
        finally:
            browser.close()

    return out


# ============================================================================
# Phase 2: PDF rendering
# ============================================================================
CSS_STYLE = """
@page { size: A4; margin: 0.9in 0.8in; }
body { font-family: -apple-system, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
       font-size: 10.5pt; line-height: 1.5; color: #222; max-width: 100%; }
h1 { font-size: 22pt; color: #0b3d91; border-bottom: 2px solid #0b3d91;
     padding-bottom: 6px; margin-top: 1.2em; page-break-before: always; }
h1:first-of-type { page-break-before: avoid; }
h2 { font-size: 15pt; color: #0b3d91; margin-top: 1.3em; }
h3 { font-size: 12pt; color: #333; margin-top: 1.1em; }
h4 { font-size: 11pt; color: #444; }
a  { color: #0b6cbe; text-decoration: none; }
code { font-family: "Cascadia Mono", "Consolas", "DejaVu Sans Mono", monospace;
       font-size: 9.5pt; background: #f4f4f4; padding: 1px 4px; border-radius: 3px; }
pre  { background: #f7f7f9; border: 1px solid #e0e0e6; border-left: 3px solid #0b6cbe;
       padding: 8px 10px; border-radius: 3px; overflow-x: auto;
       font-size: 9pt; line-height: 1.35; }
pre code { background: none; padding: 0; }
blockquote { border-left: 3px solid #f0a020; background: #fff8e8;
             padding: 6px 12px; margin: 0.6em 0; color: #5a4500; }
table { border-collapse: collapse; margin: 0.6em 0; font-size: 9.5pt; width: 100%; }
th, td { border: 1px solid #ccc; padding: 5px 8px; text-align: left; vertical-align: top; }
th { background: #eef3fa; }
img { max-width: 100%; height: auto; }
hr { border: none; border-top: 1px solid #ddd; margin: 1.2em 0; }
.section { margin-bottom: 1.4em; }
.section-header { background:#f5f5f5; padding:8px 12px; border-radius:4px;
                  margin-bottom:0.6em; }
.section-header a { color: #0078d4; word-break: break-all; font-size: 9pt; }
.NOTE, .TIP, .IMPORTANT, .WARNING, .CAUTION {
    padding: 8px 12px; margin: 0.7em 0; border-radius: 4px; border-left: 4px solid; }
.NOTE      { background: #e7f3ff; border-color: #0078d4; }
.NOTE      > p:first-child { font-weight: bold; color: #0078d4; margin-top: 0; }
.TIP       { background: #e8f5e9; border-color: #2e7d32; }
.TIP       > p:first-child { font-weight: bold; color: #2e7d32; margin-top: 0; }
.IMPORTANT { background: #fff4e5; border-color: #c25e00; }
.IMPORTANT > p:first-child { font-weight: bold; color: #c25e00; margin-top: 0; }
.WARNING, .CAUTION { background: #fdecea; border-color: #c62828; }
.WARNING > p:first-child, .CAUTION > p:first-child {
    font-weight: bold; color: #c62828; margin-top: 0; }
"""

PDF_FOOTER_TEMPLATE = (
    '<div style="font-size:8pt; width:100%; text-align:center; color:#666;">'
    '<span class="pageNumber"></span> / <span class="totalPages"></span>'
    '</div>'
)


def _wrap_doc(title: str, body_html: str) -> str:
    return (f'<!doctype html><html><head><meta charset="utf-8">'
            f'<title>{title}</title><style>{CSS_STYLE}</style></head>'
            f'<body>{body_html}</body></html>')


async def render_html_pdf_async(browser, body_html: str,
                                pdf_path: Path, title: str) -> None:
    """Render `body_html` directly to PDF via Chromium with no on-disk HTML.
    Uses page.set_content() so the wrapped HTML lives in memory only.
    A render failure (e.g. timeout) propagates to the caller."""
    page = await browser.new_page()
    try:
        await page.set_content(_wrap_doc(title, body_html),
                               wait_until="load", timeout=PAGE_LOAD_TIMEOUT_MS)
        await page.pdf(
            path=str(pdf_path),
            format="A4",
            margin={"top": "0.9in", "right": "0.8in",
                    "bottom": "1.0in", "left": "0.8in"},
            print_background=True,
            display_header_footer=True,
            header_template="<div></div>",
            footer_template=PDF_FOOTER_TEMPLATE,
        )
    finally:
        await page.close()


# ============================================================================
# Phase 3: Module scraper
# ============================================================================
def _fetch_soup(url: str) -> BeautifulSoup | None:
    """Fetch and parse `url` through the retry-equipped session.
    Returns None and logs once on connection error or final non-2xx status."""
    try:
        r = _SESSION.get(url, timeout=30)
    except requests.RequestException as e:
        print(f"\n    [!] fetch {url}: {e}")
        return None
    if not r.ok:
        print(f"\n    [!] fetch {url}: HTTP {r.status_code}")
        return None
    return BeautifulSoup(r.content, "html.parser")


def _extract_unit_sort_key(url: str) -> int | float:
    m = re.search(r"/([^/]+)/?$", url.rstrip("/"))
    if m:
        num_match = re.match(r"^(\d+)", m.group(1))
        if num_match:
            return int(num_match.group(1))
    return float("inf")


def _extract_h1(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""


# Substring patterns for feedback/rating widgets, metadata strips, and MS promo
# wrappers. Regex (not exact selectors) so we survive MS Learn class-name churn.
_CHROME_CLASS_RE = re.compile(r"feedback|rating|metadata|background-color-body")


def _clean_chrome(content_root) -> None:
    """Strip chrome from the extracted root: HTML comments, nav/aside/footer,
    role=navigation elements, and any element whose class matches the chrome regex."""
    for c in content_root.find_all(string=lambda s: isinstance(s, Comment)):
        c.extract()
    for tag in content_root.find_all(["nav", "aside", "footer"]):
        tag.decompose()
    for tag in content_root.find_all(attrs={"role": "navigation"}):
        tag.decompose()
    for tag in content_root.find_all(class_=_CHROME_CLASS_RE):
        tag.decompose()


def _fix_image_urls(content_div, base_url: str) -> None:
    """Rewrite relative <img src> and srcset URLs to absolute against `base_url`."""
    for img in content_div.find_all("img"):
        src = img.get("src")
        if src:
            img["src"] = urljoin(base_url, src)
        srcset = img.get("srcset")
        if srcset:
            new_srcset = []
            for item in srcset.split(","):
                parts = item.strip().split(" ")
                if parts:
                    parts[0] = urljoin(base_url, parts[0])
                    new_srcset.append(" ".join(parts))
            img["srcset"] = ", ".join(new_srcset)


def _fetch_image_data_uri(url: str) -> str | None:
    """Fetch `url` and return a base64 data URI, or None on failure (with cache).
    Failures are queued into _PENDING_IMG_WARNINGS for the LP loop to drain
    after the in-progress LP line finishes."""
    if url in _IMAGE_CACHE:
        return _IMAGE_CACHE[url]
    try:
        r = _SESSION.get(url, timeout=INLINE_IMAGE_TIMEOUT_S)
    except requests.RequestException as e:
        _PENDING_IMG_WARNINGS.append(f"    [!] inline image {url}: {e}")
        _IMAGE_CACHE[url] = None
        return None
    if not r.ok:
        _PENDING_IMG_WARNINGS.append(f"    [!] inline image {url}: HTTP {r.status_code}")
        _IMAGE_CACHE[url] = None
        return None
    if len(r.content) > INLINE_IMAGE_MAX_BYTES:
        _PENDING_IMG_WARNINGS.append(
            f"    [!] inline image {url}: {len(r.content)} bytes exceeds cap, skipping"
        )
        _IMAGE_CACHE[url] = None
        return None
    mime = (r.headers.get("Content-Type") or "image/png").split(";")[0].strip()
    data_uri = f"data:{mime};base64,{base64.b64encode(r.content).decode('ascii')}"
    _IMAGE_CACHE[url] = data_uri
    return data_uri


def _inline_images(content_div) -> None:
    """Replace every remote <img src> with a base64 data URI and drop srcset.
    Per-image failure leaves the original src in place; the image tag stays."""
    targets: list[tuple] = []
    for img in content_div.find_all("img"):
        if img.get("srcset"):
            del img["srcset"]
        src = img.get("src")
        if not src or src.startswith("data:") or not src.startswith(("http://", "https://")):
            continue
        targets.append((img, src))
    if not targets:
        return
    urls = [t[1] for t in targets]
    with ThreadPoolExecutor(max_workers=INLINE_IMAGE_WORKERS) as ex:
        results = list(ex.map(_fetch_image_data_uri, urls))
    for (img, _), data_uri in zip(targets, results):
        if data_uri is not None:
            img["src"] = data_uri


def _extract_main_html(soup: BeautifulSoup, base_url: str) -> str:
    """Pick the article root, strip chrome, absolutize + inline images, return HTML."""
    content_root = (
        soup.find("main")
        or soup.find("article")
        or soup.find("div", class_="content")
    )
    if not content_root:
        return f"<p><i>(content not found at {base_url})</i></p>"
    _clean_chrome(content_root)
    _fix_image_urls(content_root, base_url)
    _inline_images(content_root)
    return str(content_root)


def scrape_module_html(slug: str) -> tuple[str, str, int] | None:
    """Scrape a module from learn.microsoft.com.
    Returns (title, body_html, units_failed) or None on module-level failure.
    Per-unit failures are non-fatal: marker stays in body_html, counter ticks."""
    module_url = f"{LEARN_BASE}/{slug}/"
    soup = _fetch_soup(module_url)
    if soup is None:
        return None

    title = _extract_h1(soup) or slug

    unit_links = set()
    for a in soup.find_all("a", href=True):
        full = urljoin(module_url, a["href"])
        parsed = urlparse(full)
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if clean.startswith(module_url) and clean != module_url:
            unit_links.add(clean)
    unit_urls = sorted(unit_links, key=_extract_unit_sort_key)

    if not unit_urls:
        print(f"\n    [!] {slug}: no unit pages found on {module_url}")
        return None

    units_failed = 0
    body_parts = [f"<h1>{title}</h1>"]
    for i, url in enumerate(unit_urls, start=1):
        if REQUEST_DELAY_S:
            time.sleep(REQUEST_DELAY_S)
        u_soup = _fetch_soup(url)
        if u_soup is None:
            units_failed += 1
            body_parts.append(f'<div class="section"><h2>{i}. (failed to load)</h2>'
                              f'<a href="{url}">{url}</a></div>')
            continue
        u_title = _extract_h1(u_soup) or url.rsplit("/", 1)[-1]
        u_html = _extract_main_html(u_soup, url)
        body_parts.append(
            f'<div class="section">'
            f'<div class="section-header"><h2>{i}. {u_title}</h2>'
            f'<a href="{url}">{url}</a></div>'
            f'<div class="content">{u_html}</div>'
            f'</div>'
        )

    return title, "\n".join(body_parts), units_failed


# ============================================================================
# Filename / dirname sanitization
# ============================================================================
def safe_dirname(name: str) -> str:
    """Sanitize for use as a directory name (Windows-safe, capped at 80 chars)."""
    name = re.sub(r'[<>:"/\\|?*]', "_", name).strip(" .")
    # Re-strip after the 80-char cap: truncation can leave a trailing space or
    # dot mid-word, which Windows silently strips when creating the directory
    # but Python keeps in memory -- causing WinError 3 on subsequent path joins.
    return name[:80].rstrip(" .") or "output"


def safe_filename(name: str) -> str:
    """Sanitize a title for use as a file STEM (no extension). Capped at 60 chars."""
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', " ", name)
    name = re.sub(r"\s*[,:;]\s*", "-", name)
    name = re.sub(r"\s+", " ", name).strip(" .-_")
    return name[:60] or "untitled"


# ============================================================================
# readme drop (CloudWalker branding artifact)
# ============================================================================
# Source-of-truth lives at project root. Copied as-is into each cert folder so
# a future swap to readme.pdf is a one-line constant change.
README_SOURCE = SCRIPT_DIR / "How To Certyze.pdf"


def drop_readme(out_dir: Path) -> Path | None:
    """Copy README_SOURCE into out_dir. Logs and returns None if source is missing
    -- a missing readme should not kill an otherwise-successful render."""
    if not README_SOURCE.exists():
        print(f"    [!] {README_SOURCE.name} not found at {README_SOURCE.parent}, skipping copy")
        return None
    dest = out_dir / README_SOURCE.name
    shutil.copy2(README_SOURCE, dest)
    return dest


def zip_output(out_dir: Path) -> Path:
    """Bundle out_dir as a sibling .zip and delete the source folder.
    The .zip is the canonical artifact -- shipping both fills the user's disk
    and invites them to redistribute the unzipped tree (which is what hits
    MAX_PATH on copy in the first place). Overwrites existing <name>.zip on
    re-run. Caller decides whether to call (e.g. gate on _check_pdf_sizes)."""
    zip_path = Path(shutil.make_archive(
        base_name=str(out_dir),
        format="zip",
        root_dir=str(out_dir.parent),
        base_dir=out_dir.name,
    ))
    shutil.rmtree(out_dir)
    return zip_path


# ============================================================================
# Render loop
# ============================================================================
async def _render_one_module(browser, sem: asyncio.Semaphore,
                             di: int, mi: int, mslug: str,
                             lp_dir: Path
                             ) -> tuple[int, str | None, bool, int]:
    """Scrape + render one module under the semaphore.
    Returns (module_index, body_html_or_None, render_succeeded, units_failed)."""
    async with sem:
        scraped = await asyncio.to_thread(scrape_module_html, mslug)
        if not scraped:
            return (mi, None, False, 0)
        mtitle, body_html, units_failed = scraped
        stem = f"D{di}M{mi}-{safe_filename(mtitle)}"
        try:
            await render_html_pdf_async(browser, body_html,
                                        lp_dir / f"{stem}.pdf", mtitle)
        except Exception as e:
            print(f"\n    [!] {mslug} PDF render failed: {e}")
            return (mi, None, False, units_failed)
        return (mi, body_html, True, units_failed)


async def render_all(plan: list[dict], out_dir: Path) -> tuple[int, int]:
    """Render every LP in `plan`: per-module PDFs plus one combined per-LP PDF.
    Returns (pdf_count, total_units_failed)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_count = total_units_failed = 0
    print(f"\n[+] Rendering PDFs")
    sem = asyncio.Semaphore(RENDER_CONCURRENCY)

    # Derive an exam-code prefix (e.g. "DP800-") from out_dir name so combined
    # LP PDFs at root are self-identifying when shared/uploaded to NotebookLM
    # alongside other certs. Cert folder is bare exam code ("DP-800") -> prefix
    # "DP800-"; trailing-dash form is kept matchable for user-supplied --out.
    # Empty string when no exam code is parseable from the cert slug.
    exam_match = re.match(r"^([A-Z]{2,4})-(\d{3,4})(?:$|-)", out_dir.name)
    exam_prefix = f"{exam_match.group(1)}{exam_match.group(2)}-" if exam_match else ""

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        try:
            for di, lp in enumerate(plan, start=1):
                modules = lp.get("modules", [])
                if not modules:
                    print(f"    [{di}/{len(plan)}] {lp['lp_title']} (0 modules) ... skipped")
                    continue
                lp_start = time.time()
                print(f"    [{di}/{len(plan)}] {lp['lp_title']} ({len(modules)} modules) ... ",
                      end="", flush=True)

                # Combined per-LP PDFs land at out_dir root with the exam-code
                # prefix for one-shot NotebookLM upload (drag all DPxxx-D{N}-...
                # pdf files at once). Per-module PDFs stay in D{N}-<lp-title>/
                # folders alongside, no exam prefix (parent folder gives context).
                lp_dir = out_dir / safe_dirname(f"D{di}-{lp['lp_title']}")
                lp_dir.mkdir(parents=True, exist_ok=True)

                tasks = [
                    _render_one_module(browser, sem, di, mi, mslug, lp_dir)
                    for mi, mslug in enumerate(modules, start=1)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                ordered_bodies: list[tuple[int, str]] = []
                for r in results:
                    if isinstance(r, BaseException):
                        print(f"\n    [!] task error: {r}")
                        continue
                    mi_r, body, ok, units_failed = r
                    total_units_failed += units_failed
                    if not ok:
                        continue
                    pdf_count += 1
                    if body:
                        ordered_bodies.append((mi_r, body))

                ordered_bodies.sort(key=lambda x: x[0])
                domain_title = f"Domain {di}: {lp['lp_title']}"
                combined_body = (
                    f"<h1>{domain_title}</h1>\n"
                    + "\n".join(b for _, b in ordered_bodies)
                )
                stem = f"{exam_prefix}D{di}-{safe_filename(lp['lp_title'])}"
                try:
                    await render_html_pdf_async(browser, combined_body,
                                                out_dir / f"{stem}.pdf", domain_title)
                    pdf_count += 1
                except Exception as e:
                    print(f"\n    [!] combined LP PDF failed: {e}")
                print(f"{time.time() - lp_start:.0f}s")
                # Drain any image-inline warnings queued during this LP so they
                # render BELOW the LP timing line, not as mid-line interruptions.
                while _PENDING_IMG_WARNINGS:
                    print(_PENDING_IMG_WARNINGS.pop(0))
        finally:
            await browser.close()

    return pdf_count, total_units_failed


# ============================================================================
# Entry point
# ============================================================================
def _check_pdf_sizes(out_dir: Path) -> tuple[int, int, int]:
    """Print alarm block (if triggered) and return (suspicious_count, smallest, largest).
    Caller surfaces suspicious_count in the final summary line."""
    pdfs = list(out_dir.rglob("*.pdf"))
    if not pdfs:
        return (0, 0, 0)
    sizes = [(p, p.stat().st_size) for p in pdfs]
    smallest = min(sz for _, sz in sizes)
    largest  = max(sz for _, sz in sizes)
    small = [(p, sz) for p, sz in sizes if sz < SUSPICIOUS_PDF_BYTES]
    if len(small) > SUSPICIOUS_PDF_LIMIT:
        print(f"\n[!] {len(small)} PDFs under {SUSPICIOUS_PDF_BYTES // 1024} KB "
              f"(threshold > {SUSPICIOUS_PDF_LIMIT}). Likely silent regression:")
        for p, sz in sorted(small, key=lambda x: x[1]):
            print(f"    {sz/1024:>5.0f} KB  {p.relative_to(out_dir).as_posix()}")
        return (len(small), smallest, largest)
    return (0, smallest, largest)


def _format_duration(seconds: float) -> str:
    """Render a wall-clock duration as '12s' or '2m 47s'."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    return f"{seconds // 60:.0f}m {seconds % 60:.0f}s"


def main():
    run_start = time.time()

    ap = argparse.ArgumentParser(
        description="Cert -> PDFs via learn.microsoft.com scraping")
    ap.add_argument("input", nargs="?", help="Microsoft Learn cert URL")
    ap.add_argument("--out", help="Override output dir")
    args = ap.parse_args()

    cert_url = args.input or input(
        "Paste a cert URL (https://learn.microsoft.com/.../credentials/certifications/<slug>/): "
    ).strip()
    if not cert_url:
        sys.exit("No input given.")

    cert_slug = parse_cert_url(cert_url)
    scraped = scrape_cert_page(cert_url)

    if args.out:
        out_dir = Path(args.out)
    else:
        dirname = scraped.get("exam_code") or cert_slug
        out_dir = OUT_BASE / safe_dirname(dirname)

    plan = [
        {"lp_slug": lp["slug"], "lp_title": lp["title"], "modules": lp["modules"]}
        for lp in scraped["lps"]
    ]
    total_modules = sum(len(lp["modules"]) for lp in plan)

    title_line = (
        f"{scraped['exam_code']}: {scraped['title']}"
        if scraped.get("exam_code") else scraped['title']
    )
    print(f"\n[+] {title_line}")
    print(f"    {len(plan)} learning paths, {total_modules} modules")

    if not any(lp["modules"] for lp in plan):
        sys.exit("\n[!] No modules to render.")

    pdf_count, units_failed = asyncio.run(render_all(plan, out_dir))

    drop_readme(out_dir)

    suspicious, _, _ = _check_pdf_sizes(out_dir)
    suffix = f" ({suspicious} suspicious -- see [!] above)" if suspicious else ""
    elapsed = _format_duration(time.time() - run_start)
    print(f"\n[done] {pdf_count} PDFs in {elapsed}{suffix}")
    if units_failed:
        print(f"       units failed: {units_failed}")

    # ZIP gated on the suspicious-PDF alarm: on a clean run zip_output also
    # deletes the source folder, so .zip is the only artifact left on disk.
    # If the alarm fires we keep the folder for inspection and skip zipping.
    if suspicious:
        print(f"       {out_dir}")
        print("       [!] skipping ZIP -- investigate suspicious PDFs first")
    else:
        zip_path = zip_output(out_dir)
        print(f"       {zip_path}")


if __name__ == "__main__":
    main()
