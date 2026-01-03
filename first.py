#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The Infatuation reviews scraper (multi-city, one JSON per city)

Features:
- --cities london,new-york (or --city london)
- Full extraction by default: opens /<city>/reviews, collects all /reviews/ links by scrolling
- Scrapes each detail page with Playwright
- Date filtering (inclusive): --start-date YYYY-MM-DD --end-date YYYY-MM-DD
- Incremental mode: skips items already in output JSON (by source_id)
- Image extraction: RELIABLE (HYBRID)
    * scroll a bit to trigger lazy load
    * parse page.content() with BeautifulSoup (like your working requests+bs4 script)
    * also scan DOM for <picture><source srcset> and <img> attrs
- Downloads images from Infatuation (Cloudinary) + Instagram CDN when present
- Records image source: theinfatuation / instagram / other
"""

import argparse
import asyncio
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout

BASE = "https://www.theinfatuation.com"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120 Safari/537.36"
)

# ---------- helpers ----------

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def ensure_dir(p: str) -> None:
    if p:
        os.makedirs(p, exist_ok=True)

def safe_filename(s: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s)
    return s.strip("._")[:180] or "file"

def parse_source_id(url: str) -> str:
    return urlparse(url).path.strip("/")

def normalize_url(u: Optional[str], page_url: str) -> str:
    if not u:
        return ""
    return urljoin(page_url, u)

def iso_date_from_time_attr(time_attr: Optional[str], time_text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns:
      source_created_at: YYYY-MM-DDT00:00:00
      published_date_text: human readable (from page)
    """
    published_date_text = clean_text(time_text or "") or None
    if time_attr:
        m = re.match(r"^\d{4}-\d{2}-\d{2}$", time_attr.strip())
        if m:
            return f"{time_attr}T00:00:00", published_date_text

    if published_date_text:
        try:
            dt = datetime.strptime(published_date_text, "%B %d, %Y")
            return dt.strftime("%Y-%m-%dT00:00:00"), published_date_text
        except Exception:
            pass

    return None, published_date_text

def in_inclusive_range(date_iso_yyyy_mm_dd: str, start: Optional[str], end: Optional[str]) -> bool:
    if not date_iso_yyyy_mm_dd:
        return False
    if start and date_iso_yyyy_mm_dd < start:
        return False
    if end and date_iso_yyyy_mm_dd > end:
        return False
    return True

async def get_text_or_none(page: Page, selector: str) -> Optional[str]:
    loc = page.locator(selector).first
    if await loc.count():
        return clean_text(await loc.inner_text())
    return None

async def get_attr_or_none(page: Page, selector: str, attr: str) -> Optional[str]:
    loc = page.locator(selector).first
    if await loc.count():
        v = await loc.get_attribute(attr)
        return v.strip() if isinstance(v, str) else v
    return None

async def maybe_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", clean_text(s))
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None

async def click_cookie_ok(page: Page, debug: bool = False) -> None:
    try:
        btn = page.locator('button:has-text("OK")').first
        if await btn.count():
            if debug:
                print('    [COOKIE] clicking button:has-text("OK")')
            await btn.click(timeout=3000)
            await page.wait_for_timeout(300)
    except Exception:
        pass

# ---------- address parsing (UK) ----------

UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", re.IGNORECASE)

def split_uk_address(addr: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    addr = clean_text(addr)
    if not addr:
        return None, None, None, None

    postal = None
    m = UK_POSTCODE_RE.search(addr.upper())
    if m:
        postal = clean_text(m.group(1).upper())

    parts = [clean_text(p) for p in addr.split(",") if clean_text(p)]
    street = parts[0] if parts else None
    city = None
    if len(parts) >= 2:
        city = parts[1]
    elif "London" in addr:
        city = "London"

    country = "GB"
    return street, city, postal, country

# ---------- image extraction + download ----------

def is_infatuation_cloudinary(url: str) -> bool:
    return "res.cloudinary.com/the-infatuation" in (url or "")

def is_instagram_cdn(url: str) -> bool:
    u = (url or "").lower()
    # include instagram.com too (some pages embed direct image URLs under instagram domains)
    return ("cdninstagram" in u) or ("fbcdn.net" in u) or ("instagram" in u)

def clean_cloudinary_url(url: str) -> str:
    # upgrade any w_### to w_3840
    return re.sub(r"w_\d+", "w_3840", url)

def parse_srcset_best(srcset: str) -> str:
    """
    Pick the largest image candidate from srcset.
    Handles "... 640w" and "... 2x" (treat 2x as bigger than 1x).
    """
    if not srcset:
        return ""
    candidates: List[Tuple[int, str]] = []
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        bits = part.split()
        url = bits[0].strip()
        score = 0
        if len(bits) >= 2:
            size = bits[1].strip().lower()
            if size.endswith("w"):
                try:
                    score = int(size[:-1])
                except Exception:
                    score = 0
            elif size.endswith("x"):
                try:
                    score = int(float(size[:-1]) * 1000)
                except Exception:
                    score = 0
        candidates.append((score, url))
    if not candidates:
        return ""
    candidates.sort(key=lambda t: t[0])
    return candidates[-1][1]

def classify_image_source(url: str) -> str:
    if is_infatuation_cloudinary(url):
        return "theinfatuation"
    if is_instagram_cdn(url):
        return "instagram"
    return "other"

def looks_like_direct_image(url: str) -> bool:
    if not url:
        return False
    if is_infatuation_cloudinary(url) or is_instagram_cdn(url):
        return True
    return bool(re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", url, re.I))

async def scroll_to_load_images(page: Page) -> None:
    """
    Trigger lazy-loaded images.
    """
    try:
        for _ in range(6):
            await page.mouse.wheel(0, 3000)
            await page.wait_for_timeout(450)
        await page.mouse.wheel(0, -3000)
        await page.wait_for_timeout(250)
    except Exception:
        pass

@dataclass
class ImageHit:
    url: str
    alt: str
    source: str
    credit: Optional[str] = None
    note: Optional[str] = None  # e.g. IG post link not downloadable

async def extract_credits_map(page: Page) -> Dict[str, str]:
    credits: Dict[str, str] = {}
    spans = page.locator('[data-testid="richTextMultimedia-photoCreds"]')
    n = await spans.count()
    for i in range(n):
        t = clean_text(await spans.nth(i).inner_text())
        if t:
            credits[f"idx:{i}"] = t.replace("photo credit:", "").strip()

    header_credit = await get_text_or_none(page, "p.flatplan_photoCredit")
    if header_credit and "photo credit" in header_credit.lower():
        credits["header"] = header_credit.split(":", 1)[-1].strip()
    return credits

async def scan_images_on_page(page: Page, page_url: str, debug: bool = False, debug_max_print: int = 25) -> List[ImageHit]:
    """
    HYBRID scan (this is the important upgrade):
    - scroll to trigger lazy load
    - parse page.content() with BeautifulSoup (like your working requests+bs4 script)
    - also scan DOM for picture/source srcset and img attrs
    - keeps Infatuation Cloudinary + Instagram CDN + other direct image URLs
    - records IG post links (not downloadable) if present
    """
    credits = await extract_credits_map(page)
    hits: List[ImageHit] = []
    seen_urls: Set[str] = set()
    seen_keys: Set[str] = set()

    def pick_credit(idx: int) -> Optional[str]:
        if idx == 0 and "header" in credits:
            return credits["header"]
        if credits:
            return next(iter(credits.values()))
        return None

    def add(url: str, alt: str = "", note: Optional[str] = None, idx_for_credit: int = 0):
        if not url or url.startswith("data:"):
            return
        u = normalize_url(url, page_url)
        if not u:
            return
        if is_infatuation_cloudinary(u):
            u = clean_cloudinary_url(u)

        src = classify_image_source(u)

        # keep only direct image urls OR cloudinary/instagram cdn
        if not looks_like_direct_image(u):
            return

        if u in seen_urls:
            return
        seen_urls.add(u)

        hits.append(ImageHit(
            url=u,
            alt=clean_text(alt),
            source=src,
            credit=pick_credit(idx_for_credit),
            note=note
        ))

    # 0) trigger lazy load
    await scroll_to_load_images(page)

    # 1) BS4 parse of current HTML (very reliable on this site)
    try:
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")

        # img tags: src + srcset + data-src + data-srcset
        for img_idx, img in enumerate(soup.find_all("img")):
            alt = (img.get("alt") or "").strip()

            src = (img.get("src") or "").strip()
            data_src = (img.get("data-src") or img.get("data-lazy-src") or "").strip()

            srcset = (img.get("srcset") or "").strip()
            data_srcset = (img.get("data-srcset") or "").strip()

            best = ""
            if srcset:
                best = parse_srcset_best(srcset)
            elif data_srcset:
                best = parse_srcset_best(data_srcset)

            candidate = best or data_src or src
            if not candidate:
                continue

            add(candidate, alt=alt, idx_for_credit=img_idx)

        # instagram post links (record, not downloadable)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "instagram.com/p/" in href or "instagram.com/reel/" in href:
                href = normalize_url(href, page_url)
                key = f"igpost::{href}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                hits.append(ImageHit(
                    url=href,
                    alt="instagram post",
                    source="instagram",
                    credit=None,
                    note="Instagram post URL found; direct image download not available unless a CDN image URL is present."
                ))
    except Exception as e:
        if debug:
            print(f"    [IMG-BS4] failed: {type(e).__name__}: {e}")

    # 2) DOM scan for <picture><source srcset> (sometimes best URLs live here)
    try:
        sources = page.locator("picture source")
        sc = await sources.count()
        for i in range(sc):
            srcset = (await sources.nth(i).get_attribute("srcset")) or (await sources.nth(i).get_attribute("data-srcset")) or ""
            best = parse_srcset_best(srcset)
            if not best:
                continue
            add(best, alt="", idx_for_credit=i)
    except Exception:
        pass

    # 3) DOM scan for <img> attrs (fallback)
    try:
        imgs = page.locator("main img, article img, img")
        cnt = await imgs.count()
        if debug:
            print(f"    [IMG-SCAN] dom_img_count={cnt}")

        for i in range(cnt):
            img = imgs.nth(i)
            alt = (await img.get_attribute("alt")) or ""

            srcset = (await img.get_attribute("srcset")) or (await img.get_attribute("data-srcset")) or ""
            best = parse_srcset_best(srcset) if srcset else ""

            if best:
                candidate = best
            else:
                candidate = (
                    (await img.get_attribute("data-src"))
                    or (await img.get_attribute("data-lazy-src"))
                    or (await img.get_attribute("src"))
                    or ""
                )

            if candidate:
                add(candidate, alt=alt, idx_for_credit=i)
    except Exception:
        pass

    # debug print
    if debug:
        print(f"    [IMG-SCAN] total_unique={len(hits)}")
        shown = 0
        for h in hits:
            if shown >= debug_max_print:
                break
            shown += 1
            print(f"      - keep=1 source={h.source} alt={h.alt!r}")
            print(f"        url={h.url}")

    # de-dupe IG post links vs direct images (keep both, but avoid exact duplicates)
    final: List[ImageHit] = []
    seen_final: Set[str] = set()
    for h in hits:
        k = h.url if h.note else f"img::{h.url}"
        if k in seen_final:
            continue
        seen_final.add(k)
        final.append(h)

    return final

def ext_from_content_type(ct: str) -> str:
    ct = (ct or "").lower()
    if "jpeg" in ct or "jpg" in ct:
        return ".jpg"
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    return ".jpg"

async def download_file(context, url: str, out_path_no_ext: str, debug: bool = False) -> Optional[str]:
    """
    Downloads via Playwright request API.
    - Determines extension via Content-Type when possible.
    - Returns final saved filepath, or None on failure.
    """
    try:
        resp = await context.request.get(url, timeout=60000)
        if not resp.ok:
            if debug:
                print(f"      [DL] FAIL {resp.status} {url}")
            return None

        ct = resp.headers.get("content-type", "")
        ext = ext_from_content_type(ct)
        out_path = out_path_no_ext + ext

        if os.path.exists(out_path):
            return out_path

        data = await resp.body()
        ensure_dir(os.path.dirname(out_path))
        with open(out_path, "wb") as f:
            f.write(data)

        if debug:
            print(f"      [DL] OK -> {out_path}")
        return out_path
    except Exception as e:
        if debug:
            print(f"      [DL] EXC {type(e).__name__}: {e}")
        return None

# ---------- detail page extraction ----------

async def extract_rating(page: Page) -> Optional[float]:
    candidates = [
        '[data-testid="venue-rating"]',
        '[data-testid="rating-number"]',
        'span[data-testid="ratingNumber"]',
    ]
    for sel in candidates:
        t = await get_text_or_none(page, sel)
        r = await maybe_float(t)
        if r is not None and 0 <= r <= 10:
            return r

    try:
        top = page.locator("main").first
        txt = clean_text(await top.inner_text())
        m = re.search(r"\b(\d\.\d)\b", txt)
        if m:
            r = float(m.group(1))
            if 0 <= r <= 10:
                return r
    except Exception:
        pass
    return None

async def extract_price(page: Page) -> Optional[str]:
    loc = page.locator('[data-testid="caption-venue-price"]').first
    if await loc.count():
        txt = clean_text(await loc.inner_text()).replace(" ", "")
        m = re.search(r"(£+)", txt)
        if m:
            return m.group(1)
    return None

async def extract_cuisine_and_neighborhood(page: Page) -> Tuple[Optional[str], Optional[str]]:
    cuisine = await get_text_or_none(page, '[data-testid="cuisineTagClick"] span.chakra-text')
    if not cuisine:
        cuisine = await get_text_or_none(page, 'a[data-testid^="tag-tagLink-"][href*="/cuisines/"] span.chakra-text')

    neighborhood = await get_text_or_none(page, '[data-testid="neighborhoodTagClick"] span.chakra-text')
    if not neighborhood:
        neighborhood = await get_text_or_none(page, 'a[data-testid^="tag-tagLink-"][href*="/neighborhoods/"] span.chakra-text')

    return cuisine, neighborhood

async def extract_perfect_for(page: Page) -> List[str]:
    out: List[str] = []
    tags = page.locator('[data-testid="large-tag"] span.chakra-heading, [data-testid="large-tag"] span.chakra-text')
    n = await tags.count()
    for i in range(n):
        t = clean_text(await tags.nth(i).inner_text())
        if t and t not in out:
            out.append(t)
    return out

async def extract_author(page: Page) -> Dict[str, Optional[str]]:
    name = await get_text_or_none(page, '[data-testid="contributorName"]')
    if not name:
        name = await get_text_or_none(page, ".flatplan_authorName")

    role = None
    try:
        block = page.locator(".flatplan_authorDetails").first
        if await block.count():
            txt = clean_text(await block.inner_text())
            if name:
                txt2 = txt.replace(name, "").strip()
                txt2 = re.sub(
                    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b.*",
                    "",
                    txt2
                ).strip()
                role = clean_text(txt2) or None
    except Exception:
        role = None
    return {"name": name, "role": role}

async def extract_review_text(page: Page) -> Optional[str]:
    containers = ["div.flatplan_body", "div.styles_richText__fd47G", "article", "main"]
    for sel in containers:
        loc = page.locator(sel).first
        if await loc.count():
            ps = loc.locator("p.chakra-text, p")
            n = await ps.count()
            chunks = []
            for i in range(n):
                t = clean_text(await ps.nth(i).inner_text())
                if t:
                    chunks.append(t)
            text = "\n\n".join(chunks).strip()
            if len(text) >= 50:
                return text
    return None

async def extract_food_rundown(page: Page) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    sec = page.locator("#foodRundown").first
    if not await sec.count():
        return out

    blocks = sec.locator("div.css-1p1knjt, div:has(h2)")
    n = await blocks.count()
    for i in range(n):
        b = blocks.nth(i)
        h = b.locator("h2").first
        p = b.locator("p").first
        if not await h.count() or not await p.count():
            continue
        name = clean_text(await h.inner_text())
        desc = clean_text(await p.inner_text())
        if name and desc:
            out.append({"name": name, "description": desc})

    seen = set()
    final = []
    for x in out:
        if x["name"] in seen:
            continue
        seen.add(x["name"])
        final.append(x)
    return final

async def extract_cta_links(page: Page) -> Dict[str, Optional[str]]:
    def visible_marker(found: bool) -> Optional[str]:
        return "VISIBLE_ON_PAGE" if found else None

    directions = await page.locator('[data-testid*="directions"]').count()
    website = await page.locator('[data-testid*="website"]').count()
    reserve = await page.locator('[data-testid*="reserve"]').count()
    instagram = await page.locator('a[href*="instagram.com"]').count()

    return {
        "reserve": visible_marker(reserve > 0),
        "website": visible_marker(website > 0),
        "directions": visible_marker(directions > 0),
        "instagram": visible_marker(instagram > 0),
    }

async def extract_address(page: Page, city_slug: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    addr_text = await get_text_or_none(page, '[data-testid="venue-googleMapUrl"] p')
    if not addr_text:
        addr_text = await get_text_or_none(page, 'a[data-testid="venue-googleMapUrl"]')

    if city_slug == "london":
        street, city, postal, country = split_uk_address(addr_text or "")
        return addr_text, street, city, postal, country

    return addr_text, None, None, None, None

# ---------- listing collection (scroll) ----------

async def collect_review_urls_by_scrolling(page: Page, city_slug: str, max_reviews: Optional[int], debug: bool = False) -> List[str]:
    """
    Scrolls /<city>/reviews and collects unique /<city>/reviews/<slug> links.
    """
    found: List[str] = []
    seen: Set[str] = set()

    last_count = 0
    stable_rounds = 0

    while True:
        loc = page.locator(f'a[href^="/{city_slug}/reviews/"]')
        n = await loc.count()
        for i in range(n):
            href = await loc.nth(i).get_attribute("href")
            if not href:
                continue
            if "/reviews/" not in href:
                continue
            full = urljoin(BASE, href)
            if f"/{city_slug}/reviews/" not in full:
                continue
            if full not in seen:
                seen.add(full)
                found.append(full)

        if max_reviews and len(found) >= max_reviews:
            found = found[:max_reviews]
            break

        await page.mouse.wheel(0, 3000)
        await page.wait_for_timeout(1200)

        if len(found) == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = len(found)

        if debug:
            print(f"  [LIST] collected={len(found)} stable_rounds={stable_rounds}")

        if stable_rounds >= 5:
            break

    return found

# ---------- incremental output ----------

def load_existing_ids(out_path: str) -> Set[str]:
    if not os.path.exists(out_path):
        return set()
    try:
        with open(out_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {item.get("source_id") for item in data if isinstance(item, dict) and item.get("source_id")}
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return {item.get("source_id") for item in data["items"] if isinstance(item, dict) and item.get("source_id")}
    except Exception:
        return set()
    return set()

def read_existing_items(out_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(out_path):
        return []
    try:
        with open(out_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return [x for x in data["items"] if isinstance(x, dict)]
    except Exception:
        return []
    return []

def write_output(out_path: str, items: List[Dict[str, Any]]) -> None:
    ensure_dir(os.path.dirname(out_path) or ".")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

# ---------- scrape one detail page ----------

async def scrape_one(
    context,
    page: Page,
    url: str,
    city_slug: str,
    media_dir: str,
    debug_images: bool,
    debug_downloads: bool,
    debug_max_print: int,
) -> Dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=120000)
    await click_cookie_ok(page)

    # small wait helps React hydration
    await page.wait_for_timeout(600)

    name = await get_text_or_none(page, "h1.chakra-heading") or await get_text_or_none(page, "h1") or "UNKNOWN"
    source_id = parse_source_id(url)

    addr_text, street, city, postal, country = await extract_address(page, city_slug)
    rating = await extract_rating(page)
    price = await extract_price(page)
    cuisine, neighborhood = await extract_cuisine_and_neighborhood(page)
    perfect_for = await extract_perfect_for(page)
    author = await extract_author(page)

    time_attr = await get_attr_or_none(page, ".flatplan_authorDetails time, time[datetime]", "datetime")
    time_text = await get_text_or_none(page, ".flatplan_authorDetails time, time")
    source_created_at, published_date_text = iso_date_from_time_attr(time_attr, time_text)

    cta_links = await extract_cta_links(page)
    review_text = await extract_review_text(page)
    food_rundown = await extract_food_rundown(page)

    # images: collect + download (HYBRID)
    image_hits = await scan_images_on_page(page, page_url=url, debug=debug_images, debug_max_print=debug_max_print)
    ensure_dir(media_dir)

    images_out: List[Dict[str, Any]] = []
    for idx, hit in enumerate(image_hits, start=1):
        local_path = None

        is_direct = bool(re.search(r"^https?://", hit.url)) and looks_like_direct_image(hit.url)

        if is_direct:
            base_name = safe_filename(f"{source_id}__img_{idx:03d}")
            out_no_ext = os.path.join(media_dir, base_name)
            saved = await download_file(context, hit.url, out_no_ext, debug=debug_downloads)
            if saved:
                local_path = os.path.relpath(saved, media_dir).replace("\\", "/")

        images_out.append({
            "url": hit.url,
            "source": hit.source,
            "credit": hit.credit or None,
            "alt": hit.alt or None,
            "local_path": local_path,
            "note": hit.note,
        })

    item = {
        "name": name,
        "source_platform": "theinfatuation",
        "source_id": source_id,
        "source_created_at": source_created_at,

        "location_name": name,

        "google_place_id": f"theinfatuation:{source_id}",
        "item_id": f"theinfatuation:{source_id}",

        "street": street,
        "city": city or (city_slug.replace("-", " ").title()),
        "postal_code": postal,
        "country": country or ("GB" if city_slug == "london" else None),

        "source_data": {
            "url": url,
            "city_slug": city_slug,

            "rating": rating,
            "price": price,
            "cuisine": cuisine,
            "neighborhood": neighborhood,

            "perfect_for": perfect_for,

            "author": author,

            "published_date_text": published_date_text,

            "cta_links": cta_links,

            "review_text": review_text,

            "food_rundown": food_rundown,

            "images": images_out,
        }
    }

    return item

# ---------- run city ----------

async def run_city(args: argparse.Namespace, city_slug: str) -> None:
    city_slug = city_slug.strip().lower()
    start_url = f"{BASE}/{city_slug}/reviews"

    out_path = os.path.join(args.outdir, f"{city_slug}.json")
    media_dir = os.path.join(args.media, city_slug)

    existing_ids = load_existing_ids(out_path) if args.incremental else set()
    existing_items = read_existing_items(out_path) if args.incremental else []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.headful)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=UA,
        )
        page = await context.new_page()

        print(f"\n[CITY] {city_slug} -> {start_url}")
        await page.goto(start_url, wait_until="domcontentloaded", timeout=120000)
        await click_cookie_ok(page, debug=args.debug_images)

        urls = await collect_review_urls_by_scrolling(page, city_slug, max_reviews=args.limit, debug=args.debug_list)
        print(f"[INFO] Found {len(urls)} review links (limit={args.limit})")

        merged: Dict[str, Dict[str, Any]] = {
            it["source_id"]: it
            for it in existing_items
            if isinstance(it, dict) and it.get("source_id")
        }

        kept = 0
        skipped = 0

        for i, url in enumerate(urls, start=1):
            sid = parse_source_id(url)

            if args.incremental and sid in existing_ids:
                skipped += 1
                print(f"[SKIP] {i}/{len(urls)} already in output: {sid}")
                continue

            print(f"[OPEN] {i}/{len(urls)} {url}")

            try:
                item = await scrape_one(
                    context=context,
                    page=page,
                    url=url,
                    city_slug=city_slug,
                    media_dir=media_dir,
                    debug_images=args.debug_images,
                    debug_downloads=args.debug_downloads,
                    debug_max_print=args.debug_max_print,
                )
            except PWTimeout:
                print(f"  [WARN] Timeout scraping: {url}")
                continue
            except Exception as e:
                print(f"  [WARN] Error scraping: {url} -> {type(e).__name__}: {e}")
                continue

            # Date filter (inclusive) based on source_created_at if present
            if args.start_date or args.end_date:
                sca = item.get("source_created_at") or ""
                yyyy_mm_dd = sca[:10] if isinstance(sca, str) and len(sca) >= 10 else ""
                if not yyyy_mm_dd:
                    print("  [DATE] No parseable date -> skip (because date filter is enabled)")
                    continue
                if not in_inclusive_range(yyyy_mm_dd, args.start_date, args.end_date):
                    print(f"  [DATE] {yyyy_mm_dd} outside range -> skip")
                    continue

            merged[item["source_id"]] = item
            kept += 1

            if args.incremental_write:
                write_output(out_path, list(merged.values()))
                print(f"  [OK] saved incrementally (total now: {len(merged)})")

        write_output(out_path, list(merged.values()))
        print(f"[DONE] city={city_slug} kept={kept} skipped={skipped} total_out={len(merged)} -> {out_path}")

        await browser.close()

# ---------- main ----------

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default=None, help="Single city slug (e.g., london, new-york)")
    ap.add_argument("--cities", default=None, help="Comma-separated city slugs (e.g., london,new-york)")

    ap.add_argument("--limit", type=int, default=None, help="Max number of reviews to scrape (default: all found)")
    ap.add_argument("--outdir", default="output", help="Output folder for JSON files (one per city)")
    ap.add_argument("--media", default="output_media", help="Root folder to save images (per city subfolder)")

    ap.add_argument("--incremental", action="store_true", help="Skip items already in output (by source_id)")
    ap.add_argument("--incremental-write", action="store_true", help="Write JSON after each item (safer)")

    ap.add_argument("--start-date", default=None, help="Inclusive start date YYYY-MM-DD")
    ap.add_argument("--end-date", default=None, help="Inclusive end date YYYY-MM-DD")

    ap.add_argument("--headful", action="store_true", help="Run browser visible (headful)")
    ap.add_argument("--debug-images", action="store_true", help="Print image scan info")
    ap.add_argument("--debug-downloads", action="store_true", help="Print download info")
    ap.add_argument("--debug-list", action="store_true", help="Print listing scroll debug")
    ap.add_argument("--debug-max-print", type=int, default=25, help="Max image lines to print when --debug-images is on")
    return ap

async def main_async() -> int:
    args = build_parser().parse_args()

    cities: List[str] = []
    if args.cities:
        cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    elif args.city:
        cities = [args.city.strip()]
    else:
        cities = ["london", "new-york"]

    for d in [args.start_date, args.end_date]:
        if d and not re.match(r"^\d{4}-\d{2}-\d{2}$", d):
            print(f"[ERR] Bad date format: {d}. Expected YYYY-MM-DD", file=sys.stderr)
            return 2

    ensure_dir(args.outdir)
    ensure_dir(args.media)

    for city in cities:
        await run_city(args, city)

    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main_async()))
    except KeyboardInterrupt:
        print("\n[STOP] interrupted by user")
        raise SystemExit(130)
