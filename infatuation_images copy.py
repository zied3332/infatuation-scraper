#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import re
import time
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as dt_parse
from playwright.sync_api import sync_playwright


BASE = "https://www.theinfatuation.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120 Safari/537.36"
}

# -------------------------
# filters
# -------------------------

# EXCLUDE author/editor headshots like:
# .../c_thumb,w_512,ar_1:1,g_face,.../images/Editorial_Team_Headshots_...
AUTHOR_HEADSHOT_URL_PATTERNS = [
    r"/images/Editorial_Team_Headshots_",  # strong signal
    r"/c_thumb,",                          # thumbnail transform
    r"\bar_1:1\b",                         # square aspect
    r"\bg_face\b",                         # face crop
]

def is_author_headshot(url: str, alt: str = "") -> bool:
    u = (url or "").lower()
    a = (alt or "").strip().lower()

    hits = 0
    for pat in AUTHOR_HEADSHOT_URL_PATTERNS:
        if re.search(pat.lower(), u):
            hits += 1

    if "editorial_team_headshots_" in u:
        return True

    if hits >= 3:
        return True

    # conservative: if alt looks like a name + url has thumb-ish signals
    if hits >= 2 and a and len(a.split()) in (2, 3):
        parts = a.replace("’", "'").replace("-", " ").split()
        if all(part.isalpha() for part in parts):
            return True

    return False


# ✅ NEW: Skip Suggested Reading + Included In / Featured In card images
# We detect by checking if the img has an ancestor with one of these classes.
SKIP_IMG_ANCESTOR_CLASS_SUBSTRINGS = [
    "styles_story__",                 # suggested reading cards
    "styles_featuredInContainer__",   # included in / featured in block
]

def has_skipped_ancestor(img_tag) -> bool:
    """
    Returns True if this <img> is inside suggested reading / featured-in blocks.
    We check ancestors and look for class names that contain known substrings.
    """
    for parent in img_tag.parents:
        if not hasattr(parent, "get"):
            continue
        classes = parent.get("class") or []
        if not classes:
            continue
        # classes may be list like ["chakra-linkbox", "styles_story__EAXyY", ...]
        for c in classes:
            c = (c or "")
            for sub in SKIP_IMG_ANCESTOR_CLASS_SUBSTRINGS:
                if sub in c:
                    return True
    return False


# -------------------------
# helpers
# -------------------------

def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)

def safe_slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "item"

def load_json(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: str, data: List[dict]) -> None:
    folder = os.path.dirname(path)
    if folder:
        ensure_dir(folder)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def parse_date_any(text: str) -> Optional[str]:
    if not text:
        return None
    text = " ".join(text.split()).strip()
    try:
        dt = dt_parse(text, fuzzy=True)
        return dt.date().isoformat()
    except Exception:
        return None

def in_inclusive_range(date_iso: str, start_iso: Optional[str], end_iso: Optional[str]) -> bool:
    if not date_iso:
        return False
    if start_iso and date_iso < start_iso:
        return False
    if end_iso and date_iso > end_iso:
        return False
    return True

def clean_cloudinary_url(url: str) -> str:
    return re.sub(r"w_\d+", "w_3840", url)

def guess_ext_from_content_type(ct: str) -> str:
    ct = (ct or "").lower()
    if "jpeg" in ct or "jpg" in ct:
        return ".jpg"
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    return ""

def download_binary(url: str, dest_path: str, timeout: int = 40) -> bool:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()

        if os.path.splitext(dest_path)[1] == "":
            ext = guess_ext_from_content_type(r.headers.get("Content-Type", ""))
            if ext:
                dest_path = dest_path + ext

        ensure_dir(os.path.dirname(dest_path))
        with open(dest_path, "wb") as f:
            f.write(r.content)
        return True
    except Exception:
        return False

def is_infatuation_cloudinary(url: str) -> bool:
    return "res.cloudinary.com/the-infatuation" in (url or "")

def is_instagram_cdn(url: str) -> bool:
    u = (url or "").lower()
    return ("cdninstagram" in u) or ("instagram" in u) or ("fbcdn.net" in u)

def normalize_url(u: str, page_url: str) -> str:
    if not u:
        return ""
    return urljoin(page_url, u)

# -------------------------
# 1) Collect review URLs (Playwright)
# -------------------------

def collect_review_urls(city_slug: str, max_reviews: Optional[int] = None) -> List[str]:
    listing_url = f"{BASE}/{city_slug}/reviews"
    found: List[str] = []
    seen: Set[str] = set()

    print(f"[LIST] Opening listing: {listing_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(listing_url, wait_until="domcontentloaded", timeout=120000)

        try:
            page.get_by_role("button", name=re.compile(r"OK|Accept|I agree", re.I)).click(timeout=3000)
        except Exception:
            pass

        last_count = 0
        stable_rounds = 0

        while True:
            anchors = page.query_selector_all("a[href]")
            for a in anchors:
                href = a.get_attribute("href") or ""
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

            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(1200)

            if len(found) == last_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
                last_count = len(found)

            if stable_rounds >= 5:
                break

        browser.close()

    print(f"[LIST] Collected {len(found)} review URLs")
    return found

# -------------------------
# 2) Parse one review page (Requests + BS4)
# -------------------------

def extract_review_date(soup: BeautifulSoup) -> Optional[str]:
    t = soup.find("time")
    if t:
        dt = t.get("datetime") or t.get_text(" ", strip=True)
        d = parse_date_any(dt)
        if d:
            return d

    for key in ["article:published_time", "og:updated_time", "article:modified_time"]:
        m = soup.find("meta", attrs={"property": key})
        if m and m.get("content"):
            d = parse_date_any(m["content"])
            if d:
                return d

    candidates = soup.find_all(["span", "p", "div"], limit=200)
    for c in candidates:
        txt = c.get_text(" ", strip=True)
        if not txt:
            continue
        if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", txt, re.I):
            d = parse_date_any(txt)
            if d:
                return d

    return None

def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(" ", strip=True)
    if soup.title:
        return soup.title.get_text(" ", strip=True)
    return ""

def extract_images(page_url: str, soup: BeautifulSoup) -> List[dict]:
    results: List[dict] = []
    seen_urls: Set[str] = set()

    for img in soup.find_all("img"):
        # ✅ NEW: skip images from Suggested Reading / Featured In blocks
        if has_skipped_ancestor(img):
            continue

        src = img.get("src") or ""
        src = normalize_url(src, page_url)
        if not src:
            continue

        alt = (img.get("alt") or "").strip()

        # skip author headshots
        if is_author_headshot(src, alt):
            continue

        if is_infatuation_cloudinary(src):
            src = clean_cloudinary_url(src)
            source = "infatuation"
        elif is_instagram_cdn(src):
            source = "instagram"
        else:
            if not re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", src, re.I):
                continue
            source = "other"

        if src in seen_urls:
            continue
        seen_urls.add(src)

        results.append({
            "url": src,
            "source": source,
            "alt": alt,
            "downloaded": False,
            "local_path": None
        })

    # Instagram post links (optional)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "instagram.com/p/" in href or "instagram.com/reel/" in href:
            href = normalize_url(href, page_url)
            key = f"insta-post::{href}"
            if key in seen_urls:
                continue
            seen_urls.add(key)
            results.append({
                "url": href,
                "source": "instagram",
                "alt": "instagram post",
                "downloaded": False,
                "local_path": None,
                "note": "Post URL found; direct image download may not be available without IG CDN image in HTML."
            })

    return results

def scrape_review(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    return {
        "url": url,
        "title": extract_title(soup),
        "date": extract_review_date(soup),
        "images": extract_images(url, soup),
    }

# -------------------------
# 3) Download images
# -------------------------

def download_images_for_item(city_out_dir: str, item: dict) -> None:
    images: List[dict] = item.get("images") or []
    if not images:
        return

    review_slug = safe_slug(urlparse(item["url"]).path.split("/")[-1])

    for idx, im in enumerate(images, 1):
        src = im.get("source", "other")
        url = im.get("url", "")

        if not url or not re.search(r"^https?://", url):
            continue
        if not (is_infatuation_cloudinary(url) or is_instagram_cdn(url) or re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", url, re.I)):
            continue

        folder = os.path.join(city_out_dir, "images", src, review_slug)
        ensure_dir(folder)

        base_name = f"{idx:02d}_{safe_slug(im.get('alt') or 'image')}"
        dest = os.path.join(folder, base_name)

        ok = download_binary(url, dest)
        if ok:
            possible = []
            for ext in ["", ".jpg", ".png", ".webp"]:
                p = dest + ext
                if os.path.exists(p):
                    possible.append(p)
            final_path = max(possible, key=lambda p: os.path.getmtime(p)) if possible else dest

            im["downloaded"] = True
            im["local_path"] = os.path.relpath(final_path, city_out_dir).replace("\\", "/")
        else:
            im["downloaded"] = False
            im["local_path"] = None

# -------------------------
# main
# -------------------------

def main():
    ap = argparse.ArgumentParser(description="The Infatuation scraper (reviews + images)")

    ap.add_argument("--city", required=True, help="City slug: london, new-york, etc.")
    ap.add_argument("--outdir", default="output", help="Output directory root")
    ap.add_argument("--start-date", default=None, help="Inclusive start date YYYY-MM-DD (optional)")
    ap.add_argument("--end-date", default=None, help="Inclusive end date YYYY-MM-DD (optional)")
    ap.add_argument("--max", type=int, default=None, help="Limit number of reviews (optional)")
    ap.add_argument("--incremental", action="store_true",
                    help="Skip URLs already present in output JSON")
    ap.add_argument("--no-images", action="store_true",
                    help="Do not download images (still records URLs + sources)")

    args = ap.parse_args()

    city = args.city.strip().lower()
    city_out_dir = os.path.join(args.outdir, city)
    ensure_dir(city_out_dir)

    out_json = os.path.join(city_out_dir, "items.json")

    existing_items = load_json(out_json)
    existing_urls = {it.get("url") for it in existing_items if it.get("url")} if args.incremental else set()

    urls = collect_review_urls(city, max_reviews=args.max)

    items: List[dict] = existing_items[:] if args.incremental else []
    kept = 0
    skipped = 0

    for i, url in enumerate(urls, 1):
        if args.incremental and url in existing_urls:
            skipped += 1
            print(f"[SKIP] {i}/{len(urls)} already scraped: {url}")
            continue

        print(f"[OPEN] {i}/{len(urls)} {url}")

        try:
            item = scrape_review(url)

            if args.start_date or args.end_date:
                d = item.get("date")
                if not d:
                    print("  [DATE] No date found -> skipping due to date filter")
                    continue
                if not in_inclusive_range(d, args.start_date, args.end_date):
                    print(f"  [DATE] {d} outside range -> skip")
                    continue

            item["city"] = city

            if not args.no_images:
                download_images_for_item(city_out_dir, item)

            items.append(item)
            kept += 1

            save_json(out_json, items)
            print(f"  [OK] saved item (total now: {len(items)})")

        except Exception as e:
            print(f"  [ERR] {e}")

        time.sleep(0.2)

    print("\n[DONE]")
    print(f"City: {city}")
    print(f"Output JSON: {out_json}")
    print(f"New items kept: {kept}")
    print(f"Skipped (incremental): {skipped}")


if __name__ == "__main__":
    main()
