#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import re
import time
from typing import Any, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as dt_parse
from playwright.sync_api import sync_playwright

BASE = "https://www.theinfatuation.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
}

# -------------------------
# filters
# -------------------------

AUTHOR_HEADSHOT_URL_PATTERNS = [
    r"/images/Editorial_Team_Headshots_",
    r"/c_thumb,",
    r"\bar_1:1\b",
    r"\bg_face\b",
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

    if hits >= 2 and a and len(a.split()) in (2, 3):
        parts = a.replace("’", "'").replace("-", " ").split()
        if all(part.isalpha() for part in parts):
            return True

    return False


SKIP_IMG_ANCESTOR_CLASS_SUBSTRINGS = [
    "styles_story__",
    "styles_featuredInContainer__",
]


def has_skipped_ancestor(img_tag) -> bool:
    for parent in img_tag.parents:
        if not hasattr(parent, "get"):
            continue
        classes = parent.get("class") or []
        for c in classes:
            c = (c or "")
            for sub in SKIP_IMG_ANCESTOR_CLASS_SUBSTRINGS:
                if sub in c:
                    return True
    return False


# -------------------------
# helpers
# -------------------------

CITY_COUNTRY = {
    "london": "GB",
    "manchester": "GB",
    "edinburgh": "GB",
    "new-york": "US",
    "san-francisco": "US",
    "los-angeles": "US",
    "chicago": "US",
    "austin": "US",
    "toronto": "CA",
}


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
    text = " ".join(str(text).split()).strip()
    try:
        dt = dt_parse(text, fuzzy=True)
        return dt.date().isoformat()
    except Exception:
        return None


def iso_datetime_midnight(date_iso: Optional[str]) -> Optional[str]:
    if not date_iso:
        return None
    return f"{date_iso}T00:00:00"


def in_inclusive_range(date_iso: str, start_iso: Optional[str], end_iso: Optional[str]) -> bool:
    if not date_iso:
        return False
    if start_iso and date_iso < start_iso:
        return False
    if end_iso and date_iso > end_iso:
        return False
    return True


def is_infatuation_cloudinary(url: str) -> bool:
    return "res.cloudinary.com/the-infatuation" in (url or "")


def is_instagram_cdn(url: str) -> bool:
    u = (url or "").lower()
    return ("cdninstagram" in u) or ("instagram" in u) or ("fbcdn.net" in u)


def normalize_url(u: str, page_url: str) -> str:
    if not u:
        return ""
    return urljoin(page_url, u)


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


# -------------------------
# Next.js / JSON helpers
# -------------------------

def get_next_data(soup: BeautifulSoup) -> Optional[dict]:
    s = soup.find("script", id="__NEXT_DATA__")
    if not s or not s.string:
        return None
    try:
        return json.loads(s.string)
    except Exception:
        return None


def deep_find_values(obj: Any, key: str) -> List[Any]:
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                out.append(v)
            out.extend(deep_find_values(v, key))
    elif isinstance(obj, list):
        for it in obj:
            out.extend(deep_find_values(it, key))
    return out


def deep_find_first(obj: Any, keys: List[str]) -> Optional[Any]:
    for k in keys:
        vals = deep_find_values(obj, k)
        for v in vals:
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            if isinstance(v, list) and len(v) == 0:
                continue
            return v
    return None


def pick_best_string(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return None


def pick_best_number(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            m = re.search(r"(\d+(?:\.\d+)?)", v)
            return float(m.group(1)) if m else None
    except Exception:
        return None
    return None


# -------------------------
# 0) Image URL extraction helpers (handles lazy images)
# -------------------------

_BG_URL_RE = re.compile(r'background-image\s*:\s*url\((["\']?)(.*?)\1\)', re.I)


def extract_bg_image_url(style: str) -> Optional[str]:
    if not style:
        return None
    m = _BG_URL_RE.search(style)
    if not m:
        return None
    u = (m.group(2) or "").strip().strip('"').strip("'")
    return u or None


def pick_best_from_srcset(srcset: str) -> Optional[str]:
    """
    srcset like: "url 16w, url 32w, ... url 3840w"
    pick the largest width.
    """
    if not srcset:
        return None
    parts = [p.strip() for p in srcset.split(",") if p.strip()]
    best_url = None
    best_w = -1
    for p in parts:
        toks = p.split()
        if not toks:
            continue
        u = toks[0].strip()
        w = -1
        if len(toks) >= 2:
            m = re.match(r"(\d+)\s*w", toks[1].strip(), re.I)
            if m:
                w = int(m.group(1))
        if w > best_w:
            best_w = w
            best_url = u
    return best_url


def get_real_img_url(img, page_url: str) -> str:
    """
    Handles:
    - normal src
    - lazy src="data:image/gif..." with real url in style background-image
    - best from srcset
    """
    src = (img.get("src") or "").strip()
    srcset = (img.get("srcset") or "").strip()
    style = (img.get("style") or "").strip()

    best = pick_best_from_srcset(srcset)
    if best:
        return normalize_url(best, page_url)

    if src.startswith("data:image"):
        bg = extract_bg_image_url(style)
        if bg:
            return normalize_url(bg, page_url)

    return normalize_url(src, page_url)


def find_nearby_photo_credit(container) -> Optional[str]:
    """
    Looks for: <span data-testid="richTextMultimedia-photoCreds">photo credit: X</span>
    near the image container.
    """
    if not container:
        return None
    span = container.find(attrs={"data-testid": "richTextMultimedia-photoCreds"})
    if span:
        txt = span.get_text(" ", strip=True)
        txt = re.sub(r"^\s*photo\s*credit\s*:\s*", "", txt, flags=re.I).strip()
        return txt or None
    return None


# -------------------------
# HTML tag extraction (Perfect for / Cuisine / Neighborhood)
# -------------------------

def _collect_tag_texts_by_href(soup: BeautifulSoup, href_substr: str) -> List[str]:
    out: List[str] = []
    for a in soup.select(f'a[href*="{href_substr}"]'):
        t = a.get_text(" ", strip=True)
        if t:
            out.append(" ".join(t.split()))
    dedup: List[str] = []
    seen = set()
    for x in out:
        k = x.strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        dedup.append(x)
    return dedup


def extract_tags_from_html(soup: BeautifulSoup) -> Tuple[List[str], List[str], List[str]]:
    perfect_for = _collect_tag_texts_by_href(soup, "/perfect-for/")
    cuisines = _collect_tag_texts_by_href(soup, "/cuisines/")
    neighborhoods = _collect_tag_texts_by_href(soup, "/neighborhoods/")

    if soup.select('[data-testid="cuisineTagClick"]'):
        cuisines2 = []
        for a in soup.select('[data-testid="cuisineTagClick"] a[href*="/cuisines/"]'):
            t = a.get_text(" ", strip=True)
            if t:
                cuisines2.append(" ".join(t.split()))
        if cuisines2:
            cuisines = []
            seen = set()
            for x in cuisines2:
                k = x.lower()
                if k not in seen:
                    seen.add(k)
                    cuisines.append(x)

    if soup.select('[data-testid="neighborhoodTagClick"]'):
        neigh2 = []
        for a in soup.select('[data-testid="neighborhoodTagClick"] a[href*="/neighborhoods/"]'):
            t = a.get_text(" ", strip=True)
            if t:
                neigh2.append(" ".join(t.split()))
        if neigh2:
            neighborhoods = []
            seen = set()
            for x in neigh2:
                k = x.lower()
                if k not in seen:
                    seen.add(k)
                    neighborhoods.append(x)

    h = soup.find("h2", string=re.compile(r"^\s*Perfect\s+for\s*$", re.I))
    if h:
        container = h.find_parent()
        if container:
            pf2 = []
            for a in container.select('a[href*="/perfect-for/"]'):
                t = a.get_text(" ", strip=True)
                if t:
                    pf2.append(" ".join(t.split()))
            if pf2:
                perfect_for = []
                seen = set()
                for x in pf2:
                    k = x.lower()
                    if k not in seen:
                        seen.add(k)
                        perfect_for.append(x)

    return perfect_for, cuisines, neighborhoods


# -------------------------
# 1) Collect review URLs (Playwright) - FIXED (no infinite 95<->96 loop)
# -------------------------

def _collect_links_from_current_page(page, city_slug: str, seen: Set[str], found: List[str]) -> int:
    """Collect new review links from the current listing DOM. Returns number of NEW links added."""
    before = len(found)
    anchors = page.query_selector_all("a[href]")
    for a in anchors:
        href = (a.get_attribute("href") or "").strip()
        if "/reviews/" not in href:
            continue

        full = urljoin(BASE, href)

        # keep only *review detail* pages for this city
        if f"/{city_slug}/reviews/" not in full:
            continue

        if full not in seen:
            seen.add(full)
            found.append(full)

    return len(found) - before


def _find_load_more_href(page) -> Optional[str]:
    """
    Find the "Load more" link and return its href (absolute).
    """
    candidates = [
        'a:has-text("Load more")',
        'a[aria-disabled="false"]:has-text("Load more")',
        'a[href*="reviews?page="]:has-text("Load more")',
        "a.styles_loadMoreButton___IN38",
    ]

    for sel in candidates:
        loc = page.locator(sel)
        try:
            if loc.count() > 0:
                el = loc.first
                if el.is_visible():
                    href = (el.get_attribute("href") or "").strip()
                    if href:
                        return urljoin(BASE, href)
        except Exception:
            continue
    return None


def collect_review_urls(city_slug: str, max_reviews: Optional[int] = None, headed: bool = False) -> List[str]:
    listing_url = f"{BASE}/{city_slug}/reviews"
    found: List[str] = []
    seen: Set[str] = set()

    print(f"[LIST] Opening listing: {listing_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=(not headed))
        page = browser.new_page()
        page.goto(listing_url, wait_until="domcontentloaded", timeout=120000)

        # cookie popup (best-effort)
        try:
            page.get_by_role("button", name=re.compile(r"OK|Accept|I agree|Agree", re.I)).click(timeout=3000)
            print("[COOKIE] clicked consent button")
        except Exception:
            pass

        current_page_no = 1
        last_total = 0
        no_growth_rounds = 0
        seen_next_urls: Set[str] = set()

        while True:
            page.wait_for_timeout(500)

            added = _collect_links_from_current_page(page, city_slug, seen, found)
            print(f"[LIST] page={current_page_no} total_links={len(found)} (+{added}) url={page.url}")

            if max_reviews and len(found) >= max_reviews:
                found = found[:max_reviews]
                print(f"[LIST] Reached max={max_reviews}. Stop.")
                break

            # stop if it doesn't grow for a few rounds
            if len(found) == last_total:
                no_growth_rounds += 1
            else:
                no_growth_rounds = 0
                last_total = len(found)

            if no_growth_rounds >= 3:
                print("[LIST] No new links for 3 rounds -> stopping to avoid infinite loop.")
                break

            load_more_url = _find_load_more_href(page)
            if not load_more_url:
                print("[LOADMORE] Not found -> end.")
                break

            # avoid bouncing (e.g., 95 <-> 96)
            if load_more_url in seen_next_urls:
                print(f"[LOADMORE] Next URL repeated ({load_more_url}) -> stopping to avoid loop.")
                break
            seen_next_urls.add(load_more_url)

            # also avoid "next" equals current url
            if load_more_url.rstrip("/") == page.url.rstrip("/"):
                print(f"[LOADMORE] Next URL equals current ({load_more_url}) -> stopping to avoid loop.")
                break

            print(f"[LOADMORE] Found -> navigating to: {load_more_url}")
            page.goto(load_more_url, wait_until="domcontentloaded", timeout=120000)
            current_page_no += 1
            page.wait_for_timeout(800)

        browser.close()

    print(f"[LIST] Collected {len(found)} review URLs total")
    return found


# -------------------------
# 2) Parse one review page (Requests + BS4)
# -------------------------

def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(" ", strip=True)
    if soup.title:
        return soup.title.get_text(" ", strip=True)
    return ""


def extract_published_date_text_and_iso(
    soup: BeautifulSoup, next_data: Optional[dict]
) -> Tuple[Optional[str], Optional[str]]:
    t = soup.find("time")
    if t:
        text = t.get_text(" ", strip=True) or t.get("datetime")
        iso = parse_date_any(t.get("datetime") or text)
        return (text, iso)

    for key in ["article:published_time", "og:updated_time", "article:modified_time"]:
        m = soup.find("meta", attrs={"property": key})
        if m and m.get("content"):
            iso = parse_date_any(m["content"])
            if iso:
                return (m["content"], iso)

    if next_data:
        raw = deep_find_first(next_data, ["publishedDate", "published_at", "publishedAt", "date", "published_date"])
        if raw:
            raw_s = str(raw)
            iso = parse_date_any(raw_s)
            return (raw_s, iso)

    return (None, None)


def extract_author(next_data: Optional[dict]) -> Optional[dict]:
    if not next_data:
        return None

    def find_author_objects(obj: Any) -> List[dict]:
        out = []
        if isinstance(obj, dict):
            if "name" in obj and any(k in obj for k in ["title", "role", "jobTitle"]):
                out.append(obj)
            for v in obj.values():
                out.extend(find_author_objects(v))
        elif isinstance(obj, list):
            for it in obj:
                out.extend(find_author_objects(it))
        return out

    candidates = find_author_objects(next_data)
    if candidates:
        for c in candidates:
            n = pick_best_string(c.get("name"))
            r = pick_best_string(c.get("title") or c.get("role") or c.get("jobTitle"))
            if n and len(n.split()) >= 2:
                return {"name": n, "role": r}

    author_name = pick_best_string(deep_find_first(next_data, ["authorName", "author_name"]))
    author_role = pick_best_string(deep_find_first(next_data, ["authorTitle", "author_title"]))
    if author_name and len(author_name.split()) >= 2:
        return {"name": author_name, "role": author_role}

    return None


def extract_rating_price_from_next_or_html(
    next_data: Optional[dict], html_text: str
) -> Tuple[Optional[float], Optional[str]]:
    rating = None
    price = None

    if next_data:
        rating = pick_best_number(deep_find_first(next_data, ["rating", "score", "numericRating"]))
        price = pick_best_string(deep_find_first(next_data, ["price", "priceRange", "price_range"]))

    if rating is None:
        m = re.search(r'"rating"\s*:\s*(\d+(?:\.\d+)?)', html_text)
        if m:
            rating = float(m.group(1))

    if not price:
        m = re.search(r"(£{1,4}|\${1,4}|€{1,4})", html_text)
        if m:
            price = m.group(1)

    return rating, price


def extract_review_text(next_data: Optional[dict], soup: BeautifulSoup) -> Optional[str]:
    if next_data:
        raw = deep_find_first(next_data, ["reviewText", "review_text", "body", "content", "description"])
        if isinstance(raw, str) and len(raw.strip()) > 80:
            return " ".join(raw.split()).strip()

    rich = soup.find("div", class_=re.compile(r"styles_richText__", re.I))
    if rich:
        ps = rich.find_all("p")
        chunks = []
        for p in ps:
            t = p.get_text(" ", strip=True)
            t = " ".join(t.split())
            if t and len(t) >= 20:
                chunks.append(t)
        text = "\n\n".join(chunks).strip()
        if len(text) > 120:
            return text

    article = soup.find("article")
    if article:
        txt = article.get_text(" ", strip=True)
        txt = " ".join(txt.split())
        if len(txt) > 200:
            return txt

    return None


def extract_food_rundown(next_data: Optional[dict], soup: BeautifulSoup, page_url: str) -> List[dict]:
    if next_data:
        fr = deep_find_first(next_data, ["foodRundown", "food_rundown", "dishes", "menuItems"])
        if isinstance(fr, list):
            out = []
            for it in fr:
                if isinstance(it, dict):
                    name = pick_best_string(it.get("name") or it.get("title"))
                    desc = pick_best_string(it.get("description") or it.get("body") or it.get("text"))
                    img = pick_best_string(it.get("image") or it.get("imageUrl") or it.get("url"))
                    credit = pick_best_string(it.get("credit") or it.get("attribution"))
                    if img:
                        img = normalize_url(img, page_url)
                        if is_infatuation_cloudinary(img):
                            img = clean_cloudinary_url(img)
                    if name or desc or img:
                        out.append({"name": name, "description": desc, "image_url": img, "image_credit": credit})
                elif isinstance(it, str) and it.strip():
                    out.append({"name": it.strip(), "description": None, "image_url": None, "image_credit": None})
            if out:
                return out

    out: List[dict] = []
    sec = soup.find("section", id="foodRundown")
    if not sec:
        return out

    blocks = sec.find_all("div", class_=re.compile(r"css-1p1knjt|foodRundown", re.I))
    if not blocks:
        blocks = sec.find_all("div", recursive=True)

    for b in blocks:
        h = b.find(["h2", "h3"], class_=re.compile(r"chakra-heading", re.I))
        p = b.find("p", class_=re.compile(r"chakra-text", re.I))
        img = b.find("img")
        name = h.get_text(" ", strip=True) if h else None
        desc = p.get_text(" ", strip=True) if p else None
        if desc:
            desc = " ".join(desc.split())

        image_url = None
        image_credit = None

        if img:
            real = get_real_img_url(img, page_url)
            if real and is_infatuation_cloudinary(real):
                real = clean_cloudinary_url(real)
            image_url = real or None
            image_credit = find_nearby_photo_credit(b)

        if (name and len(name) >= 2) or (desc and len(desc) >= 20) or image_url:
            out.append(
                {
                    "name": name,
                    "description": desc,
                    "image_url": image_url,
                    "image_credit": image_credit,
                }
            )

    dedup = []
    seen2 = set()
    for it in out:
        key = (it.get("name") or "", it.get("description") or "", it.get("image_url") or "")
        if key in seen2:
            continue
        seen2.add(key)
        dedup.append(it)
    return dedup


def extract_reservation_url(soup: BeautifulSoup, page_url: str) -> Optional[str]:
    a = soup.find("a", attrs={"data-testid": "reservation-reserveButton"})
    if a and a.get("href"):
        return normalize_url(a["href"], page_url)
    return None


def extract_cta_links(soup: BeautifulSoup, page_url: str) -> dict:
    out = {"website": None, "directions": None, "reserve": None}

    out["reserve"] = extract_reservation_url(soup, page_url)

    for a in soup.find_all("a", href=True):
        txt = (a.get_text(" ", strip=True) or "").lower()
        href = a["href"]

        if not out["website"] and "website" in txt:
            out["website"] = normalize_url(href, page_url)

        if not out["directions"] and ("directions" in txt or "get directions" in txt):
            out["directions"] = normalize_url(href, page_url)

    return out


def extract_address_from_jsonld(
    soup: BeautifulSoup,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[float], Optional[float]]:
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for s in scripts:
        if not s.string:
            continue
        try:
            data = json.loads(s.string)
        except Exception:
            continue

        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue

            phone = pick_best_string(obj.get("telephone") or obj.get("phone"))
            lat = None
            lng = None
            geo = obj.get("geo")
            if isinstance(geo, dict):
                try:
                    lat = float(geo.get("latitude")) if geo.get("latitude") is not None else None
                except Exception:
                    lat = None
                try:
                    lng = float(geo.get("longitude")) if geo.get("longitude") is not None else None
                except Exception:
                    lng = None

            addr = obj.get("address")
            if isinstance(addr, dict):
                street = pick_best_string(addr.get("streetAddress"))
                city = pick_best_string(addr.get("addressLocality"))
                postal = pick_best_string(addr.get("postalCode"))
                country = pick_best_string(addr.get("addressCountry"))
                if street or city or postal or country or phone or (lat is not None and lng is not None):
                    return street, city, postal, country, phone, lat, lng

    return None, None, None, None, None, None, None


def extract_images(page_url: str, soup: BeautifulSoup, next_data: Optional[dict]) -> List[dict]:
    results: List[dict] = []
    seen: Set[str] = set()

    if next_data:
        imgs = deep_find_first(next_data, ["images", "media", "gallery"])
        if isinstance(imgs, list):
            for it in imgs:
                if not isinstance(it, dict):
                    continue
                url = pick_best_string(it.get("url") or it.get("src"))
                if not url:
                    continue
                url = normalize_url(url, page_url)
                if is_infatuation_cloudinary(url):
                    url = clean_cloudinary_url(url)
                if url in seen:
                    continue
                seen.add(url)
                results.append(
                    {
                        "url": url,
                        "source": "theinfatuation" if is_infatuation_cloudinary(url) else "other",
                        "credit": pick_best_string(it.get("credit") or it.get("attribution")),
                        "type": pick_best_string(it.get("type")) or ("video" if "video" in str(it).lower() else None),
                        "alt": pick_best_string(it.get("alt")),
                        "downloaded": False,
                        "local_path": None,
                    }
                )

    for img in soup.find_all("img"):
        if has_skipped_ancestor(img):
            continue

        alt = (img.get("alt") or "").strip()

        real = get_real_img_url(img, page_url)
        if not real:
            continue

        if is_author_headshot(real, alt):
            continue

        if is_infatuation_cloudinary(real):
            real = clean_cloudinary_url(real)
            source = "theinfatuation"
        elif is_instagram_cdn(real):
            source = "instagram"
        else:
            if not re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", real, re.I):
                continue
            source = "other"

        if real in seen:
            continue
        seen.add(real)

        credit = find_nearby_photo_credit(img.parent) or find_nearby_photo_credit(
            img.parent.parent if img.parent else None
        )

        results.append(
            {
                "url": real,
                "source": source,
                "credit": credit,
                "type": None,
                "alt": alt or None,
                "downloaded": False,
                "local_path": None,
            }
        )

    return results


def scrape_review(url: str, city_slug: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    next_data = get_next_data(soup)

    title = extract_title(soup)
    published_text, published_iso = extract_published_date_text_and_iso(soup, next_data)
    author = extract_author(next_data)

    rating, price = extract_rating_price_from_next_or_html(next_data, html)

    perfect_for_html, cuisines_html, neighborhoods_html = extract_tags_from_html(soup)

    cuisine = None
    neighborhood = None
    perfect_for: List[str] = []

    if next_data:
        cuisine = pick_best_string(deep_find_first(next_data, ["cuisine", "cuisineName", "primaryCuisine"]))
        neighborhood = pick_best_string(
            deep_find_first(next_data, ["neighborhood", "neighbourhood", "neighborhoodName"])
        )

        pf = deep_find_first(next_data, ["perfectFor", "perfect_for", "occasions"])
        if isinstance(pf, list):
            perfect_for = [str(x).strip() for x in pf if isinstance(x, (str, int, float)) and str(x).strip()]
        elif isinstance(pf, str) and pf.strip():
            perfect_for = [p.strip() for p in pf.split(",") if p.strip()]

        if not perfect_for:
            tags = deep_find_first(next_data, ["tags"])
            if isinstance(tags, list):
                tmp = []
                for t in tags:
                    if isinstance(t, str) and t.strip():
                        tmp.append(t.strip())
                    elif isinstance(t, dict):
                        name = pick_best_string(t.get("name") or t.get("label") or t.get("title"))
                        if name:
                            tmp.append(name)
                if tmp:
                    perfect_for = tmp

    if not perfect_for:
        perfect_for = perfect_for_html

    cuisines = cuisines_html[:]
    neighborhoods = neighborhoods_html[:]

    if cuisine and cuisine not in cuisines:
        cuisines.insert(0, cuisine)
    if neighborhood and neighborhood not in neighborhoods:
        neighborhoods.insert(0, neighborhood)

    cuisine_final = cuisines[0] if cuisines else None
    neighborhood_final = neighborhoods[0] if neighborhoods else None

    review_text = extract_review_text(next_data, soup)
    food_rundown = extract_food_rundown(next_data, soup, url)

    reservation_url = extract_reservation_url(soup, url)
    cta_links = extract_cta_links(soup, url)

    street, city, postal_code, country, phone, lat, lng = extract_address_from_jsonld(soup)
    if not country:
        country = CITY_COUNTRY.get(city_slug)

    if not city and city_slug:
        city = city_slug.replace("-", " ").title()

    images = extract_images(url, soup, next_data)

    path = urlparse(url).path.lstrip("/")
    source_id = path
    item_id = f"theinfatuation:{source_id}"

    out = {
        "name": title or None,
        "source_platform": "theinfatuation",
        "source_id": source_id,
        "source_created_at": iso_datetime_midnight(published_iso),
        "location_name": title or None,
        "google_place_id": item_id,
        "item_id": item_id,
        "street": street,
        "city": city,
        "postal_code": postal_code,
        "country": country,
        "source_data": {
            "url": url,
            "city_slug": city_slug,
            "rating": rating,
            "price": price,
            "cuisine": cuisine_final,
            "neighborhood": neighborhood_final,
            "perfect_for": perfect_for or [],
            "cuisines": cuisines,
            "neighborhoods": neighborhoods,
            "author": author,
            "published_date_text": published_text,
            "phone": phone,
            "lat": lat,
            "lng": lng,
            "reservation_url": reservation_url,
            "cta_links": cta_links,
            "review_text": review_text,
            "food_rundown": food_rundown or [],
            "images": [
                {
                    "url": im.get("url"),
                    "source": im.get("source"),
                    "credit": im.get("credit"),
                    "type": im.get("type"),
                    "alt": im.get("alt"),
                    "downloaded": im.get("downloaded", False),
                    "local_path": im.get("local_path"),
                }
                for im in images
            ],
        },
    }

    return out


# -------------------------
# 3) Download images
# -------------------------

def download_images_for_item(city_out_dir: str, item: dict) -> None:
    images: List[dict] = (item.get("source_data") or {}).get("images") or []
    if not images:
        return

    review_slug = safe_slug(item.get("source_id") or item.get("name") or "item")

    for idx, im in enumerate(images, 1):
        url = im.get("url") or ""
        src = im.get("source") or "other"

        if not url or not re.search(r"^https?://", url):
            continue

        if not (is_infatuation_cloudinary(url) or re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", url, re.I)):
            continue

        folder = os.path.join(city_out_dir, "images", src, review_slug)
        ensure_dir(folder)

        base_name = f"{idx:02d}_{safe_slug(im.get('alt') or im.get('credit') or 'image')}"
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
    ap = argparse.ArgumentParser(description="The Infatuation scraper -> structured item JSON")

    ap.add_argument("--city", required=True, help="City slug: london, new-york, etc.")
    ap.add_argument("--outdir", default="output", help="Output directory root")

    ap.add_argument("--start-date", default=None, help="Inclusive start date YYYY-MM-DD (optional)")
    ap.add_argument("--end-date", default=None, help="Inclusive end date YYYY-MM-DD (optional)")

    ap.add_argument("--max", type=int, default=None, help="Limit number of reviews (optional)")
    ap.add_argument("--incremental", action="store_true", help="Skip items already in output JSON")
    ap.add_argument("--no-images", action="store_true", help="Do not download images")

    ap.add_argument("--headed", action="store_true", help="Show browser when collecting URLs")

    args = ap.parse_args()

    city = args.city.strip().lower()
    city_out_dir = os.path.join(args.outdir, city)
    ensure_dir(city_out_dir)

    out_json = os.path.join(city_out_dir, "items.json")

    existing_items = load_json(out_json)

    if args.incremental:
        existing_urls = {
            ((it.get("source_data") or {}).get("url"))
            for it in existing_items
            if ((it.get("source_data") or {}).get("url"))
        }
    else:
        existing_urls = set()

    urls = collect_review_urls(city, max_reviews=args.max, headed=args.headed)

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
            item = scrape_review(url, city)

            if args.start_date or args.end_date:
                date_iso = None
                sca = item.get("source_created_at")
                if sca:
                    date_iso = sca.split("T", 1)[0]
                if not date_iso:
                    print("  [DATE] No date found -> skipping due to date filter")
                    continue
                if not in_inclusive_range(date_iso, args.start_date, args.end_date):
                    print(f"  [DATE] {date_iso} outside range -> skip")
                    continue

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
