#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import re
import argparse
from urllib.parse import urljoin, urlparse
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, TimeoutError as PWTimeout


# ---------------- utils ----------------

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def parse_source_id_from_url(url: str) -> str:
    p = urlparse(url)
    return (p.path or "").strip("/")

def city_slug_from_source_id(source_id: str) -> Optional[str]:
    parts = source_id.split("/")
    return parts[0] if parts else None

def split_address(addr: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    addr = clean_text(addr)
    if not addr:
        return None, None, None
    parts = [clean_text(x) for x in addr.split(",") if clean_text(x)]
    if len(parts) >= 3:
        street = ", ".join(parts[:-2])
        city = parts[-2]
        postal = parts[-1]
        return street, city, postal
    if len(parts) == 2:
        return parts[0], parts[1], None
    return parts[0], None, None

def normalize_price_text(price_text: str) -> Optional[str]:
    price_text = clean_text(price_text)
    if not price_text:
        return None
    m = re.findall(r"[£$€]+", price_text)
    if m:
        return m[0]
    return price_text

def visible_flag_or_none(found: bool) -> Optional[str]:
    return "VISIBLE_ON_PAGE" if found else None


# ---------------- extractors (NO IMAGES) ----------------

async def extract_rating_from_ld_json(page) -> Optional[float]:
    scripts = await page.query_selector_all('script[type="application/ld+json"]')
    for sc in scripts:
        try:
            txt = (await sc.inner_text() or "").strip()
            if not txt:
                continue
            data = json.loads(txt)
        except Exception:
            continue

        stack = [data]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                ar = cur.get("aggregateRating")
                if isinstance(ar, dict):
                    rv = ar.get("ratingValue") or ar.get("rating")
                    try:
                        if rv is not None:
                            return float(rv)
                    except Exception:
                        pass

                rv = cur.get("ratingValue") or cur.get("rating")
                try:
                    if rv is not None:
                        return float(rv)
                except Exception:
                    pass

                stack.extend(list(cur.values()))
            elif isinstance(cur, list):
                stack.extend(cur)
    return None

async def extract_cta_links(page) -> Dict[str, Optional[str]]:
    async def has_any(selector: str) -> bool:
        return (await page.query_selector(selector)) is not None

    has_directions = await has_any('a[data-testid*="directions"]')
    has_reserve    = await has_any('a[data-testid*="reserve"]')
    has_website    = await has_any('a[data-testid*="website"]')
    has_instagram  = await has_any('a[data-testid*="instagram"]')

    return {
        "website":    visible_flag_or_none(has_website),
        "directions": visible_flag_or_none(has_directions),
        "reserve":    visible_flag_or_none(has_reserve),
        "instagram":  visible_flag_or_none(has_instagram),
    }

async def extract_perfect_for(page) -> List[str]:
    tags = []
    els = await page.query_selector_all(
        '[data-testid="large-tag"] a span, [data-testid="large-tag"] span.chakra-heading'
    )
    for el in els:
        t = clean_text(await el.inner_text())
        if t:
            tags.append(t)

    seen = set()
    out = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

async def extract_food_rundown(page) -> List[Dict[str, str]]:
    out = []
    section = await page.query_selector('section#foodRundown')
    if not section:
        return out

    blocks = await section.query_selector_all(
        'div.css-1p1knjt, div[data-testid="foodRundown-item"], div:has(h2.chakra-heading):has(p.chakra-text)'
    )
    for b in blocks:
        title_el = await b.query_selector('h2.chakra-heading')
        desc_el  = await b.query_selector('p.chakra-text')
        if not title_el or not desc_el:
            continue
        name = clean_text(await title_el.inner_text())
        desc = clean_text(await desc_el.inner_text())
        if name and desc:
            out.append({"name": name, "description": desc})
    return out

async def extract_review_text(page) -> str:
    body = await page.query_selector('.flatplan_body, div.styles_richText__fd47G, div[data-testid="richText"]')
    if not body:
        return ""

    ps = await body.query_selector_all('p.chakra-text')
    chunks = []
    for p in ps:
        t = clean_text(await p.inner_text())
        if not t:
            continue
        if t.lower().startswith("photo credit:"):
            continue
        chunks.append(t)

    return clean_text("\n\n".join(chunks))

async def extract_author_and_date(page) -> Tuple[Optional[Dict[str, Optional[str]]], Optional[str], Optional[str]]:
    author_name = None
    author_role = None

    name_el = await page.query_selector('[data-testid="contributorName"], .flatplan_authorName a')
    if name_el:
        author_name = clean_text(await name_el.inner_text())

    details = await page.query_selector('.flatplan_authorDetails, .styles_contributorsList__mDfU8')
    if details:
        ps = await details.query_selector_all('p')
        for p in ps:
            t = clean_text(await p.inner_text())
            if not t:
                continue
            if author_name and t == author_name:
                continue
            if re.search(r"\b\d{4}\b", t):
                continue
            if len(t) <= 80:
                author_role = t
                break

    published_date_text = None
    published_date_iso = None
    time_el = await page.query_selector('.flatplan_authorDetails time, time[datetime]')
    if time_el:
        published_date_text = clean_text(await time_el.inner_text())
        published_date_iso = (await time_el.get_attribute("datetime")) or None

    author_obj = None
    if author_name or author_role:
        author_obj = {"name": author_name, "role": author_role}

    return author_obj, published_date_text, published_date_iso

async def extract_header_fields(page) -> Dict[str, Any]:
    name = None
    h1 = await page.query_selector('h1.flatplan_venue-heading, h1.chakra-heading')
    if h1:
        name = clean_text(await h1.inner_text())

    addr = None
    addr_el = await page.query_selector('a[data-testid="venue-googleMapUrl"] p, a[data-testid="venue-googleMapUrl"]')
    if addr_el:
        addr = clean_text(await addr_el.inner_text())

    street, city, postal = split_address(addr or "")

    price = None
    price_el = await page.query_selector('[data-testid="caption-venue-price"]')
    if price_el:
        price_txt = clean_text(await price_el.inner_text())
        price = normalize_price_text(price_txt)

    cuisine = None
    neigh = None

    cuisine_el = await page.query_selector(
        '[data-testid="cuisineTagClick"] a span, a[data-testid^="tag-tagLink-"][href*="/cuisines/"] span'
    )
    if cuisine_el:
        cuisine = clean_text(await cuisine_el.inner_text())

    neigh_el = await page.query_selector(
        '[data-testid="neighborhoodTagClick"] a span, a[data-testid^="tag-tagLink-"][href*="/neighborhoods/"] span'
    )
    if neigh_el:
        neigh = clean_text(await neigh_el.inner_text())

    return {
        "location_name": name,
        "street": street,
        "city": city,
        "postal_code": postal,
        "country": "GB",
        "price": price,
        "cuisine": cuisine,
        "neighborhood": neigh,
    }


# ---------------- build item object (NO IMAGES) ----------------

async def build_item_from_review_page(page, review_url: str) -> Dict[str, Any]:
    source_id = parse_source_id_from_url(review_url)
    city_slug = city_slug_from_source_id(source_id)

    header = await extract_header_fields(page)
    perfect_for = await extract_perfect_for(page)
    author_obj, published_date_text, published_date_iso = await extract_author_and_date(page)
    rating = await extract_rating_from_ld_json(page)
    cta_links = await extract_cta_links(page)
    review_text = await extract_review_text(page)
    food_rundown = await extract_food_rundown(page)

    source_created_at = None
    if published_date_iso and re.fullmatch(r"\d{4}-\d{2}-\d{2}", published_date_iso.strip()):
        source_created_at = f"{published_date_iso.strip()}T00:00:00"

    name = header.get("location_name") or source_id.split("/")[-1]

    item = {
        "name": name,
        "source_platform": "theinfatuation",
        "source_id": source_id,
        "source_created_at": source_created_at,
        "location_name": name,
        "google_place_id": f"theinfatuation:{source_id}",
        "item_id": f"theinfatuation:{source_id}",
        "street": header.get("street"),
        "city": header.get("city") or "London",
        "postal_code": header.get("postal_code"),
        "country": header.get("country") or "GB",
        "source_data": {
            "url": review_url,
            "city_slug": city_slug,
            "rating": rating,
            "price": header.get("price"),
            "cuisine": header.get("cuisine"),
            "neighborhood": header.get("neighborhood"),
            "perfect_for": perfect_for,
            "author": author_obj,
            "published_date_text": published_date_text,
            "cta_links": cta_links,
            "review_text": review_text,
            "food_rundown": food_rundown,
            "images": [],  # intentionally empty
        }
    }
    return item


# ---------------- navigation helpers ----------------

async def accept_cookies_if_present(page, debug: bool = False):
    try:
        btn = await page.query_selector('button:has-text("OK")')
        if btn:
            await btn.click(timeout=2000)
            if debug:
                print("  [COOKIE] clicked OK")
    except Exception:
        pass

async def get_review_links_from_listing(page, base_url: str, city: str, limit: int) -> List[str]:
    await page.wait_for_selector('a[data-testid^="detailedStory-link-"]', timeout=60000)
    anchors = await page.query_selector_all('a[data-testid^="detailedStory-link-"]')

    links = []
    for a in anchors:
        href = await a.get_attribute("href")
        if not href:
            continue
        href = href.strip()
        if not href.startswith("/"):
            continue
        full = urljoin(base_url, href)
        if f"/{city}/reviews/" not in full:
            continue
        links.append(full)
        if len(links) >= limit:
            break

    return links


# ---------------- main ----------------

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default="london")
    ap.add_argument("--max", type=int, default=10)
    ap.add_argument("--out", default="items.json")
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    debug = bool(args.debug)
    base = "https://www.theinfatuation.com"
    start_url = f"{base}/{args.city}/reviews"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=(False if args.headed else True))
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        page = await context.new_page()
        page.set_default_navigation_timeout(60000)
        page.set_default_timeout(60000)

        print(f"[OPEN] Listing: {start_url}")
        await page.goto(start_url, wait_until="domcontentloaded")
        await accept_cookies_if_present(page, debug=debug)

        links = await get_review_links_from_listing(page, base, args.city, args.max)
        print(f"[INFO] Found {len(links)} review links (limit={args.max})")

        items: List[Dict[str, Any]] = []

        for idx, url in enumerate(links, 1):
            print(f"[OPEN] {idx}/{len(links)} {url}")
            try:
                await page.goto(url, wait_until="domcontentloaded")
                await asyncio.sleep(0.4)
            except PWTimeout:
                print(f"  [WARN] timeout loading {url}, retry once")
                await page.goto(url, wait_until="domcontentloaded")

            await accept_cookies_if_present(page, debug=debug)

            item = await build_item_from_review_page(page, url)
            items.append(item)

            # go back to listing (your flow)
            if idx < len(links):
                print("  [NAV] back to listing")
                try:
                    await page.go_back(wait_until="domcontentloaded")
                except Exception:
                    await page.goto(start_url, wait_until="domcontentloaded")
                await accept_cookies_if_present(page, debug=debug)

        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)

        print(f"[DONE] wrote {len(items)} items -> {args.out}")

        await context.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
