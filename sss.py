#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
from urllib.parse import urlparse, urlunparse


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def normalize_url(u: str) -> str:
    """Normalize URLs so matching is reliable (remove query/fragment, trim trailing slash)."""
    if not u:
        return ""
    u = u.strip()
    p = urlparse(u)
    # drop query + fragment
    p = p._replace(query="", fragment="")
    out = urlunparse(p)
    # trim trailing slash (but keep https://domain/)
    if out.endswith("/") and p.path not in ("", "/"):
        out = out.rstrip("/")
    return out


def path_key(u: str) -> str:
    """Secondary key: just the path (useful if domains differ or slashes differ)."""
    if not u:
        return ""
    p = urlparse(u.strip())
    return (p.path or "").rstrip("/")


def main():
    ap = argparse.ArgumentParser("Merge main items JSON with images JSON")
    ap.add_argument("--items", required=True, help="Main items JSON (ex: output\\london.json)")
    ap.add_argument("--images", required=True, help="Images JSON (ex: output\\london\\items.json)")
    ap.add_argument("--out", required=True, help="Final merged JSON output")
    ap.add_argument("--debug", action="store_true", help="Print detailed merge diagnostics")
    args = ap.parse_args()

    items = load_json(args.items)
    imgs = load_json(args.images)

    if not isinstance(items, list):
        raise SystemExit("[ERR] --items JSON must be a LIST")
    if not isinstance(imgs, list):
        raise SystemExit("[ERR] --images JSON must be a LIST")

    # Build index from images file
    # Expected image row: { "url": "...", "title": "...", "date": "...", "images": [ ... ], "city": "london" }
    by_url = {}
    by_path = {}

    bad_rows = 0
    for row in imgs:
        if not isinstance(row, dict):
            bad_rows += 1
            continue
        u = row.get("url") or ""
        nu = normalize_url(u)
        pk = path_key(u)

        images_list = row.get("images") or []
        if not isinstance(images_list, list):
            images_list = []

        if nu:
            by_url[nu] = images_list
        if pk:
            by_path[pk] = images_list

    # Merge into main items
    matched = 0
    matched_by_path = 0
    missing = 0

    for it in items:
        if not isinstance(it, dict):
            continue

        src = it.get("source_data") or {}
        review_url = src.get("url") or it.get("url") or ""
        nu = normalize_url(review_url)
        pk = path_key(review_url)

        images_list = None

        if nu and nu in by_url:
            images_list = by_url[nu]
            matched += 1
        elif pk and pk in by_path:
            images_list = by_path[pk]
            matched_by_path += 1
        else:
            missing += 1
            images_list = []

        # Put images into the expected place
        if "source_data" not in it or not isinstance(it["source_data"], dict):
            it["source_data"] = {}
        it["source_data"]["images"] = images_list

    # Save
    save_json(args.out, items)

    # Debug summary
    print("\n=== MERGE SUMMARY ===")
    print(f"Main items rows      : {len(items)}")
    print(f"Images rows          : {len(imgs)}")
    print(f"Indexed by url       : {len(by_url)}")
    print(f"Indexed by path      : {len(by_path)}")
    if bad_rows:
        print(f"Bad image rows skipped: {bad_rows}")

    print(f"Matched by URL       : {matched}")
    print(f"Matched by PATH      : {matched_by_path}")
    print(f"Missing matches      : {missing}")
    print(f"Output written to    : {args.out}")

    if args.debug:
        # Show first 5 missing URLs to help diagnose
        print("\n[DEBUG] Example missing URLs:")
        shown = 0
        for it in items:
            u = (it.get("source_data") or {}).get("url") or ""
            nu = normalize_url(u)
            pk = path_key(u)
            if (nu not in by_url) and (pk not in by_path):
                print(" -", u)
                shown += 1
                if shown >= 5:
                    break


if __name__ == "__main__":
    main()
