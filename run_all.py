#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import subprocess
import sys


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def run_cmd(cmd, label: str) -> None:
    print(f"\n=== {label} ===")
    print("CMD:", " ".join(cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        raise SystemExit(f"[FAIL] {label} (exit code {r.returncode})")


def pick_existing(*paths: str) -> str:
    for p in paths:
        if p and os.path.exists(p):
            return p
    return ""


def main():
    ap = argparse.ArgumentParser("Run Infatuation pipeline (items + images + merge)")
    ap.add_argument("--city", required=True, help="city slug, e.g. london")
    ap.add_argument("--out-dir", default="output", help="output folder")
    ap.add_argument("--max", type=int, default=10, help="limit number of reviews")
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--debug", action="store_true")

    # script filenames (change only if you rename files)
    ap.add_argument("--items-script", default="infatuation_items.py")
    ap.add_argument("--images-script", default="infatuation_images.py")
    ap.add_argument("--merge-script", default="sss.py")

    args = ap.parse_args()

    city = args.city.strip().lower()
    out_dir = args.out_dir

    # create output folders
    ensure_dir(out_dir)
    ensure_dir(os.path.join(out_dir, city))

    # IMPORTANT: items.json is a FILE, not a folder → put city folder separately
    items_json = os.path.join(out_dir, f"{city}.json")

    # images script writes either:
    #   output/<city>/items.json
    # or (your current behavior):
    #   output/<city>/<city>/items.json
    images_json_expected_1 = os.path.join(out_dir, city, "items.json")
    images_json_expected_2 = os.path.join(out_dir, city, city, "items.json")

    final_json = os.path.join(out_dir, f"{city}_final.json")

    py = sys.executable  # current python interpreter

    # =========================
    # 1) MAIN ITEMS SCRAPER
    # supports: --city --out --max --headed --debug
    # =========================
    cmd_items = [
        py, args.items_script,
        "--city", city,
        "--out", items_json,
        "--max", str(args.max),
    ]
    if args.headed:
        cmd_items.append("--headed")
    if args.debug:
        cmd_items.append("--debug")

    # =========================
    # 2) IMAGES SCRAPER
    # supports: --city --outdir --max (no --debug in your script)
    # =========================
    cmd_images = [
        py, args.images_script,
        "--city", city,
        "--outdir", os.path.join(out_dir, city),
        "--max", str(args.max),
    ]

    # =========================
    # Run steps 1 & 2 first
    # =========================
    run_cmd(cmd_items,  "STEP 1/3: ITEMS SCRAPER")
    run_cmd(cmd_images, "STEP 2/3: IMAGES SCRAPER")

    # =========================
    # 3) MERGE (auto-detect correct images path)
    # =========================
    images_json = pick_existing(images_json_expected_1, images_json_expected_2)
    if not images_json:
        print("\n[ERROR] Could not find images JSON after running images scraper.")
        print("Tried:")
        print(" -", images_json_expected_1)
        print(" -", images_json_expected_2)
        print("\nFix options:")
        print("1) Open the folder and see where items.json was written")
        print("2) Or fix infatuation_images.py to write to outdir/items.json")
        raise SystemExit(2)

    cmd_merge = [
        py, args.merge_script,
        "--items", items_json,
        "--images", images_json,
        "--out", final_json,
    ]

    run_cmd(cmd_merge, "STEP 3/3: MERGE")

    print("\n✅ ALL DONE")
    print("Items :", items_json)
    print("Images:", images_json)
    print("Final :", final_json)


if __name__ == "__main__":
    main()
