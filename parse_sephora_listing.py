#!/usr/bin/env python3
"""
parse_sephora_listing.py

Extract product JSON blobs embedded in a Sephora listing HTML (view-source)
and save selected fields to CSV. Optionally download hero images.

Usage examples:
# parse from saved HTML file
python parse_sephora_listing.py --input-file listing.html --out sephora_perfumes.csv --download-images

# parse directly from URL (server-rendered JSON present in view-source)
python parse_sephora_listing.py --url "https://www.sephora.com/shop/perfume?ref=filters[sizeRefinement]=mini,filters[sizeRefinement]=value,filters[sizeRefinement]=refill" --out sephora_perfumes.csv

Dependencies: requests (for URL + image download). Install with: pip install requests
"""
import argparse
import json
import re
import csv
import requests
import os
from urllib.parse import urljoin

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def load_html_from_file(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()

def load_html_from_url(url):
    import requests

def load_html_from_url(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.sephora.com/",
    }

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def find_json_objects_in_html(html):
    """
    Scan html for balanced JSON objects '{...}', attempt json.loads on each,
    and return list of parsed dicts. The routine is tolerant of nested braces
    and quoted strings so it doesn't prematurely match braces inside strings.
    """
    objs = []
    L = len(html)
    i = 0
    while i < L:
        if html[i] == '{':
            start = i
            depth = 0
            in_str = False
            esc = False
            j = i
            while j < L:
                ch = html[j]
                if ch == '"' and not esc:
                    in_str = not in_str
                if ch == '\\' and not esc:
                    esc = True
                    j += 1
                    continue
                else:
                    esc = False
                if not in_str:
                    if ch == '{':
                        depth += 1
                    elif ch == '}':
                        depth -= 1
                        if depth == 0:
                            candidate = html[start:j+1]
                            try:
                                parsed = json.loads(candidate)
                                if isinstance(parsed, dict):
                                    objs.append(parsed)
                            except Exception:
                                # fail quietly—many JS objects are not strict JSON
                                pass
                            break
                j += 1
            i = j + 1
        else:
            i += 1
    return objs

def normalize_and_extract(prod):
    """
    Given a parsed product-like dict, return a row with requested fields.
    Handles the fact that some fields are under currentSku and some top-level.
    """
    get = prod.get
    current = prod.get("currentSku") or {}
    # fallback: some pages use slightly different keys (altImage, imageAltText, etc.)
    imageAlt = current.get("imageAltText") or prod.get("imageAltText") or prod.get("altImage") or ""
    isLimited = current.get("isLimitedEdition")
    if isLimited is None:
        isLimited = prod.get("isLimitedEdition")

    isNew = current.get("isNew")
    if isNew is None:
        isNew = prod.get("isNew")

    listPrice = current.get("listPrice") or prod.get("listPrice") or ""
    heroImage = prod.get("heroImage") or prod.get("image") or prod.get("mainImage") or ""
    targetUrl = prod.get("targetUrl") or prod.get("productUrl") or ""

    return {
        "productId": prod.get("productId") or prod.get("skuId") or "",
        "displayName": prod.get("displayName") or prod.get("brandName") + " " + prod.get("displayName", ""),
        "brandName": prod.get("brandName") or "",
        "imageAltText": imageAlt,
        "isLimitedEdition": bool(isLimited) if isLimited is not None else "",
        "isNew": bool(isNew) if isNew is not None else "",
        "listPrice": listPrice,
        "heroImage": heroImage,
        "targetUrl": targetUrl,
        # optional extras if you'd like:
        "rating": prod.get("rating") or "",
        "reviews": prod.get("reviews") or ""
    }

def download_image(url, dest_folder):
    if not url:
        return ""
    os.makedirs(dest_folder, exist_ok=True)
    try:
        local_name = os.path.basename(url.split("?")[0])
        if not local_name:
            local_name = "img_" + str(abs(hash(url))) + ".jpg"
        local_path = os.path.join(dest_folder, local_name)
        if os.path.exists(local_path):
            return local_path
        r = requests.get(url, headers=HEADERS, stream=True, timeout=30)
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(1024 * 8):
                f.write(chunk)
        return local_path
    except Exception as e:
        print("Failed to download image", url, ":", e)
        return ""

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input-file", help="Saved HTML file (view-source) to parse")
    p.add_argument("--url", help="Or supply the listing URL to fetch directly")
    p.add_argument("--out", default="sephora_perfumes.csv", help="Output CSV file")
    p.add_argument("--download-images", action="store_true", help="Download heroImage into images/")
    p.add_argument("--images-dir", default="images", help="Folder to save images")
    args = p.parse_args()

    if not args.input_file and not args.url:
        print("Supply either --input-file or --url")
        return

    if args.input_file:
        html = load_html_from_file(args.input_file)
    else:
        print("Fetching URL ...")
        html = load_html_from_url(args.url)

    print("Scanning HTML for JSON objects (this may yield many candidate objects)...")
    parsed_objs = find_json_objects_in_html(html)
    print(f"Found {len(parsed_objs)} JSON objects; filtering for product-like objects...")

    products = []
    seen = set()
    for obj in parsed_objs:
        # consider objects that look like product entries
        if "brandName" in obj or "productId" in obj or "displayName" in obj:
            # normalize and dedupe by productId or targetUrl
            row = normalize_and_extract(obj)
            unique_key = row.get("productId") or row.get("targetUrl") or row.get("displayName")
            if unique_key and unique_key not in seen:
                seen.add(unique_key)
                products.append(row)

    print(f"Kept {len(products)} product-like objects after filtering/dedup.")

    # Save CSV
    fieldnames = ["productId","displayName","brandName","imageAltText","isLimitedEdition","isNew","listPrice","heroImage","targetUrl","rating","reviews","localHeroImage"]
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for pr in products:
            local_img = ""
            if args.download_images and pr.get("heroImage"):
                # heroImage may be relative
                img_url = pr["heroImage"]
                # if targetUrl exists and heroImage is relative, try to build absolute using targetUrl as base
                if img_url and img_url.startswith("/"):
                    base = "https://www.sephora.com"
                    img_url = urljoin(base, img_url)
                local_img = download_image(img_url, args.images_dir)
            outrow = {k: pr.get(k, "") for k in fieldnames}
            outrow["localHeroImage"] = local_img
            writer.writerow(outrow)

    print("Wrote CSV to", args.out)
    if args.download_images:
        print("Downloaded images into", args.images_dir)

if __name__ == "__main__":
    main()
