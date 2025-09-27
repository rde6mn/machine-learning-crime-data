"""
sephora_perfume_scraper.py

What it collects:
- product_page_url
- product_name
- price (display price on product page)
- star_rating (float if available)
- review_count (int if available)
- image_urls (all main images)
- local image filenames downloaded into `images/`

Notes:
- Uses Selenium to render JS and scroll the product listing.
- Visits each product page to extract structured data (meta tags + DOM fallbacks).
- Resumes from existing CSV; will not re-download images already present.
- Be polite: configurable delay between product page requests.
"""

import os
import time
import csv
import re
import sys
import argparse
import requests
from urllib.parse import urljoin, urlparse
from pathlib import Path
from tqdm import tqdm
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -------- CONFIG ----------
START_URL = "https://www.sephora.com/shop/perfume?ref=filters[sizeRefinement]=mini,filters[sizeRefinement]=value,filters[sizeRefinement]=refill"
OUTPUT_CSV = "sephora_perfumes.csv"
IMAGES_DIR = "images"
HEADLESS = True
MAX_PRODUCTS = None  # set to 636 or None to collect everything found
DELAY_BETWEEN_PAGES = 1.0  # seconds (polite)
SCROLL_PAUSE = 1.0
REQUESTS_TIMEOUT = 20
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
# ---------------------------

os.makedirs(IMAGES_DIR, exist_ok=True)

def init_driver(headless=True):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"user-agent={USER_AGENT}")
    chrome_options.add_argument("--lang=en-US")
    # optionally: chrome_options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver

def scroll_to_load_all(driver, pause_time=SCROLL_PAUSE, max_scrolls=200):
    """
    Scrolls down the page to attempt to trigger lazy loading/infinite scroll.
    Returns when no more new height is observed or when max_scrolls reached.
    """
    last_height = driver.execute_script("return document.body.scrollHeight")
    scrolls = 0
    while scrolls < max_scrolls:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause_time)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        scrolls += 1

def extract_listing_product_links(driver):
    """Return a list of absolute product page URLs found on the listing page."""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    anchors = soup.find_all("a", href=True)
    product_urls = []
    for a in anchors:
        href = a["href"]
        # Sephora product pages commonly have '/product/' or '/shop/' with product slug
        if re.search(r"(/product/|/shop/)[^/?#]+", href):
            # sometimes links are relative
            url = urljoin("https://www.sephora.com", href)
            # weed out cart/checkout links and filter duplicates
            if "signin" in url or "favorites" in url or "register" in url or "checkout" in url:
                continue
            product_urls.append(url.split("?")[0])
    # deduplicate preserving order
    seen = set()
    uniq = []
    for u in product_urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def safe_get(url, session=None):
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
    sess = session or requests.Session()
    try:
        r = sess.get(url, headers=headers, timeout=REQUESTS_TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"requests error for {url}: {e}")
        return None

def parse_product_page(html, base_url):
    """Extracts name, price, rating, reviews, image urls from a product HTML."""
    soup = BeautifulSoup(html, "html.parser")
    data = {
        "product_name": None,
        "price": None,
        "star_rating": None,
        "review_count": None,
        "image_urls": []
    }

    # 1) Try JSON-LD structured data
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json
            j = json.loads(script.string or "{}")
            # product json-ld might be a dict or list
            if isinstance(j, list):
                for item in j:
                    if item.get("@type", "").lower() == "product":
                        j = item
                        break
            if isinstance(j, dict) and j.get("@type", "").lower() == "product":
                data["product_name"] = data["product_name"] or j.get("name")
                offers = j.get("offers")
                if offers:
                    price = offers.get("price") or (offers[0].get("price") if isinstance(offers, list) else None)
                    data["price"] = price or data["price"]
                aggregate = j.get("aggregateRating")
                if aggregate:
                    try:
                        data["star_rating"] = float(aggregate.get("ratingValue"))
                    except:
                        pass
                    try:
                        data["review_count"] = int(aggregate.get("reviewCount"))
                    except:
                        pass
                images = j.get("image")
                if images:
                    if isinstance(images, list):
                        data["image_urls"].extend(images)
                    else:
                        data["image_urls"].append(images)
        except Exception:
            continue

    # 2) Meta tags fallback
    if not data["product_name"]:
        tag = soup.find("meta", property="og:title") or soup.find("meta", attrs={"name": "twitter:title"})
        if tag and tag.get("content"):
            data["product_name"] = tag["content"].strip()

    if not data["image_urls"]:
        # try og:image and other common patterns
        imgs = []
        tag = soup.find("meta", property="og:image")
        if tag and tag.get("content"):
            imgs.append(tag.get("content"))
        # also look for img tags within product gallery
        gallery_imgs = soup.select("img")
        for img in gallery_imgs:
            src = img.get("src") or img.get("data-src") or img.get("data-ec-src")
            if src and "placeholder" not in src:
                imgs.append(urljoin(base_url, src))
        # dedupe
        imgs_uniq = []
        for u in imgs:
            if u and u not in imgs_uniq:
                imgs_uniq.append(u)
        data["image_urls"].extend(imgs_uniq)

    # price fallback: look for currency-like text
    if not data["price"]:
        # common Sephora price element might include $ and decimals
        price_el = soup.find(text=re.compile(r"\$\d+[.,]?\d*"))
        if price_el:
            # extract first $xx.xx
            m = re.search(r"\$\d+[.,]?\d*", price_el)
            if m:
                data["price"] = m.group(0)

    # rating / reviews fallback: look for meta tags or textual patterns
    if not data["star_rating"]:
        meta_rating = soup.find("meta", itemprop="ratingValue")
        if meta_rating and meta_rating.get("content"):
            try:
                data["star_rating"] = float(meta_rating["content"])
            except:
                pass

    if not data["review_count"]:
        meta_count = soup.find("meta", itemprop="reviewCount")
        if meta_count and meta_count.get("content"):
            try:
                data["review_count"] = int(meta_count["content"].replace(",", ""))
            except:
                pass

    # also look for elements that mention "ratings" or "reviews"
    if data["review_count"] is None:
        text = soup.get_text(separator=" ")
        m = re.search(r"([\d,]+)\s+reviews", text, flags=re.I)
        if m:
            try:
                data["review_count"] = int(m.group(1).replace(",", ""))
            except:
                pass

    return data

def download_image(url, dest_folder, session=None):
    os.makedirs(dest_folder, exist_ok=True)
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path)
    if not filename:
        filename = re.sub(r'\W+', '_', url)[:40]
    dest_path = os.path.join(dest_folder, filename)
    if os.path.exists(dest_path):
        return dest_path
    s = session or requests.Session()
    headers = {"User-Agent": USER_AGENT}
    try:
        r = s.get(url, headers=headers, stream=True, timeout=REQUESTS_TIMEOUT)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(1024 * 8):
                if chunk:
                    f.write(chunk)
        return dest_path
    except Exception as e:
        print(f"Failed to download image {url}: {e}")
        return None

def load_existing_csv(csv_path):
    existing = {}
    if os.path.exists(csv_path):
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                existing[row["product_page_url"]] = row
    return existing

def save_row(csv_path, fieldnames, row):
    write_header = not os.path.exists(csv_path)
    mode = "a"
    with open(csv_path, mode, newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

def main():
    parser = argparse.ArgumentParser(description="Sephora perfume scraper")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--max", type=int, default=MAX_PRODUCTS, help="Max number of products to collect")
    parser.add_argument("--start-url", default=START_URL, help="Listing page URL")
    parser.add_argument("--out", default=OUTPUT_CSV, help="Output CSV filename")
    parser.add_argument("--images-dir", default=IMAGES_DIR, help="Directory to save images")
    args = parser.parse_args()

    driver = init_driver(headless=args.headless or HEADLESS)
    driver.get(args.start_url)
    time.sleep(2)
    scroll_to_load_all(driver)

    product_links = extract_listing_product_links(driver)
    print(f"Found {len(product_links)} product links on the listing page.")
    if args.max:
        product_links = product_links[:args.max]
    existing = load_existing_csv(args.out)
    session = requests.Session()

    fieldnames = [
        "product_page_url", "product_name", "price", "star_rating", "review_count", "image_urls", "downloaded_images"
    ]

    for prod_url in tqdm(product_links, desc="Products"):
        if prod_url in existing:
            # skip if already present
            continue
        # load product page using requests first (faster); fall back to Selenium if necessary
        html = safe_get(prod_url, session=session)
        # sometimes Sephora blocks requests; use Selenium if requests didn't return or content seems minimal
        if not html or len(html) < 2000 or "javascript" in html.lower() and "window.__INITIAL_STATE__" in html:
            try:
                driver.get(prod_url)
                # wait a little for page to render main content
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                html = driver.page_source
            except Exception as e:
                print(f"Failed to load {prod_url} in browser: {e}")
                html = html or ""

        parsed = parse_product_page(html, prod_url)
        # download images
        downloaded = []
        for img_url in parsed["image_urls"]:
            # make absolute if needed
            img_abs = urljoin(prod_url, img_url)
            fpath = download_image(img_abs, args.images_dir, session=session)
            if fpath:
                downloaded.append(fpath)
            time.sleep(0.2)

        row = {
            "product_page_url": prod_url,
            "product_name": parsed.get("product_name") or "",
            "price": parsed.get("price") or "",
            "star_rating": parsed.get("star_rating") if parsed.get("star_rating") is not None else "",
            "review_count": parsed.get("review_count") if parsed.get("review_count") is not None else "",
            "image_urls": " | ".join(parsed.get("image_urls") or []),
            "downloaded_images": " | ".join(downloaded)
        }
        save_row(args.out, fieldnames, row)
        # polite delay
        time.sleep(DELAY_BETWEEN_PAGES)

    driver.quit()
    print("Done. Results saved to", args.out)

if __name__ == "__main__":
    main()
