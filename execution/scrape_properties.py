import os, json, argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
OUTPUT_DIR = Path(".tmp")
LISTINGS_FILE = OUTPUT_DIR / "listings.json"
NEW_LISTINGS_FILE = OUTPUT_DIR / "new_listings.json"
SEEN_FILE = OUTPUT_DIR / "seen_listings.json"
ACTOR_ID = "dhrumil/rightmove-scraper"

SEARCH_URLS = {
    "Hitchin": "https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=OUTCODE%5E2247&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&includeSSTC=false",
    "Potters Bar": "https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=OUTCODE%5E1616&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&includeSSTC=false",
    "Dartford": "https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=OUTCODE%5E493&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&includeSSTC=false",
    "Gravesend": "https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=OUTCODE%5E494&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&includeSSTC=false",
    "Welwyn": "https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=OUTCODE%5E171&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&includeSSTC=false",
}

def run_apify_actor(search_urls, max_items=50):
    try:
        from apify_client import ApifyClient
    except ImportError:
        return []
    if not APIFY_API_TOKEN:
        return []
    client = ApifyClient(APIFY_API_TOKEN)
    actor_input = {
        "listUrls": [{"url": url} for url in search_urls],
        "maxListings": max_items,
    }
    print(f"Running Apify actor: {ACTOR_ID}")
    print(f"Scraping {len(search_urls)} URLs...")
    try:
        run = client.actor(ACTOR_ID).call(run_input=actor_input)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"Scraped {len(items)} properties")
        return items
    except Exception as e:
        print(f"ERROR: {e}")
        return []

def transform_listing(raw):
    url = raw.get("url", raw.get("propertyUrl", ""))
    property_id = str(raw.get("id", hash(url)))
    address = raw.get("address", raw.get("displayAddress", "Unknown"))
    area = "Unknown"
    for area_name in SEARCH_URLS.keys():
        if area_name.lower() in address.lower():
            area = area_name
            break
    return {
        "id": property_id, "url": url,
        "price": str(raw.get("price", "Unknown")),
        "address": address,
        "bedrooms": raw.get("bedrooms", 4),
        "property_type": raw.get("propertyType", ""),
        "area": area, "scraped_at": datetime.now().isoformat(),
    }

def load_seen_listings():
    if SEEN_FILE.exists():
        with open(SEEN_FILE) as f: return set(json.load(f))
    return set()

def save_seen_listings(seen):
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(SEEN_FILE, "w") as f: json.dump(list(seen), f)

def scrape_all_areas(areas=None):
    if areas is None: areas = list(SEARCH_URLS.keys())
    urls = [SEARCH_URLS[a] for a in areas if a in SEARCH_URLS]
    if not urls: return [], []
    raw_results = run_apify_actor(urls)
    seen = load_seen_listings()
    all_listings, new_listings = [], []
    for raw in raw_results:
        listing = transform_listing(raw)
        all_listings.append(listing)
        if listing["id"] not in seen:
            new_listings.append(listing)
            seen.add(listing["id"])
    save_seen_listings(seen)
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(LISTINGS_FILE, "w") as f: json.dump(all_listings, f, indent=2)
    with open(NEW_LISTINGS_FILE, "w") as f: json.dump(new_listings, f, indent=2)
    print(f"Total: {len(all_listings)} listings, {len(new_listings)} new")
    return all_listings, new_listings

def test_connection():
    try:
        from apify_client import ApifyClient
        client = ApifyClient(APIFY_API_TOKEN)
        print(f"Connected as: {client.user().get().get('username')}")
        return True
    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()
    if args.test: test_connection()
    else: scrape_all_areas()
