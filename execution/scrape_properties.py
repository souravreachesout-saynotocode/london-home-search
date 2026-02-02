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
    "Hitchin": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E61356&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&maxDaysSinceAdded=30&sortType=6",
    "Potters Bar": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E1040&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&maxDaysSinceAdded=30&sortType=6",
    "Dartford": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E330&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&maxDaysSinceAdded=30&sortType=6",
    "Gravesend": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E513&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&maxDaysSinceAdded=30&sortType=6",
    "Welwyn Garden City": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E1326&minBedrooms=4&maxBedrooms=4&propertyTypes=detached%2Csemi-detached&maxDaysSinceAdded=30&sortType=6",
}

def run_apify_actor(search_urls, max_items=100):
    try:
        from apify_client import ApifyClient
    except ImportError:
        print("ERROR: pip install apify-client")
        return []
    if not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_TOKEN not set in .env")
        return []
    client = ApifyClient(APIFY_API_TOKEN)
    actor_input = {"startUrls": [{"url": url} for url in search_urls], "maxItems": max_items, "proxy": {"useApifyProxy": True}}
    print(f"Running Apify actor: {ACTOR_ID}")
    try:
        run = client.actor(ACTOR_ID).call(run_input=actor_input)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"Scraped {len(items)} properties")
        return items
    except Exception as e:
        print(f"ERROR: {e}")
        return []

def transform_listing(raw, area):
    url = raw.get("url", raw.get("propertyUrl", ""))
    property_id = url.split("/properties/")[1].split("/")[0].split("#")[0] if "/properties/" in url else str(raw.get("propertyId", hash(url)))
    return {
        "id": property_id, "url": url,
        "price": raw.get("price", raw.get("displayPrice", "Unknown")),
        "address": raw.get("address", raw.get("displayAddress", "Unknown")),
        "bedrooms": raw.get("bedrooms", 4),
        "property_type": raw.get("propertySubType", raw.get("propertyType", "")),
        "description": raw.get("summary", raw.get("description", ""))[:300],
        "agent": raw.get("agent", {}).get("name", raw.get("branchName", "")),
        "area": area, "scraped_at": datetime.now().isoformat(), "source": "rightmove",
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
    urls_to_scrape = [SEARCH_URLS[a] for a in areas if a in SEARCH_URLS]
    area_map = {SEARCH_URLS[a]: a for a in areas if a in SEARCH_URLS}
    if not urls_to_scrape: return [], []
    raw_results = run_apify_actor(urls_to_scrape)
    seen = load_seen_listings()
    all_listings, new_listings = [], []
    for raw in raw_results:
        address = raw.get("address", raw.get("displayAddress", "")).lower()
        area = next((a for a in SEARCH_URLS.keys() if a.lower() in address), "Unknown")
        listing = transform_listing(raw, area)
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
    except ImportError:
        print("ERROR: pip install apify-client"); return False
    if not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_TOKEN not set"); return False
    try:
        client = ApifyClient(APIFY_API_TOKEN)
        user = client.user().get()
        print(f"Connected as: {user.get('username')}")
        return True
    except Exception as e:
        print(f"ERROR: {e}"); return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--areas", type=str)
    parser.add_argument("--list-areas", action="store_true")
    args = parser.parse_args()
    if args.list_areas: print("\n".join(SEARCH_URLS.keys()))
    elif args.test: exit(0 if test_connection() else 1)
    else: scrape_all_areas([a.strip() for a in args.areas.split(",")] if args.areas else None)
