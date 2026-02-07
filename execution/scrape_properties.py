"""
Property Scraper using Apify

Scrapes property listings from Rightmove via Apify actor.

Usage:
    python execution/scrape_properties.py --areas "Hitchin,Potters Bar,Dartford"
    python execution/scrape_properties.py --all
    python execution/scrape_properties.py --test

Setup:
    1. Create Apify account: https://apify.com/ (free tier: $5/month)
    2. Get API token from: https://console.apify.com/account/integrations
    3. Add to .env: APIFY_API_TOKEN=your_token

Actor used: dhrumil/rightmove-scraper
Docs: https://apify.com/dhrumil/rightmove-scraper
"""

import os
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
OUTPUT_DIR = Path(".tmp")
LISTINGS_FILE = OUTPUT_DIR / "listings.json"
NEW_LISTINGS_FILE = OUTPUT_DIR / "new_listings.json"
SEEN_FILE = OUTPUT_DIR / "seen_listings.json"

# Apify actor ID
ACTOR_ID = "dhrumil/rightmove-scraper"

# Search URLs for each area (Rightmove search result pages)
# 4 bed, detached/semi-detached, £600k-£900k, no shared ownership, added in last 30 days
SEARCH_URLS = {
    # North/Hertfordshire
    "Hitchin": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E61356&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    "Potters Bar": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E1040&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    "Welwyn Garden City": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E1326&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    "Watford": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E1306&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    "Barnet": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E93536&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    "Hatch End": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E61267&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    # South/Kent
    "Dartford": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E330&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    "Gravesend": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E513&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    "Orpington": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E949&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    # South/Surrey
    "Sutton": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E40444&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
    "Purley": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E1056&minBedrooms=4&maxBedrooms=4&minPrice=600000&maxPrice=900000&propertyTypes=detached%2Csemi-detached&dontShow=sharedOwnership&maxDaysSinceAdded=30&sortType=6",
}


def run_apify_actor(search_urls: list[str], max_items: int = 100) -> list[dict]:
    """
    Run Apify Rightmove scraper actor.

    Args:
        search_urls: List of Rightmove search URLs
        max_items: Maximum items to scrape per URL

    Returns:
        List of property dictionaries
    """
    try:
        from apify_client import ApifyClient
    except ImportError:
        print("ERROR: apify-client not installed.")
        print("Run: pip install apify-client")
        return []

    if not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_TOKEN not set in .env")
        print("Get token from: https://console.apify.com/account/integrations")
        return []

    client = ApifyClient(APIFY_API_TOKEN)

    # Prepare actor input
    actor_input = {
        "startUrls": [{"url": url} for url in search_urls],
        "maxItems": max_items,
        "proxy": {
            "useApifyProxy": True,
        },
    }

    print(f"Running Apify actor: {ACTOR_ID}")
    print(f"Scraping {len(search_urls)} search URLs...")

    try:
        # Run the actor and wait for completion
        run = client.actor(ACTOR_ID).call(run_input=actor_input)

        # Get results from dataset
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())

        print(f"Scraped {len(items)} properties")
        return items

    except Exception as e:
        print(f"ERROR running Apify actor: {e}")
        return []


def transform_listing(raw: dict, area: str) -> dict:
    """
    Transform raw Apify result to standardized format.

    Args:
        raw: Raw listing from Apify
        area: Area name

    Returns:
        Standardized listing dict
    """
    # Extract property ID from URL
    url = raw.get("url", raw.get("propertyUrl", ""))
    property_id = ""
    if "/properties/" in url:
        property_id = url.split("/properties/")[1].split("/")[0].split("#")[0]
    elif "propertyId" in raw:
        property_id = str(raw.get("propertyId", ""))

    return {
        "id": property_id or raw.get("id", str(hash(url))),
        "url": url,
        "price": raw.get("price", raw.get("displayPrice", "Unknown")),
        "address": raw.get("address", raw.get("displayAddress", "Unknown")),
        "bedrooms": raw.get("bedrooms", 4),
        "bathrooms": raw.get("bathrooms", ""),
        "property_type": raw.get("propertySubType", raw.get("propertyType", "")),
        "description": raw.get("summary", raw.get("description", ""))[:300],
        "agent": raw.get("agent", {}).get("name", raw.get("branchName", "")),
        "added_date": raw.get("addedOrReduced", raw.get("listingUpdateDate", "")),
        "images": raw.get("images", raw.get("propertyImages", []))[:3],
        "area": area,
        "scraped_at": datetime.now().isoformat(),
        "source": "rightmove",
    }


def load_seen_listings() -> set:
    """Load set of previously seen listing IDs."""
    if SEEN_FILE.exists():
        with open(SEEN_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_seen_listings(seen: set):
    """Save seen listing IDs."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)


def scrape_all_areas(areas: list[str] = None) -> tuple[list[dict], list[dict]]:
    """
    Scrape all configured areas via Apify.

    Args:
        areas: List of area names to scrape (None = all)

    Returns:
        Tuple of (all_listings, new_listings)
    """
    if areas is None:
        areas = list(SEARCH_URLS.keys())

    # Filter to valid areas
    urls_to_scrape = []
    area_map = {}  # Map URL to area name

    for area in areas:
        if area not in SEARCH_URLS:
            print(f"Unknown area: {area}")
            continue
        url = SEARCH_URLS[area]
        urls_to_scrape.append(url)
        area_map[url] = area

    if not urls_to_scrape:
        print("No valid areas to scrape")
        return [], []

    # Run Apify scraper
    raw_results = run_apify_actor(urls_to_scrape)

    # Load seen listings
    seen = load_seen_listings()
    all_listings = []
    new_listings = []

    for raw in raw_results:
        # Determine area from URL
        url = raw.get("url", raw.get("propertyUrl", ""))
        area = "Unknown"
        for search_url, area_name in area_map.items():
            # Match based on location identifier in URL
            if any(part in url for part in search_url.split("&")[0:2]):
                area = area_name
                break

        # If we can't determine area, try to infer from address
        if area == "Unknown":
            address = raw.get("address", raw.get("displayAddress", "")).lower()
            for area_name in SEARCH_URLS.keys():
                if area_name.lower() in address:
                    area = area_name
                    break

        listing = transform_listing(raw, area)
        all_listings.append(listing)

        if listing["id"] not in seen:
            new_listings.append(listing)
            seen.add(listing["id"])

    # Save updated seen list
    save_seen_listings(seen)

    # Save all listings
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(LISTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(all_listings, f, indent=2, ensure_ascii=False)

    # Save new listings separately
    with open(NEW_LISTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(new_listings, f, indent=2, ensure_ascii=False)

    print(f"\nTotal: {len(all_listings)} listings, {len(new_listings)} new")
    print(f"Saved to: {LISTINGS_FILE}")

    return all_listings, new_listings


def test_connection():
    """Test Apify API connection."""
    try:
        from apify_client import ApifyClient
    except ImportError:
        print("ERROR: apify-client not installed.")
        print("Run: pip install apify-client")
        return False

    if not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_TOKEN not set in .env")
        return False

    try:
        client = ApifyClient(APIFY_API_TOKEN)
        user = client.user().get()
        print(f"Connected to Apify as: {user.get('username', 'Unknown')}")
        print(f"Plan: {user.get('plan', {}).get('name', 'Unknown')}")
        return True
    except Exception as e:
        print(f"ERROR connecting to Apify: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Scrape properties via Apify")
    parser.add_argument(
        "--areas",
        type=str,
        help="Comma-separated list of areas to search"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Search all configured areas"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test Apify connection"
    )
    parser.add_argument(
        "--list-areas",
        action="store_true",
        help="List available search areas"
    )

    args = parser.parse_args()

    if args.list_areas:
        print("Available search areas:")
        for area in SEARCH_URLS:
            print(f"  - {area}")
        return

    if args.test:
        success = test_connection()
        exit(0 if success else 1)

    areas = None
    if args.areas:
        areas = [a.strip() for a in args.areas.split(",")]

    all_listings, new_listings = scrape_all_areas(areas)

    if new_listings:
        print(f"\nNew listings saved to: {NEW_LISTINGS_FILE}")


if __name__ == "__main__":
    main()
