import sys, json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent))
OUTPUT_DIR = Path(".tmp")

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_scraper():
    log("Scraping...")
    from scrape_properties import scrape_all_areas
    all_l, new_l = scrape_all_areas()
    log(f"Found {len(all_l)} total, {len(new_l)} new")
    return len(new_l) > 0

def update_sheet():
    log("Updating sheet...")
    from sheets_manager import upload_listings
    f = OUTPUT_DIR / "new_listings.json"
    if not f.exists(): return
    with open(f) as file: listings = json.load(file)
    upload_listings(listings, mark_new=True)

def send_notification():
    log("Sending WhatsApp...")
    from send_whatsapp import notify_new_listings
    notify_new_listings(OUTPUT_DIR / "new_listings.json")

def run_pipeline(dry_run=False):
    log("=" * 40)
    log("Starting pipeline")
    if not run_scraper():
        log("No new listings"); return
    update_sheet()
    if not dry_run: send_notification()
    log("Done!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--scrape-only", action="store_true")
    parser.add_argument("--notify-only", action="store_true")
    args = parser.parse_args()
    if args.scrape_only: run_scraper()
    elif args.notify_only: send_notification()
    else: run_pipeline(dry_run=args.dry_run)
