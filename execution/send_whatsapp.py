import os, json, argparse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
WHATSAPP_TO = os.getenv("WHATSAPP_TO")
SHEET_ID_FILE = Path(".tmp/sheet_id.txt")

def get_sheet_url():
    return f"https://docs.google.com/spreadsheets/d/{SHEET_ID_FILE.read_text().strip()}" if SHEET_ID_FILE.exists() else ""

def send_whatsapp_message(message):
    try:
        from twilio.rest import Client
    except ImportError:
        print("ERROR: pip install twilio"); return False
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, WHATSAPP_TO]):
        print("ERROR: Missing Twilio credentials"); return False
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        msg = client.messages.create(from_=TWILIO_WHATSAPP_FROM, body=message, to=WHATSAPP_TO)
        print(f"WhatsApp sent! SID: {msg.sid}")
        return True
    except Exception as e:
        print(f"ERROR: {e}"); return False

def format_batch_message(listings):
    if not listings: return ""
    if len(listings) == 1:
        l = listings[0]
        return f"*New Property!*\n\n*{l.get('price')}*\n{l.get('address')}\nArea: {l.get('area')}\n\n{l.get('url')}"
    from collections import Counter
    areas = Counter(l.get("area", "Unknown") for l in listings)
    lines = [f"*{len(listings)} New Properties!*\n"]
    for area, count in areas.most_common(): lines.append(f"  {area}: {count}")
    lines.append("\n*Top 3:*")
    for l in listings[:3]: lines.append(f"\n{l.get('price')} - {l.get('area')}\n{l.get('address')[:50]}\n{l.get('url')}")
    if len(listings) > 3: lines.append(f"\n...and {len(listings)-3} more")
    sheet_url = get_sheet_url()
    if sheet_url: lines.append(f"\nView all: {sheet_url}")
    return "\n".join(lines)

def notify_new_listings(listings_file):
    if not listings_file.exists(): return False
    with open(listings_file) as f: listings = json.load(f)
    if not listings: return True
    return send_whatsapp_message(format_batch_message(listings))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--message", type=str)
    parser.add_argument("--listings", type=Path)
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()
    if args.test: exit(0 if send_whatsapp_message("Test: London Home Search working!") else 1)
    elif args.message: exit(0 if send_whatsapp_message(args.message) else 1)
    elif args.listings: exit(0 if notify_new_listings(args.listings) else 1)
