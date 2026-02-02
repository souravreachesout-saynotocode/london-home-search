import os, json, argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
OUTPUT_DIR = Path(".tmp")
SHEET_ID_FILE = OUTPUT_DIR / "sheet_id.txt"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]

def get_google_creds():
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("ERROR: pip install google-auth google-auth-oauthlib google-api-python-client"); return None
    creds = None
    if Path("token.json").exists():
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not Path("credentials.json").exists():
                print("ERROR: credentials.json not found"); return None
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f: f.write(creds.to_json())
    return creds

def get_sheets_service():
    from googleapiclient.discovery import build
    creds = get_google_creds()
    return build("sheets", "v4", credentials=creds) if creds else None

def create_sheet(title):
    service = get_sheets_service()
    if not service: return None
    spreadsheet = {"properties": {"title": title}, "sheets": [{"properties": {"title": "Property Listings"}}, {"properties": {"title": "Search Areas"}}]}
    result = service.spreadsheets().create(body=spreadsheet).execute()
    sheet_id = result["spreadsheetId"]
    headers = [["Date Added", "Area", "Price", "Address", "Bedrooms", "Property Type", "URL", "Status", "Notes", "Rating"]]
    service.spreadsheets().values().update(spreadsheetId=sheet_id, range="Property Listings!A1:J1", valueInputOption="RAW", body={"values": headers}).execute()
    area_data = [["Area", "Commute", "Grammar", "Primary", "Priority"],
        ["Hitchin", "38-45 min", "Hitchin Boys/Girls", "Highover", "1"],
        ["Potters Bar", "33-37 min", "Dame Alice Owen's", "Cranborne", "2"],
        ["Dartford", "33-38 min", "Dartford Boys/Girls", "Leigh Academy", "3"],
        ["Gravesend", "40-45 min", "Gravesend Grammar", "Cobham", "4"],
        ["Welwyn Garden City", "~40 min", "Hitchin access", "Templewood", "5"]]
    service.spreadsheets().values().update(spreadsheetId=sheet_id, range="Search Areas!A1:E6", valueInputOption="RAW", body={"values": area_data}).execute()
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(SHEET_ID_FILE, "w") as f: f.write(sheet_id)
    print(f"Created: https://docs.google.com/spreadsheets/d/{sheet_id}")
    return sheet_id

def get_sheet_id():
    return SHEET_ID_FILE.read_text().strip() if SHEET_ID_FILE.exists() else None

def upload_listings(listings, mark_new=False):
    service = get_sheets_service()
    sheet_id = get_sheet_id()
    if not service: return []
    if not sheet_id: sheet_id = create_sheet("London Home Search")
    if not listings: print("No listings"); return []
    rows = [[datetime.now().strftime("%Y-%m-%d"), l.get("area",""), l.get("price",""), l.get("address",""), str(l.get("bedrooms",4)), l.get("property_type",""), l.get("url",""), "New", "", ""] for l in listings]
    service.spreadsheets().values().append(spreadsheetId=sheet_id, range="Property Listings!A:J", valueInputOption="RAW", insertDataOption="INSERT_ROWS", body={"values": rows}).execute()
    print(f"Uploaded {len(rows)} listings")
    return listings

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--create", type=str)
    parser.add_argument("--upload", type=Path)
    parser.add_argument("--sheet-url", action="store_true")
    args = parser.parse_args()
    if args.create: create_sheet(args.create)
    elif args.upload:
        with open(args.upload) as f: upload_listings(json.load(f))
    elif args.sheet_url:
        sid = get_sheet_id()
        print(f"https://docs.google.com/spreadsheets/d/{sid}" if sid else "No sheet")
