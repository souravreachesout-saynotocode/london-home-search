"""
Google Sheets Manager for Property Listings

Handles reading/writing property listings to Google Sheets.

Usage:
    python execution/sheets_manager.py --create "London Home Search"
    python execution/sheets_manager.py --upload .tmp/listings.json
    python execution/sheets_manager.py --add-new .tmp/new_listings.json

Output:
    Creates/updates Google Sheet with property listings

Setup (Option 1 - Service Account for CI/CD):
    1. Create Service Account at https://console.cloud.google.com/iam-admin/serviceaccounts
    2. Download JSON key
    3. Set GOOGLE_SERVICE_ACCOUNT env var with JSON contents
    4. Share your Google Sheet with the service account email

Setup (Option 2 - OAuth for local development):
    1. Go to https://console.cloud.google.com/apis/credentials
    2. Create OAuth 2.0 Client ID (Desktop app)
    3. Download JSON and save as 'credentials.json' in project root
    4. First run will open browser for authorization
"""

import os
import json
import argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = Path(".tmp")
SHEET_ID_FILE = OUTPUT_DIR / "sheet_id.txt"

# Store sheet ID in env for CI (no persistent filesystem)
SHEET_ID_ENV = os.getenv("GOOGLE_SHEET_ID", "")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file"
]

# Distance from Moorgate station (km) for each area
AREA_DISTANCES = {
    "Hitchin": 56,
    "Potters Bar": 27,
    "Welwyn Garden City": 35,
    "Watford": 27,
    "Barnet": 16,
    "Hatch End": 24,
    "Dartford": 29,
    "Gravesend": 39,
    "Orpington": 24,
    "Sutton": 21,
    "Purley": 22,
}


def get_google_creds():
    """Get Google API credentials. Tries Service Account first, then OAuth."""
    # Option 1: Service Account (for CI/CD environments like GitHub Actions)
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT")
    if service_account_json:
        try:
            from google.oauth2 import service_account
            info = json.loads(service_account_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            print("Using Service Account authentication")
            return creds
        except Exception as e:
            print(f"ERROR with Service Account: {e}")
            return None

    # Option 2: OAuth flow (for local development)
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("ERROR: Google libraries not installed.")
        print("Run: pip install google-auth google-auth-oauthlib google-api-python-client")
        return None

    creds = None
    token_path = Path("token.json")
    credentials_path = Path("credentials.json")

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not credentials_path.exists():
                print("ERROR: credentials.json not found!")
                print("Download from Google Cloud Console:")
                print("  1. Go to https://console.cloud.google.com/apis/credentials")
                print("  2. Create OAuth 2.0 Client ID (Desktop app)")
                print("  3. Download JSON and save as 'credentials.json'")
                return None

            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, "w") as token:
            token.write(creds.to_json())

    return creds


def get_sheets_service():
    """Get Google Sheets API service."""
    try:
        from googleapiclient.discovery import build
    except ImportError:
        return None

    creds = get_google_creds()
    if not creds:
        return None

    return build("sheets", "v4", credentials=creds)


def create_sheet(title: str) -> str | None:
    """
    Create a new Google Sheet for property listings.

    Args:
        title: Name for the spreadsheet

    Returns:
        Spreadsheet ID, or None if failed
    """
    service = get_sheets_service()
    if not service:
        return None

    try:
        spreadsheet = {
            "properties": {"title": title},
            "sheets": [
                {
                    "properties": {
                        "title": "Property Listings",
                        "gridProperties": {"frozenRowCount": 1}
                    }
                },
                {
                    "properties": {
                        "title": "Search Areas",
                        "gridProperties": {"frozenRowCount": 1}
                    }
                },
                {
                    "properties": {
                        "title": "Seen Listings",
                        "gridProperties": {"frozenRowCount": 1}
                    }
                }
            ]
        }

        result = service.spreadsheets().create(body=spreadsheet).execute()
        sheet_id = result["spreadsheetId"]

        # Add headers to Property Listings
        headers = [[
            "Date Added", "Date Listed", "Area", "Distance (km)", "Price",
            "Address", "Bedrooms", "Property Type", "URL", "Status", "Notes"
        ]]

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="Property Listings!A1:K1",
            valueInputOption="RAW",
            body={"values": headers}
        ).execute()

        # Add search areas info
        area_headers = [[
            "Area", "Commute to Moorgate", "Grammar Schools", "Outstanding Primary", "Priority"
        ]]

        area_data = [
            ["Hitchin", "38-45 min", "Hitchin Boys/Girls", "Highover, St Andrew's", "56 km"],
            ["Potters Bar", "33-37 min", "Dame Alice Owen's", "Cranborne, Wroxham", "27 km"],
            ["Welwyn Garden City", "~40 min", "Access to Hitchin", "Templewood, Applecroft", "35 km"],
            ["Watford", "25-30 min", "Watford Grammar Boys/Girls", "Various", "27 km"],
            ["Barnet", "30-35 min", "QE Boys, Henrietta Barnett", "Various", "16 km"],
            ["Hatch End", "~30 min", "Near Watford/Harrow grammars", "Various", "24 km"],
            ["Dartford", "33-38 min", "Dartford Boys/Girls", "Leigh Academy", "29 km"],
            ["Gravesend", "40-45 min", "Gravesend Grammar", "Cobham Primary", "39 km"],
            ["Orpington", "35-40 min", "Newstead Wood, St Olave's", "Various", "24 km"],
            ["Sutton", "35-40 min", "Wilson's, Nonsuch, Wallington", "Various", "21 km"],
            ["Purley", "30-35 min", "Near Sutton grammars", "Various", "22 km"],
        ]

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="Search Areas!A1:E12",
            valueInputOption="RAW",
            body={"values": area_headers + area_data}
        ).execute()

        # Add headers to Seen Listings
        seen_headers = [["Listing ID", "First Seen Date"]]

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="Seen Listings!A1:B1",
            valueInputOption="RAW",
            body={"values": seen_headers}
        ).execute()

        # Save sheet ID
        OUTPUT_DIR.mkdir(exist_ok=True)
        with open(SHEET_ID_FILE, "w") as f:
            f.write(sheet_id)

        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        print(f"Created sheet: {sheet_url}")
        return sheet_id

    except Exception as e:
        print(f"ERROR creating sheet: {str(e)}")
        return None


def get_sheet_id() -> str | None:
    """Get saved sheet ID from env var or file."""
    # Check env var first (for CI/CD)
    if SHEET_ID_ENV:
        return SHEET_ID_ENV
    # Fall back to file (for local dev)
    if SHEET_ID_FILE.exists():
        with open(SHEET_ID_FILE, "r") as f:
            return f.read().strip()
    return None


def get_seen_listings() -> set:
    """Get set of listing IDs already in the sheet."""
    service = get_sheets_service()
    sheet_id = get_sheet_id()

    if not service or not sheet_id:
        return set()

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="Seen Listings!A:A"
        ).execute()

        values = result.get("values", [])
        return {row[0] for row in values[1:] if row}

    except Exception as e:
        print(f"ERROR reading seen listings: {str(e)}")
        return set()


def upload_listings(listings: list[dict], mark_new: bool = False) -> list[dict]:
    """
    Upload property listings to Google Sheet.

    Args:
        listings: List of listing dictionaries
        mark_new: Whether to only upload new listings

    Returns:
        List of newly added listings
    """
    service = get_sheets_service()
    sheet_id = get_sheet_id()

    if not service:
        return []

    if not sheet_id:
        print("No sheet found. Creating one...")
        sheet_id = create_sheet("London Home Search")
        if not sheet_id:
            return []

    seen = get_seen_listings()
    new_listings = []

    # Filter to new listings if requested
    if mark_new:
        listings = [l for l in listings if l.get("id") not in seen]

    if not listings:
        print("No new listings to upload")
        return []

    # Filter out listings with Unknown area
    listings = [l for l in listings if l.get("area", "Unknown") != "Unknown"]

    if not listings:
        print("No listings with valid areas to upload")
        return []

    print(f"Uploading {len(listings)} listings...")

    try:
        rows = []
        seen_rows = []

        for listing in listings:
            area = listing.get("area", "")
            distance = AREA_DISTANCES.get(area, "")

            row = [
                datetime.now().strftime("%Y-%m-%d"),
                listing.get("added_date", ""),
                area,
                str(distance) if distance else "",
                listing.get("price", ""),
                listing.get("address", ""),
                str(listing.get("bedrooms", 4)),
                listing.get("property_type", listing.get("description", "")),
                listing.get("url", ""),
                "New",
                ""
            ]
            rows.append(row)

            # Track as seen
            listing_id = listing.get("id", listing.get("url", ""))
            if listing_id not in seen:
                seen_rows.append([listing_id, datetime.now().isoformat()])
                new_listings.append(listing)

        # Insert at row 2 (after header) so newest listings appear first
        # Using batchUpdate to insert rows
        requests = [{
            "insertDimension": {
                "range": {
                    "sheetId": 0,  # First sheet (Property Listings)
                    "dimension": "ROWS",
                    "startIndex": 1,  # After header row
                    "endIndex": 1 + len(rows)
                },
                "inheritFromBefore": False
            }
        }]

        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests}
        ).execute()

        # Now update the newly inserted rows with data
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"Property Listings!A2:K{1 + len(rows)}",
            valueInputOption="RAW",
            body={"values": rows}
        ).execute()

        # Append to Seen Listings
        if seen_rows:
            service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range="Seen Listings!A:B",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": seen_rows}
            ).execute()

        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        print(f"Uploaded {len(rows)} listings to: {sheet_url}")

        return new_listings

    except Exception as e:
        print(f"ERROR uploading to sheet: {str(e)}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Manage Google Sheets for property search")
    parser.add_argument(
        "--create",
        type=str,
        metavar="TITLE",
        help="Create a new sheet"
    )
    parser.add_argument(
        "--upload",
        type=Path,
        metavar="FILE",
        help="Upload listings from JSON file"
    )
    parser.add_argument(
        "--add-new",
        type=Path,
        metavar="FILE",
        help="Upload only new listings from JSON file"
    )
    parser.add_argument(
        "--sheet-url",
        action="store_true",
        help="Print the current sheet URL"
    )

    args = parser.parse_args()

    if args.create:
        create_sheet(args.create)

    elif args.upload:
        if not args.upload.exists():
            print(f"File not found: {args.upload}")
            exit(1)
        with open(args.upload, "r") as f:
            listings = json.load(f)
        upload_listings(listings, mark_new=False)

    elif args.add_new:
        if not args.add_new.exists():
            print(f"File not found: {args.add_new}")
            exit(1)
        with open(args.add_new, "r") as f:
            listings = json.load(f)
        upload_listings(listings, mark_new=True)

    elif args.sheet_url:
        sheet_id = get_sheet_id()
        if sheet_id:
            print(f"https://docs.google.com/spreadsheets/d/{sheet_id}")
        else:
            print("No sheet configured. Run with --create first.")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
