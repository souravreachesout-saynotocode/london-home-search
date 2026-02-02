# London Home Search

Automated pipeline for searching London property listings.

## Setup
1. Copy .env.example to .env and add your API keys
2. pip install -r requirements.txt
3. Copy credentials.json and token.json from parenting-content-pipeline

## Usage
- Test Apify: python execution/scrape_properties.py --test
- Test WhatsApp: python execution/send_whatsapp.py --test  
- Create Sheet: python execution/sheets_manager.py --create "London Home Search"
- Full run: python execution/daily_runner.py

## Search Areas
1. Hitchin (38-45 min)
2. Potters Bar (33-37 min)
3. Dartford (33-38 min)
4. Gravesend (40-45 min)
5. Welwyn Garden City (~40 min)
