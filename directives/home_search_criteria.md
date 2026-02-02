# London Home Search Criteria

## Requirements
| Criteria | Requirement |
|----------|-------------|
| Commute | <=45 mins to London Moorgate |
| Grammar School | Within 15 mins drive |
| Primary School | Outstanding Ofsted-rated catchment |
| Bedrooms | 4 |
| Property Type | Detached or Semi-detached |

## Search Areas
| Priority | Area | Commute | Grammar | Outstanding Primary |
|----------|------|---------|---------|---------------------|
| 1 | Hitchin | 38-45 min | Hitchin Boys/Girls | Highover, St Andrews |
| 2 | Potters Bar | 33-37 min | Dame Alice Owens | Cranborne, Wroxham |
| 3 | Dartford | 33-38 min | Dartford Boys/Girls | Leigh Academy |
| 4 | Gravesend | 40-45 min | Gravesend Grammar | Cobham Primary |
| 5 | Welwyn Garden City | ~40 min | Access to Hitchin | Templewood, Applecroft |

## Commands
- Test Apify: python execution/scrape_properties.py --test
- Test WhatsApp: python execution/send_whatsapp.py --test
- Create Sheet: python execution/sheets_manager.py --create "London Home Search"
- Full run: python execution/daily_runner.py
- Dry run: python execution/daily_runner.py --dry-run
