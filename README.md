# 1mg Drug & Medicine Scraper

**A high-performance, parallel web scraper that extracts structured pharmaceutical data from [1mg.com](https://www.1mg.com) — covering all drugs across every label (A-Z) with resume support and SQLite storage.**

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## Table of Contents

- [Features](#features)
- [Extracted Drug Data Fields](#extracted-drug-data-fields)
- [Installation](#installation)
- [Usage](#usage)
- [Checking Scraped Data](#checking-scraped-data)
- [Configuration](#configuration)
- [Database Schema](#database-schema)
- [How It Works](#how-it-works)
- [FAQ](#faq)

---

## Features

- **Parallel label processing** — scrapes all 26 alphabetic labels (A-Z) concurrently using thread pool
- **Structured drug data** — extracts 20+ data fields per medicine from JSON-LD structured markup
- **Resume / checkpoint support** — tracks progress per label per page; restart without losing work
- **Retry with exponential backoff** — handles rate limits (HTTP 429) and network failures gracefully
- **SQLite storage** — all drug data stored in a local database with WAL mode for concurrent writes
- **Failed slug tracking** — records failed drug pages for later retry and analysis
- **Graceful shutdown** — handles SIGINT/SIGTERM, flushes remaining data before exit
- **Dual logging** — file logs (DEBUG level) + console logs (INFO level)

---

## Extracted Drug Data Fields

| Field | Description |
|---|---|
| `sku_id` | Unique SKU identifier |
| `name` | Drug / medicine name |
| `url` | Full URL on 1mg |
| `slug` | URL slug (unique) |
| `active_ingredient` | Active pharmaceutical ingredient |
| `drug_unit` | Unit of measurement (mg, ml, etc.) |
| `dosage_form` | Tablet, capsule, injection, syrup, etc. |
| `administration_route` | Oral, intravenous, topical, etc. |
| `prescription_status` | Prescription required / OTC |
| `is_available_generically` | Generic availability flag |
| `is_proprietary` | Proprietary / branded flag |
| `non_proprietary_name` | Generic / chemical name |
| `description` | Drug description text |
| `pregnancy_warning` | Safety during pregnancy |
| `alcohol_warning` | Interaction with alcohol |
| `breastfeeding_warning` | Safety during breastfeeding |
| `food_warning` | Food interaction warnings |
| `mechanism_of_action` | How the drug works (JSON array) |
| `available_strength` | Available dosage strengths (JSON array) |
| `dose_schedule` | Recommended dosing schedule |
| `image` | Drug image URL |
| `brand_name` | Brand / manufacturer name |
| `date_published` | Publish date on 1mg |
| `date_modified` | Last modified date |
| `faqs` | Frequently asked questions (JSON) |
| `scraped_at` | Timestamp of scraping |

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/narender-rk10/1mg-drugs-medicine-scrapper.git
cd 1mg-drugs-medicine-scrapper

# 2. (Optional) Create a virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate      # Linux/Mac

# 3. Install dependencies
pip install -r requirements.txt
```

### Dependencies

- **Python 3.8+**
- `requests` — HTTP requests
- `beautifulsoup4` — HTML parsing

---

## Usage

### Run the Full Scraper

```bash
python scraper_bg.py
```

This will:
1. Scan all listing pages for labels **a** through **z**
2. Collect all drug slugs from listing pages
3. Visit each drug page and extract structured data
4. Store everything in `1mg_medicines.db`

**The scrape can be interrupted (Ctrl+C) and restarted** — progress is checkpointed per label and per page, so no duplicate work is done.

### Scraping Time Estimate

- ~100,000+ medicines across all labels
- ~1-3 seconds per drug page (to respect rate limits)
- With 26 parallel workers, full scrape: **~2-4 hours** depending on network

---

## Checking Scraped Data

```bash
python check_db.py
```

Outputs a summary including:
- Total medicines stored
- Failed slugs count
- Per-label drug counts
- Listing page progress

---

## Configuration

All configuration lives at the top of `scraper_bg.py`:

| Parameter | Default | Description |
|---|---|---|
| `MAX_WORKERS` | `26` | Number of parallel threads (one per label) |
| `REQUEST_DELAY` | `1.2` | Delay in seconds between requests per thread |
| `MAX_RETRIES` | `4` | Maximum retry attempts for failed requests |
| `DB_WRITE_INTERVAL` | `50` | Drugs to buffer before flushing to database |
| `DB_PATH` | `1mg_medicines.db` | SQLite database file path |
| `LOG_FILE` | `scraper.log` | Log file path |

---

## Database Schema

### `medicines` table
Stores all scraped drug/medicine data with `slug` as the unique key.

### `scrape_progress` table
Tracks scraping progress per label and listing page — enables resume on restart.

### `failed_slugs` table
Logs drug pages that could not be scraped, with error messages and retry counts.

---

## How It Works

```
┌─────────────────────────────────────────────────────┐
│                scraper_bg.py (Main)                  │
│                                                      │
│  ThreadPoolExecutor (26 workers, one per label)      │
│                                                      │
│  ┌──────────┐  ┌──────────┐       ┌──────────┐     │
│  │ Label A  │  │ Label B  │  ...  │ Label Z  │     │
│  │ Worker   │  │ Worker   │       │ Worker   │     │
│  └────┬─────┘  └────┬─────┘       └────┬─────┘     │
│       │             │                  │            │
│       ▼             ▼                  ▼            │
│  ┌──────────────────────────────────────────────┐   │
│  │ Phase 1: Fetch listing pages                 │   │
│  │   /drugs-all-medicines?page=N&label=A        │   │
│  │   Extract slugs from __INITIAL_STATE__ JSON   │   │
│  └────────────────────┬─────────────────────────┘   │
│                       ▼                             │
│  ┌──────────────────────────────────────────────┐   │
│  │ Phase 2: Fetch individual drug pages          │   │
│  │   /drugs/<slug>                               │   │
│  │   Extract Drug JSON-LD structured data        │   │
│  └────────────────────┬─────────────────────────┘   │
│                       ▼                             │
│                 ┌──────────┐                        │
│                 │ SQLite   │                        │
│                 │ Database │                        │
│                 └──────────┘                        │
└─────────────────────────────────────────────────────┘
```

---

## FAQ

**Q: Is this legal?**
This tool scrapes publicly available data from 1mg.com for research and educational purposes. Always respect `robots.txt`, rate limits, and the website's terms of service. Use responsibly.

**Q: Can I filter by specific labels?**
Yes. Edit the `LABELS` list in `scraper_bg.py` to include only the letters you need, e.g., `LABELS = ["a", "b", "c"]`.

**Q: How do I export data to CSV/JSON?**
The data is in SQLite. Use any SQLite browser or run:
```bash
python -c "import sqlite3, csv; conn=sqlite3.connect('1mg_medicines.db'); rows=conn.execute('SELECT * FROM medicines'); csv.writer(open('drugs.csv','w',newline='')).writerows([d[0] for d in rows.description]); csv.writer(open('drugs.csv','a',newline='')).writerows(rows)"
```

**Q: Where is the log file?**
All debug logs are written to `scraper.log` in the project directory.

---

## License

MIT — Use freely for research, education, and personal projects.
