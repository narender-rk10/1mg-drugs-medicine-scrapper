#!/usr/bin/env python3
"""
1mg Medicine Scraper - Background / Parallel Edition
----------------------------------------------------
Scrapes all medicines from https://www.1mg.com/drugs-all-medicines
across all labels (a-z) and stores structured data in SQLite.

Features:
- Parallel label processing (configurable worker count)
- SQLite storage with checkpoint/resume
- Retry with exponential backoff
- File + console logging
"""

import requests
import json
import re
import time
import os
import sys
import sqlite3
import logging
import random
import signal
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

# ───────────────────── Configuration ───────────────────── #

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

BASE_URL = "https://www.1mg.com"
LISTING_URL = f"{BASE_URL}/drugs-all-medicines"
DB_PATH = "1mg_medicines.db"
LOG_FILE = "scraper.log"

LABELS = [chr(c) for c in range(ord("a"), ord("z") + 1)]
MAX_WORKERS = 26         # parallel label workers
REQUEST_DELAY = 1.2      # seconds between requests per thread
MAX_RETRIES = 4
DB_WRITE_INTERVAL = 50   # flush to DB every N drugs

# Graceful shutdown flag
SHUTDOWN = threading.Event()

# ───────────────────── Logging Setup ───────────────────── #

def setup_logging():
    logger = logging.getLogger("scraper")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(threadName)-15s | %(message)s"
    ))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(threadName)-15s | %(message)s",
        datefmt="%H:%M:%S"
    ))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

log = setup_logging()

# ───────────────────── SQLite Setup ───────────────────── #

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS medicines (
            sku_id          INTEGER PRIMARY KEY,
            name            TEXT,
            url             TEXT,
            slug            TEXT UNIQUE,
            active_ingredient TEXT,
            drug_unit       TEXT,
            dosage_form     TEXT,
            administration_route TEXT,
            prescription_status TEXT,
            is_available_generically INTEGER,
            is_proprietary  INTEGER,
            non_proprietary_name TEXT,
            description     TEXT,
            pregnancy_warning TEXT,
            alcohol_warning TEXT,
            breastfeeding_warning TEXT,
            food_warning    TEXT,
            mechanism_of_action TEXT,
            available_strength TEXT,
            dose_schedule   TEXT,
            image           TEXT,
            brand_name      TEXT,
            date_published  TEXT,
            date_modified   TEXT,
            faqs            TEXT,
            scraped_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(slug)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_progress (
            label           TEXT,
            listing_page    INTEGER,
            status          TEXT DEFAULT 'pending',
            updated_at      TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (label, listing_page)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS failed_slugs (
            slug            TEXT PRIMARY KEY,
            label           TEXT,
            error           TEXT,
            retries         INTEGER DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)

    conn.commit()
    return conn


def get_db_connection():
    """Create a per-thread DB connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

# ───────────────────── Network ───────────────────── #

def fetch_page(url, max_retries=MAX_RETRIES):
    """Fetch a URL with retry + exponential backoff."""
    for attempt in range(max_retries):
        if SHUTDOWN.is_set():
            return None
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 429:
                wait = (2 ** attempt) * 2 + random.uniform(0, 1)
                log.warning(f"429 rate-limited on {url}, sleeping {wait:.1f}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            wait = (2 ** attempt) + random.uniform(0.5, 1.5)
            log.warning(f"Attempt {attempt+1}/{max_retries} failed for {url}: {e} | retry in {wait:.1f}s")
            if attempt < max_retries - 1:
                time.sleep(wait)
    return None

# ───────────────────── Parsing ───────────────────── #

def extract_listing_json(html):
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script"):
        if not script.string:
            continue
        s = script.string
        if "__INITIAL_STATE__" in s and "window.__INITIAL_STATE__" in s:
            idx = s.index("window.__INITIAL_STATE__")
            eq_idx = s.index("=", idx)
            json_start = eq_idx + 1
            router_idx = s.find("window.__ROUTER_INITIAL_DATA__", json_start)
            if router_idx > 0:
                json_text = s[json_start:router_idx].rstrip().rstrip(";").rstrip()
            else:
                json_text = s[json_start:].strip().rstrip(";")
            try:
                return json.loads(json_text)
            except json.JSONDecodeError:
                pass

        # Fallback: try __ROUTER_INITIAL_DATA__
        if "__ROUTER_INITIAL_DATA__" in s and "window.__ROUTER_INITIAL_DATA__" in s:
            idx = s.index("window.__ROUTER_INITIAL_DATA__")
            eq_idx = s.index("=", idx)
            json_start = eq_idx + 1
            json_text = s[json_start:].strip().rstrip(";")
            try:
                data = json.loads(json_text)
                # Router data has a different structure; extract the listing data
                for key in data:
                    if "allMedicinesPage" in str(key).lower() or "drugs-all" in str(key).lower():
                        page_data = data[key]
                        if isinstance(page_data, dict):
                            inner = page_data.get("data", {})
                            payload = inner.get("payload", inner)
                            skus = payload.get("data", {}).get("skus", [])
                            pagination = payload.get("meta", {})
                            medicines = []
                            for s in skus:
                                medicines.append({
                                    "slug": s.get("slug"),
                                    "title": s.get("name"),
                                })
                            return medicines, {
                                "totalPages": pagination.get("total_pages", pagination.get("totalPages", 1)),
                                "totalResult": pagination.get("total_count", pagination.get("totalResult", 0)),
                            }
                # Generic extraction
                for key, val in data.items():
                    if isinstance(val, dict):
                        inner = val.get("data", {})
                        payload = inner.get("payload", inner)
                        skus = payload.get("data", {}).get("skus", [])
                        if skus:
                            medicines = [{"slug": s.get("slug"), "title": s.get("name")} for s in skus]
                            meta = payload.get("meta", {})
                            return medicines, {
                                "totalPages": meta.get("total_pages", meta.get("totalPages", 1)),
                            }
            except (json.JSONDecodeError, Exception):
                pass
    return None


def get_listing_data(label, page):
    url = f"{LISTING_URL}?page={page}&label={label}"
    resp = fetch_page(url)
    if not resp:
        return None
    data = extract_listing_json(resp.text)
    if not data:
        log.warning(f"Label={label} page={page}: could not extract JSON")
        return None
    reducer = data.get("allMedicinesPageReducer", {})
    medicines = reducer.get("data", {}).get("medicines", [])
    pagination = reducer.get("data", {}).get("paginationStatus", {})
    return medicines, pagination


def extract_drug_json_ld(html):
    soup = BeautifulSoup(html, "html.parser")
    drug_schema = None
    faq_schema = None
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except json.JSONDecodeError:
            continue
        if data.get("@type") == "Drug":
            drug_schema = data
        elif data.get("@type") == "FAQPage":
            faq_schema = data
    return drug_schema, faq_schema


def scrape_drug_page(slug):
    if slug.startswith("http"):
        url = slug
    else:
        url = f"{BASE_URL}{slug}" if slug.startswith("/") else f"{BASE_URL}/drugs/{slug}"

    resp = fetch_page(url)
    if not resp:
        return None

    drug_data, faq_schema = extract_drug_json_ld(resp.text)
    if not drug_data:
        log.warning(f"No Drug schema found for {slug}")
        return None

    # Extract SKU ID from slug (last segment after -)
    sku_match = re.search(r"-(\d+)$", slug)
    sku_id = int(sku_match.group(1)) if sku_match else None

    # Parse FAQs
    faqs_json = None
    if faq_schema:
        faqs = []
        for item in faq_schema.get("mainEntity", []):
            faqs.append({
                "question": item.get("name"),
                "answer": item.get("acceptedAnswer", {}).get("text"),
            })
        if faqs:
            faqs_json = json.dumps(faqs, ensure_ascii=False)

    return {
        "sku_id": sku_id,
        "name": drug_data.get("name"),
        "url": drug_data.get("url"),
        "slug": slug,
        "active_ingredient": drug_data.get("activeIngredient"),
        "drug_unit": drug_data.get("drugUnit"),
        "dosage_form": drug_data.get("dosageForm"),
        "administration_route": drug_data.get("administrationRoute"),
        "prescription_status": drug_data.get("prescriptionStatus"),
        "is_available_generically": 1 if drug_data.get("isAvailableGenerically") else 0,
        "is_proprietary": 1 if drug_data.get("isProprietary") else 0,
        "non_proprietary_name": drug_data.get("nonProprietaryName"),
        "description": drug_data.get("description"),
        "pregnancy_warning": drug_data.get("pregnancyWarning"),
        "alcohol_warning": drug_data.get("alcoholWarning"),
        "breastfeeding_warning": drug_data.get("breastfeedingWarning"),
        "food_warning": drug_data.get("foodWarning"),
        "mechanism_of_action": json.dumps(drug_data.get("mechanismOfAction", []), ensure_ascii=False) if drug_data.get("mechanismOfAction") else None,
        "available_strength": json.dumps(drug_data.get("availableStrength", []), ensure_ascii=False) if drug_data.get("availableStrength") else None,
        "dose_schedule": json.dumps(drug_data.get("doseSchedule"), ensure_ascii=False) if drug_data.get("doseSchedule") else None,
        "image": drug_data.get("image"),
        "brand_name": drug_data.get("brand", {}).get("name") if drug_data.get("brand") else None,
        "date_published": drug_data.get("datePublished"),
        "date_modified": drug_data.get("dateModified"),
        "faqs": faqs_json,
    }

# ───────────────────── Database Operations ───────────────────── #

def mark_page_done(conn, label, page, status="done"):
    conn.execute(
        "INSERT OR REPLACE INTO scrape_progress (label, listing_page, status, updated_at) "
        "VALUES (?, ?, ?, datetime('now'))",
        (label, page, status)
    )
    conn.commit()


def is_page_done(conn, label, page):
    row = conn.execute(
        "SELECT status FROM scrape_progress WHERE label=? AND listing_page=?",
        (label, page)
    ).fetchone()
    return row and row[0] == "done"


def mark_label_listing_done(conn, label):
    """Mark all listing pages for a label as completed."""
    conn.execute(
        "UPDATE scrape_progress SET status='listing_done', updated_at=datetime('now') "
        "WHERE label=? AND status='done'",
        (label,)
    )
    conn.commit()


def record_failed_slug(conn, slug, label, error):
    conn.execute(
        "INSERT OR REPLACE INTO failed_slugs (slug, label, error, retries, created_at) "
        "VALUES (?, ?, ?, COALESCE((SELECT retries+1 FROM failed_slugs WHERE slug=?), 1), datetime('now'))",
        (slug, label, str(error), slug)
    )
    conn.commit()


def upsert_medicine(conn, med):
    conn.execute("""
        INSERT OR REPLACE INTO medicines (
            sku_id, name, url, slug, active_ingredient, drug_unit, dosage_form,
            administration_route, prescription_status, is_available_generically,
            is_proprietary, non_proprietary_name, description, pregnancy_warning,
            alcohol_warning, breastfeeding_warning, food_warning, mechanism_of_action,
            available_strength, dose_schedule, image, brand_name, date_published,
            date_modified, faqs, scraped_at
        ) VALUES (
            :sku_id, :name, :url, :slug, :active_ingredient, :drug_unit, :dosage_form,
            :administration_route, :prescription_status, :is_available_generically,
            :is_proprietary, :non_proprietary_name, :description, :pregnancy_warning,
            :alcohol_warning, :breastfeeding_warning, :food_warning, :mechanism_of_action,
            :available_strength, :dose_schedule, :image, :brand_name, :date_published,
            :date_modified, :faqs, datetime('now')
        )
    """, med)
    conn.commit()

# ───────────────────── Label Worker ───────────────────── #

def process_label(label):
    """Process a single label: fetch all listing pages, then all drug pages."""
    thread_name = f"label-{label}"
    t = threading.current_thread()
    t.name = thread_name

    conn = get_db_connection()
    listing_done_key = f"label_{label}_all_pages_listed"

    log.info(f"[{label}] Starting label processing")

    # ── Phase 1: Collect all slugs from listing pages ──
    all_slugs = []
    page = 1
    total_pages = 9999  # will update from first page result

    while page <= total_pages:
        if SHUTDOWN.is_set():
            log.info(f"[{label}] Shutdown requested, stopping")
            conn.close()
            return

        if is_page_done(conn, label, page):
            log.info(f"[{label}] Listing page {page} already done, skipping")
            # Still need to re-collect slugs for this page from DB? No.
            # We need to check if all drug slugs for this label are in DB.
            # For simplicity, we skip pages that are 'done'.
            page += 1
            continue

        result = get_listing_data(label, page)
        if not result:
            log.error(f"[{label}] Failed to fetch listing page {page}, retrying")
            # Retry up to 3 times, then skip this page permanently
            retry_success = False
            for retry_attempt in range(3):
                time.sleep(3 + retry_attempt * 2)
                result = get_listing_data(label, page)
                if result:
                    retry_success = True
                    break
                log.warning(f"[{label}] Listing page {page} retry {retry_attempt+1}/3 failed")
            if not retry_success:
                log.error(f"[{label}] SKIPPING listing page {page} after 3 retries")
                mark_page_done(conn, label, page, "failed")
                page += 1
                continue

        medicines, pagination = result
        total_pages = pagination.get("totalPages", 1)
        slugs_on_page = [m.get("slug") for m in medicines if m.get("slug")]
        all_slugs.extend(slugs_on_page)

        log.info(f"[{label}] Listing page {page}/{total_pages} | "
                 f"collected {len(slugs_on_page)} slugs | total: {len(all_slugs)}")

        mark_page_done(conn, label, page, "done")

        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    mark_label_listing_done(conn, label)
    log.info(f"[{label}] All listing pages done. Total slugs: {len(all_slugs)}")

    # ── Phase 2: Scrape each drug page ──
    drug_count = 0
    buffer = []

    for i, slug in enumerate(all_slugs):
        if SHUTDOWN.is_set():
            log.info(f"[{label}] Shutdown during drug scraping, saving buffer")
            break

        # Skip if already in DB
        existing = conn.execute(
            "SELECT 1 FROM medicines WHERE slug=?", (slug,)
        ).fetchone()
        if existing:
            drug_count += 1
            continue

        med = scrape_drug_page(slug)
        if med:
            buffer.append(med)
            drug_count += 1
        else:
            record_failed_slug(conn, slug, label, "Failed to scrape drug page")

        # Flush buffer periodically
        if len(buffer) >= DB_WRITE_INTERVAL:
            for m in buffer:
                try:
                    upsert_medicine(conn, m)
                except Exception as e:
                    log.error(f"[{label}] DB insert failed for {m.get('slug')}: {e}")
            buffer.clear()
            log.info(f"[{label}] Progress: {drug_count}/{len(all_slugs)} drugs scraped")

        time.sleep(REQUEST_DELAY)

    # Final flush
    for m in buffer:
        try:
            upsert_medicine(conn, m)
        except Exception as e:
            log.error(f"[{label}] DB insert failed for {m.get('slug')}: {e}")

    conn.close()
    log.info(f"[{label}] COMPLETED: {drug_count} drugs processed")


# ───────────────────── Signal Handler ───────────────────── #

def signal_handler(signum, frame):
    log.info("Received shutdown signal. Waiting for workers to finish...")
    SHUTDOWN.set()

# ───────────────────── Main ───────────────────── #

def print_stats(conn):
    count = conn.execute("SELECT COUNT(*) FROM medicines").fetchone()[0]
    progress = conn.execute(
        "SELECT COUNT(*) FROM scrape_progress WHERE status='done' OR status='listing_done'"
    ).fetchone()[0]
    failed = conn.execute("SELECT COUNT(*) FROM failed_slugs").fetchone()[0]
    log.info(f"STATS | Medicines: {count} | Listing pages done: {progress} | Failed slugs: {failed}")


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    init_db()
    conn = get_db_connection()
    print_stats(conn)
    conn.close()

    log.info(f"Starting scraper with {MAX_WORKERS} workers | Labels: a..z")

    start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_label, label): label for label in LABELS}

        for future in as_completed(futures):
            label = futures[future]
            try:
                future.result()
                log.info(f"Worker for label '{label}' finished successfully")
            except Exception as e:
                log.error(f"Worker for label '{label}' crashed: {e}", exc_info=True)

    elapsed = datetime.now() - start_time
    conn = get_db_connection()
    print_stats(conn)
    conn.close()

    log.info(f"DONE. Total time: {elapsed}")


if __name__ == "__main__":
    main()
