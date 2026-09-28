# ==============================================================================
# --- GLOBAL USER SETTINGS ---
#
# How many articles to get from each source (e.g., 5)
# This is a 'quota'. The script will keep scanning the feed until it saves
# this many NEW articles (or runs out of items).
MAX_ARTICLES_PER_SOURCE = 20

# Skip feed items published longer ago than this many days.
# Stops feeds like Economic Times from feeding you 2008-era articles.
MAX_ARTICLE_AGE_DAYS = 3

# Stop scanning a feed after this many consecutive already-seen/skipped items.
# Protects the 420s time budget when a feed is mostly old news.
MAX_CONSECUTIVE_SKIPS = 77

# Overall time budget (seconds) for the whole scrape job.
JOB_TIMEOUT_SECONDS = 420

# --- PROXY CONFIGURATION ---
# Set 'use_proxies' to True to route all requests (Requests & Selenium)
# through the 'proxy_url'.
#
# 'proxy_url' should be in the format: http://username:password@proxy.example.com:8080
PROXY_SETTINGS = {
    "use_proxies": False,
    "proxy_url": None  # e.g., "http://user:pass@proxy.service.com:8080"
}

# --- DATABASE CONFIGURATION ---
import os
import logging

def load_env():
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    os.environ[key.strip()] = val.strip()

load_env()

# Self-healing default local path for environments like GHA
default_db_path = '/Users/mac/Downloads/Code/Satya/satya.db'
if not os.path.exists(os.path.dirname(default_db_path)):
    default_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'satya.db')

DB_PATH = os.environ.get('SATYA_DB_PATH', default_db_path)

def get_db_connection():
    db_url = os.environ.get('SATYA_DB_URL')
    db_token = os.environ.get('SATYA_DB_TOKEN')

    if db_url and (db_url.startswith('libsql://') or db_url.startswith('https://')):
        try:
            import libsql
            return libsql.connect(database=db_url, auth_token=db_token)
        except ImportError:
            logging.error("libsql package not installed. Falling back to local sqlite3.")

    import sqlite3
    return sqlite3.connect(DB_PATH)
# ==============================================================================


import sqlite3
import zlib
# --- FATAL FIX: Prevent PyTorch CPU deadlocks in multithreaded environments ---
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"
# ------------------------------------------------------------------------------

import socket
# --- FATAL FIX: Prevent underlying requests from hanging indefinitely ---
socket.setdefaulttimeout(15)
# ------------------------------------------------------------------------------

import requests
import urllib3
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import trafilatura
import time
import json
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, wait
import sys
import random
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

# Suppress insecure request warnings for SSL bypass
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Imports for AI/Semantics ---
try:
    from sentence_transformers import SentenceTransformer, util
    import torch
except ImportError:
    logging.critical("sentence-transformers or torch not installed. Run 'pip install sentence-transformers'. AI clustering will be skipped.")
    SentenceTransformer = None
    util = None
    torch = None
# -------------------------------------

# --- Imports for Selenium ---
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.common.exceptions import WebDriverException, TimeoutException
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    logging.critical("Selenium not installed. Run 'pip install selenium'. Selenium-dependent sources will fail.")
    SELENIUM_AVAILABLE = False
# ---------------------------

# --- Configure logging ---
logging.basicConfig(filename='news_scraper.log',
                    filemode='w',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# Quiet the very chatty HTTP client loggers (HuggingFace / httpx spam in the log).
for _noisy in ("httpx", "httpcore", "urllib3", "huggingface_hub", "filelock"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

# --- Robust Session and Header Management ---

def create_robust_session():
    """Creates a requests.Session with automatic retries and disabled SSL verification."""
    logging.info("Creating new robust session with 3 retries on 5xx/connection/read errors.")
    session = requests.Session()

    # Disable SSL verification globally for this session to bypass 'Weak Key' errors
    session.verify = False

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"],
        connect=True,
        read=True,
    )
    # Pool sized for our thread count so threads don't block on connections.
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=20)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Headers and User-Agents
# 'br' (Brotli) intentionally NOT advertised: requests can't decode Brotli
# unless the 'brotli' package is installed, which produced the
# "parsed tree length: 1, wrong data type or not valid HTML" errors.
BASE_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
}

BROWSER_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
]
GOOGLEBOT_USER_AGENT = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
FEEDFETCHER_USER_AGENT = 'Mozilla/5.0 (compatible; FeedFetcher-Google; +http://www.google.com/feedfetcher.html)'

def get_headers(header_type):
    """Returns a complete header dictionary for a given "persona"."""
    headers = BASE_HEADERS.copy()
    core_type = header_type.replace('requests_', '')

    if core_type == 'browser':
        headers['User-Agent'] = random.choice(BROWSER_USER_AGENTS)
    elif core_type == 'googlebot':
        headers['User-Agent'] = GOOGLEBOT_USER_AGENT
    elif core_type == 'feedfetcher':
        headers = {'User-Agent': FEEDFETCHER_USER_AGENT}
    return headers

# Per-source page-load timeout for Selenium (seconds). Default is 7.
DEFAULT_PAGE_LOAD_TIMEOUT = 7

def create_selenium_driver(page_load_timeout=DEFAULT_PAGE_LOAD_TIMEOUT):
    """Initializes and returns a headless Selenium Chrome WebDriver."""
    if not SELENIUM_AVAILABLE:
        return None

    try:
        options = ChromeOptions()
        options.page_load_strategy = 'eager'
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={random.choice(BROWSER_USER_AGENTS)}")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--renderer-process-limit=1")

        # Don't download images/fonts. We only need article text, and this makes
        # heavy pages (The Hindu) load far faster, cutting renderer timeouts.
        # NOTE: stylesheets are left ENABLED on purpose - some sites render the
        # article body via JS/CSS and disabling CSS can hide the text.
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.fonts": 2,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--blink-settings=imagesEnabled=false")

        if PROXY_SETTINGS["use_proxies"] and PROXY_SETTINGS["proxy_url"]:
            options.add_argument(f"--proxy-server={PROXY_SETTINGS['proxy_url']}")

        chrome_bin = os.environ.get('CHROME_BIN')
        if chrome_bin:
            options.binary_location = chrome_bin

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(page_load_timeout)
        logging.info(f"Selenium driver initialized successfully (Eager load, {page_load_timeout}s timeout, images off).")
        return driver
    except WebDriverException as e:
        logging.critical(f"Failed to initialize Selenium driver. Error: {e}")
        return None
    except Exception as e:
        logging.critical(f"An unexpected error occurred during Selenium initialization: {e}")
        return None

def safe_quit_driver(driver, name="?"):
    """Quits a driver, force-killing the process if quit() fails."""
    if not driver:
        return
    pid_to_kill = None
    try:
        pid_to_kill = driver.service.process.pid
    except Exception:
        pass
    try:
        driver.quit()
        logging.info(f"[{name}] driver.quit() successful.")
    except Exception as e:
        logging.warning(f"[{name}] driver.quit() failed: {e}. Attempting surgical kill.")
        if pid_to_kill:
            try:
                os.kill(pid_to_kill, 9)
                logging.info(f"[{name}] Killed stuck driver process PID {pid_to_kill}.")
            except Exception as e_kill:
                logging.error(f"[{name}] Failed to kill PID {pid_to_kill}: {e_kill}")

# --- Central Source Configuration ---
# Optional per-source keys:
#   'max_articles'        : quota of NEW articles to save
#   'page_load_timeout'   : Selenium page-load timeout in seconds (default 7)
#   'skip_url_contains'   : list of substrings; matching article URLs are skipped
SOURCE_CONFIG = [
    {
        'name': 'BBC',
        'rss_url': 'http://feeds.bbci.co.uk/news/world/rss.xml',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['requests_browser', 'selenium_browser'],
        'article_url_contains': None,
        'max_articles': 10,
        'referer': 'https://www.bbc.com/news',
        'skip_url_contains': ['/videos/'],
    },
    {
        'name': 'Times of India',
        'rss_url': 'https://timesofindia.indiatimes.com/rssfeeds/296589292.cms',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['selenium_browser'],
        'article_url_contains': '.cms',
        'referer': 'https://timesofindia.indiatimes.com/',
    },
    {
        # requests first (The Hindu's paywall is client-side, so text is often in
        # the raw HTML), Selenium with a longer timeout as fallback.
        'name': 'The Hindu',
        'rss_url': 'https://www.thehindu.com/news/national/feeder/default.rss',
        'rss_headers_type': 'browser',
        'article_strategies': ['requests_browser', 'selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://www.thehindu.com/',
        'page_load_timeout': 15,
        'skip_url_contains': ['/videos/'],
    },
    {
        # Video pages never have 90+ words, so skip them up front.
        'name': 'Al Jazeera',
        'rss_url': 'https://www.aljazeera.com/xml/rss/all.xml',
        'rss_headers_type': 'browser',
        'article_strategies': ['requests_browser', 'selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://www.aljazeera.com/',
        'max_articles': 8,
        'skip_url_contains': ['/video/'],
    },
    {
        'name': 'TechCrunch',
        'rss_url': 'https://techcrunch.com/feed/',
        'rss_headers_type': 'browser',
        'article_strategies': ['requests_browser'],
        'article_url_contains': None,
        'referer': 'https://techcrunch.com/',
        'max_articles': 8
    },
    {
        'name': 'Economic Times',
        'rss_url': 'https://economictimes.indiatimes.com/rssfeedsdefault.cms',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['requests_browser', 'selenium_browser'],
        'article_url_contains': '.cms',
        'referer': 'https://economictimes.indiatimes.com/'
    },
    {
        # requests_browser always failed for Wired (bad body) and cost ~4s per
        # article. Selenium-only now.
        'name': 'Wired',
        'rss_url': 'https://www.wired.com/feed/rss',
        'rss_headers_type': 'browser',
        'article_strategies': ['selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://www.wired.com/',
        'max_articles': 10,
        # Coupon/promo/deal spam pages that keep appearing in the feed.
        'skip_url_contains': ['promo-code', 'coupon', 'discount-code', '/gallery/best-', 'prime-day'],
    },
    {
        # requests_browser always returned 403 for NDTV. Selenium-only.
        'name': 'NDTV',
        'rss_url': 'https://feeds.feedburner.com/ndtvnews-top-stories',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://www.ndtv.com/',
        'max_articles': 10
    },
    {
        # feedfetcher UA got a 403 on the RSS feed. Use a browser UA.
        'name': 'Indian Express',
        'rss_url': 'https://indianexpress.com/feed/',
        'rss_headers_type': 'browser',
        'article_strategies': ['selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://indianexpress.com/',
        'max_articles': 10
    },
    # --- Disabled sources kept from the original config ---
    # {'name': 'The Guardian', 'rss_url': 'https://www.theguardian.com/world/rss', ...},
    # {'name': 'The Dawn', 'rss_url': 'https://www.dawn.com/feeds/home', ...},
    # {'name': 'Livemint', 'rss_url': 'https://www.livemint.com/rss/news', ...},
    # {'name': 'India Today', 'rss_url': 'https://www.indiatoday.in/rss/home', ...},
]

# ==============================================================================
# AI MODEL INITIALIZATION
# ==============================================================================
semantic_model = None
if SentenceTransformer is not None:
    try:
        logging.info("Loading AI Semantic Model (all-MiniLM-L6-v2)...")
        semantic_model = SentenceTransformer('all-MiniLM-L6-v2')
        logging.info("AI Model loaded successfully.")
    except Exception as e:
        logging.critical(f"Failed to load AI model: {e}. Clustering is disabled.")
        semantic_model = None


# ==============================================================================
# DB SETUP & CACHING
# ==============================================================================
db_lock = threading.Lock()    # Locks access to DB writes / ID counter / URL cache
ai_lock = threading.Lock()    # Locks access to PyTorch execution to prevent deadlocks
existing_urls_cache = set()   # Memory cache for fast deduplication
recent_articles_cache = []    # Memory cache for AI clustering with pre-computed embeddings
MAX_ID = 0                    # Global counter for sequential IDs
saved_counts = {}             # Live per-source saved counts (guarded by db_lock)


def normalize_url(url):
    """Normalizes a URL so the same article isn't seen as two different URLs.
    Strips the fragment (#publisher=newsstand etc.) and tracking query params."""
    if not url:
        return url
    parts = urlsplit(url.strip())
    tracking = {'at_medium', 'at_campaign', 'traffic_source', 'utm_source', 'utm_medium',
                'utm_campaign', 'utm_term', 'utm_content', 'gclid', 'fbclid', 'ref', 'ocid'}
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in tracking]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ''))


def url_seen(url):
    """Thread-safe check: has this URL (raw or normalized) already been seen?"""
    norm = normalize_url(url)
    with db_lock:
        return url in existing_urls_cache or norm in existing_urls_cache


def init_google_sheets():
    """Loads the DB into memory caches. (Name kept for compatibility; no Google Sheets involved.)"""
    global existing_urls_cache, recent_articles_cache, MAX_ID

    try:
        logging.info("Connecting to database...")
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Update Max ID
        cursor.execute("SELECT MAX(id) FROM articles")
        max_id_val = cursor.fetchone()[0]
        if max_id_val:
            MAX_ID = max_id_val

        # 2. Populate URL cache with ALL urls (not just the last 14 days).
        # Old articles that a feed keeps re-serving were in the DB but not in
        # the cache, so every run re-fetched them and then hit UNIQUE(url).
        cursor.execute("SELECT url FROM articles")
        for r in cursor.fetchall():
            if r[0]:
                existing_urls_cache.add(r[0])
                existing_urls_cache.add(normalize_url(r[0]))

        # 3. Queue for AI Cache (Only last 24h)
        cutoff_timestamp = int(time.time()) - 24 * 3600
        cursor.execute("""
            SELECT title, content, cluster_id FROM articles
            WHERE scraped_at >= ?
        """, (cutoff_timestamp,))
        rows = cursor.fetchall()

        temp_recent_texts = []
        temp_recent_items = []

        for r in rows:
            title = r[0]
            compressed_content = r[1]
            cluster_id = r[2]

            try:
                content = zlib.decompress(compressed_content).decode('utf-8')
            except Exception:
                content = ""

            temp_recent_items.append({'title': title, 'content': content, 'cluster_id': cluster_id})
            temp_recent_texts.append(f"{title}. {content[:700]}")

        conn.close()

        if temp_recent_texts and semantic_model is not None:
            logging.info(f"Pre-calculating embeddings for {len(temp_recent_texts)} cached articles...")
            try:
                embeddings = semantic_model.encode(temp_recent_texts, convert_to_tensor=True)
                for i, item in enumerate(temp_recent_items):
                    item['embedding'] = embeddings[i]
                    recent_articles_cache.append(item)
            except Exception as e_embed:
                logging.error(f"Failed to batch-encode startup cache: {e_embed}")
                for item in temp_recent_items:
                    recent_articles_cache.append(item)

        logging.info(f"Cache built: {len(existing_urls_cache)} URLs. Current MAX_ID: {MAX_ID}")

    except Exception as e:
        logging.critical(f"Failed to initialize database: {e}")
        sys.exit(1)


# ==============================================================================
# AI DEDUPLICATION LOGIC
# ==============================================================================

def get_cluster_id_for_article(new_title, new_summary):
    """Checks cache for similar articles and assigns a cluster_id."""
    if semantic_model is None or util is None or torch is None:
        return str(uuid.uuid4()), None

    try:
        cache_list = list(recent_articles_cache)
        valid_items = [a for a in cache_list if 'embedding' in a]

        new_text = f"{new_title}. {new_summary[:700]}"
        new_embedding = semantic_model.encode(new_text, convert_to_tensor=True)

        if not valid_items:
            return str(uuid.uuid4()), new_embedding

        existing_embeddings = torch.stack([a['embedding'] for a in valid_items])
        existing_ids = [a['cluster_id'] for a in valid_items]

        cosine_scores = util.cos_sim(new_embedding, existing_embeddings)[0]

        best_score = -1
        best_idx = -1
        for i, score in enumerate(cosine_scores):
            if score > best_score:
                best_score = score.item()
                best_idx = i

        THRESHOLD = 0.82

        if best_score >= THRESHOLD:
            logging.info(f"DEDUPLICATION: Found match (Score: {best_score:.2f}). Linking to Cluster ID: {existing_ids[best_idx]}")
            return existing_ids[best_idx], new_embedding
        else:
            return str(uuid.uuid4()), new_embedding

    except Exception as e:
        logging.error(f"Error during AI clustering calculation: {e}")
        return str(uuid.uuid4()), None


def is_duplicate_error(exc):
    """True if an exception from either sqlite3 or libsql/Hrana is a UNIQUE violation."""
    if isinstance(exc, sqlite3.IntegrityError):
        return True
    msg = str(exc)
    return "UNIQUE constraint" in msg or "SQLITE_CONSTRAINT" in msg


def save_article(source, title, url, summary, image_url):
    """
    Saves a single article. Returns True if saved.
    Checks are ordered cheapest-first: word count -> URL cache -> AI clustering -> DB insert.
    """
    global existing_urls_cache, recent_articles_cache, MAX_ID

    # --- STEP 0: STRICT GLOBAL WORD COUNT CHECK ---
    if not summary:
        final_word_count = 0
        cleaned_summary = ""
    else:
        cleaned_summary = " ".join(summary.replace('\n', ' ').replace('\r', ' ').split()).strip()
        final_word_count = len(cleaned_summary.split())

    MIN_SUMMARY_WORDS = 90
    if final_word_count < MIN_SUMMARY_WORDS:
        logging.warning(f"SKIPPED (GLOBAL WORD LIMIT): Article '{title}' from {source} has only {final_word_count} words (Min: {MIN_SUMMARY_WORDS}).")
        return False

    try:
        title = " ".join(title.replace('\n', ' ').replace('\r', ' ').split()).strip()
        summary = cleaned_summary

        if not image_url:
            image_url = "No image available"

        norm_url = normalize_url(url)

        # Duplicate check BEFORE the (slow, locked) AI embedding pass.
        if url_seen(url):
            logging.info(f"Duplicate article skipped: {title} from {source}")
            return False

        # --- AI PASS: embedding + cluster id under thread-safe lock ---
        with ai_lock:
            cluster_id, new_embedding = get_cluster_id_for_article(title, summary)

        # --- THREAD-SAFE DB WRITE BLOCK ---
        with db_lock:
            # Re-check: another thread may have saved the same URL meanwhile.
            if url in existing_urls_cache or norm_url in existing_urls_cache:
                return False

            new_id = MAX_ID + 1  # only commit the ID bump if the insert succeeds

            conn = get_db_connection()
            try:
                cursor = conn.cursor()

                cursor.execute("INSERT OR IGNORE INTO sources (name) VALUES (?)", (source,))
                cursor.execute("SELECT id FROM sources WHERE name = ?", (source,))
                source_id = cursor.fetchone()[0]

                compressed_content = zlib.compress(summary.encode('utf-8'))
                scraped_timestamp = int(time.time())

                cursor.execute("""
                    INSERT INTO articles (id, cluster_id, source_id, title, url, content, image_url, scraped_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'scraped')
                """, (new_id, cluster_id, source_id, title, url, compressed_content, image_url, scraped_timestamp))
                conn.commit()
            except Exception as e:
                # Works for both sqlite3 and libsql (Hrana) errors; remembers the
                # URL so it is never fetched again this run; does NOT burn an ID.
                if is_duplicate_error(e):
                    existing_urls_cache.add(url)
                    existing_urls_cache.add(norm_url)
                    logging.info(f"Duplicate URL already in DB, cached: {url}")
                    return False
                raise
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

            MAX_ID = new_id
            existing_urls_cache.add(url)
            existing_urls_cache.add(norm_url)
            cache_entry = {'title': title, 'content': summary, 'cluster_id': cluster_id}
            if new_embedding is not None:
                cache_entry['embedding'] = new_embedding
            recent_articles_cache.append(cache_entry)
            saved_counts[source] = saved_counts.get(source, 0) + 1

        logging.info(f">>> SUCCESSFULLY SAVED [ID: {new_id}] - {title} from {source} ({final_word_count} words)")
        print(f"Saved: {title} [ID: {new_id}]")
        return True

    except Exception as e:
        logging.error(f"Error saving article {title}: {e}")
        return False


def clean_title(page_title, source):
    """Removes a trailing ' | The Hindu' / ' - The Times of India' style site-name
    suffix from <title>. Only strips when the tail looks like a site name
    (short, and ideally contains the source name) so real headlines with a
    dash in them are left alone."""
    if not page_title:
        return page_title
    t = " ".join(page_title.split())
    source_l = (source or "").lower()
    for sep in (' | ', ' - ', ' – ', ' — '):
        if sep in t:
            head, tail = t.rsplit(sep, 1)
            tail_l = tail.lower()
            looks_like_site = (
                len(tail) <= 40 and len(head) >= 15 and (
                    sep == ' | '                      # pipes are almost always site names
                    or source_l in tail_l             # tail mentions the source
                    or tail_l in source_l
                    or tail_l.endswith(('news', 'times', 'hindu', 'express', 'wired', 'jazeera', 'ndtv'))
                )
            )
            if looks_like_site:
                return head.strip()
    return t


def is_too_old(item):
    """True if the RSS item has a pubDate older than MAX_ARTICLE_AGE_DAYS."""
    try:
        pub = item.pubDate.text if item.pubDate else None
        if not pub:
            return False
        dt = parsedate_to_datetime(pub.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt) > timedelta(days=MAX_ARTICLE_AGE_DAYS)
    except Exception:
        return False  # can't parse -> don't discard


# --- Generic Scraper Function with Dynamic Quota Handling ---
def scrape_source(session, selenium_driver_holder, source_config, proxies_dict, deadline):
    """
    Scrapes one source. `selenium_driver_holder` is a dict {'driver': <driver or None>}
    so the driver can be rebuilt in place if it gets wedged by a renderer timeout.
    `deadline` is an absolute time.time() value after which the source stops
    cleanly (so its saved count is reported instead of being lost to a timeout).
    """
    name = source_config['name']
    rss_url = source_config['rss_url']
    page_timeout = source_config.get('page_load_timeout', DEFAULT_PAGE_LOAD_TIMEOUT)
    skip_contains = source_config.get('skip_url_contains') or []

    articles_saved = 0
    consecutive_skips = 0

    logging.info(f"Starting scrape for {name} RSS feed: {rss_url}")

    try:
        # 1. Get RSS Feed
        rss_headers = get_headers(source_config['rss_headers_type'])
        response = session.get(rss_url, headers=rss_headers, timeout=10, proxies=proxies_dict)
        if response.status_code != 200:
            # Log status + body start so empty/blocked feeds are diagnosable.
            logging.error(f"[{name}] RSS returned HTTP {response.status_code}. Body starts: {response.text[:200]!r}")
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'xml')
        items = soup.find_all('item')
        if not items:
            logging.error(f"[{name}] RSS parsed but 0 <item> tags. Content-Type: {response.headers.get('Content-Type')}. Body starts: {response.text[:200]!r}")

        max_quota = source_config.get('max_articles', MAX_ARTICLES_PER_SOURCE)
        logging.info(f"Found {len(items)} articles in {name} RSS feed. Processing until {max_quota} new articles are saved.")

        # 2. Process each article
        for item in items:

            if articles_saved >= max_quota:
                logging.info(f"[{name}] Target reached: Successfully saved {max_quota} new articles.")
                break

            if time.time() >= deadline:
                logging.warning(f"[{name}] Job deadline reached. Stopping cleanly with {articles_saved} saved.")
                break

            # Bail out of feeds that are mostly already-seen / stale items.
            if consecutive_skips >= MAX_CONSECUTIVE_SKIPS:
                logging.info(f"[{name}] {consecutive_skips} consecutive skips. Stopping this feed early.")
                break

            article_url = None
            rss_title = "Title not found"
            rss_description = None

            try:
                if not item.link:
                    continue

                article_url = item.link.text.strip()

                if source_config['article_url_contains'] and source_config['article_url_contains'] not in article_url:
                    logging.warning(f"[{name}] Skipping non-article link: {article_url}")
                    continue

                # Per-source URL skip list (videos, coupon spam)
                if any(s in article_url for s in skip_contains):
                    logging.info(f"[{name}] Skipping filtered URL: {article_url}")
                    consecutive_skips += 1
                    continue

                # Skip stale items (Economic Times serves 2008-era articles)
                if is_too_old(item):
                    logging.info(f"[{name}] Skipping stale item (> {MAX_ARTICLE_AGE_DAYS}d old): {article_url}")
                    consecutive_skips += 1
                    continue

                # --- Early Skip Check (raw + normalized, thread-safe) ---
                if url_seen(article_url):
                    logging.info(f"[{name}] Early skip: URL {article_url} already exists.")
                    consecutive_skips += 1
                    continue

                consecutive_skips = 0  # a genuinely new item resets the streak

                rss_title = item.title.text if item.title else "Title not found"

                if item.description:
                    summary_soup = BeautifulSoup(item.description.text, 'html.parser')
                    rss_description = summary_soup.get_text().strip()

                # --- MULTI-STRATEGY LOGIC ---
                summary = None
                raw_html = None
                final_title = rss_title
                image_url = "No image available"

                strategies = source_config['article_strategies']

                for i, strategy in enumerate(strategies):
                    logging.info(f"[{name}] Article: {article_url}")
                    logging.info(f"[{name}] Attempt {i+1}/{len(strategies)}: Trying with '{strategy}' strategy...")
                    raw_html = None

                    try:
                        if strategy.startswith('requests_'):
                            header_type = strategy.replace('requests_', '')
                            article_headers = get_headers(header_type)
                            article_headers['Referer'] = source_config['referer']

                            page_response = session.get(article_url, headers=article_headers, timeout=10, proxies=proxies_dict)
                            page_response.raise_for_status()
                            raw_html = page_response.text

                        elif strategy == 'selenium_browser':
                            driver = selenium_driver_holder.get('driver')
                            if not driver:
                                logging.error(f"[{name}] Selenium strategy selected but driver is not available. Skipping.")
                                continue

                            try:
                                try:
                                    driver.get("about:blank")
                                except Exception:
                                    pass
                                driver.get(article_url)
                                resolved_url = driver.current_url
                                if resolved_url and "news.google.com" not in resolved_url:
                                    article_url = resolved_url
                            except TimeoutException:
                                logging.warning(f"[{name}] Page get timed out ({page_timeout}s). Proceeding to grab partial source anyway.")

                            try:
                                WebDriverWait(driver, 3).until(
                                    EC.presence_of_element_located((By.TAG_NAME, "p"))
                                )
                                logging.info(f"[{name}] Page content loaded.")
                            except TimeoutException:
                                logging.warning(f"[{name}] Page explicit wait timed out (3s). Proceeding anyway.")

                            raw_html = driver.page_source

                        else:
                            logging.error(f"[{name}] Unknown strategy: {strategy}. Skipping.")
                            continue

                        if not raw_html:
                            logging.warning(f"[{name}] FAILED with '{strategy}' (HTML was empty).")
                            continue

                        temp_summary = trafilatura.extract(raw_html, include_comments=False, include_tables=False)
                        word_count = len(temp_summary.split()) if temp_summary else 0

                        if word_count >= 90:
                            logging.info(f"[{name}] Success with '{strategy}'. Found content ({word_count} words).")
                            summary = temp_summary

                            page_soup = BeautifulSoup(raw_html, 'html.parser')
                            # Prefer og:title, then <title> with the site-name suffix stripped.
                            og_title = page_soup.find('meta', property='og:title')
                            if og_title and og_title.get('content'):
                                final_title = og_title['content']
                            else:
                                page_title = page_soup.find('title')
                                if page_title:
                                    final_title = clean_title(page_title.text, name)

                            og_image = page_soup.find('meta', property='og:image')
                            if og_image and og_image.get('content'):
                                image_url = og_image['content']

                            break
                        else:
                            logging.warning(f"[{name}] FAILED with '{strategy}' (content was too short: {word_count} words).")

                    except Exception as e:
                        logging.error(f"[{name}] Request failed for strategy '{strategy}' on URL {article_url}: {e}")

                        # A renderer timeout leaves Chrome wedged; rebuild the
                        # driver so the NEXT article doesn't inherit the hang.
                        if strategy == 'selenium_browser' and 'timeout' in str(e).lower():
                            logging.warning(f"[{name}] Rebuilding Selenium driver after renderer timeout.")
                            safe_quit_driver(selenium_driver_holder.get('driver'), name)
                            selenium_driver_holder['driver'] = create_selenium_driver(page_timeout)

                    if i < len(strategies) - 1:
                        time.sleep(random.uniform(0.5, 1.0))

                if not summary:
                    logging.error(f"[{name}] All scrape strategies failed for {article_url}. Falling back to RSS description.")
                    summary = rss_description or "No content available"

                if save_article(name, final_title, article_url, summary, image_url):
                    articles_saved += 1

                time.sleep(random.uniform(0.5, 1.5))

            except Exception as e:
                logging.error(f"[{name}] Article-level Error: {e} for url {article_url}")

    except requests.RequestException as e:
        logging.error(f"Failed to fetch {name} RSS feed: {e}")
    except Exception as e:
        logging.error(f"Failed to parse {name} RSS feed: {e}")

    return (name, articles_saved)


# --- Thread Wrapper Function ---
def scrape_source_wrapper(source, session, proxies_dict, deadline):
    """Runs in its own thread; creates and destroys its own Selenium driver if needed."""
    name = source.get('name', 'Unknown')
    holder = {'driver': None}

    needs_selenium = any('selenium' in s for s in source.get('article_strategies', []))

    try:
        if needs_selenium and SELENIUM_AVAILABLE:
            logging.info(f"[{name}] (Thread) requires Selenium. Initializing driver...")
            holder['driver'] = create_selenium_driver(source.get('page_load_timeout', DEFAULT_PAGE_LOAD_TIMEOUT))
            if not holder['driver']:
                logging.error(f"[{name}] (Thread) Selenium driver failed to start. Selenium strategies will fail.")

        return scrape_source(session, holder, source, proxies_dict, deadline)

    except Exception as e:
        logging.critical(f"--- CRITICAL: (Thread) Scrape job for {name} failed entirely. --- {e}")
        return (name, 0)

    finally:
        if holder['driver']:
            logging.info(f"[{name}] (Thread) Finished. Shutting down its Selenium driver.")
            safe_quit_driver(holder['driver'], name)


# --- scrape_all() ---
def scrape_all():
    """Runs all scraping jobs defined in SOURCE_CONFIG in parallel."""
    logging.info("--- Starting new scraping job (Parallel Mode) ---")

    init_google_sheets()

    session = create_robust_session()

    proxies_dict = None
    if PROXY_SETTINGS["use_proxies"] and PROXY_SETTINGS["proxy_url"]:
        logging.info(f"Proxy is ENABLED. Routing requests through: {PROXY_SETTINGS['proxy_url']}")
        proxies_dict = {"http": PROXY_SETTINGS["proxy_url"], "https": PROXY_SETTINGS["proxy_url"]}
    else:
        logging.info("Proxy is DISABLED.")

    futures = []
    executor = ThreadPoolExecutor(max_workers=len(SOURCE_CONFIG))
    timed_out = 0

    # Sources get a soft deadline slightly BEFORE the hard wait() timeout, so they
    # stop on their own, close their Chrome drivers, and report their counts.
    start = time.time()
    soft_deadline = start + JOB_TIMEOUT_SECONDS - 20

    try:
        for source in SOURCE_CONFIG:
            futures.append(executor.submit(scrape_source_wrapper, source, session, proxies_dict, soft_deadline))

        logging.info(f"Submitted {len(futures)} jobs to thread pool. Waiting up to {JOB_TIMEOUT_SECONDS}s for completion...")

        done, not_done = wait(futures, timeout=JOB_TIMEOUT_SECONDS)

        for future in done:
            try:
                future.result()
            except Exception as e:
                logging.error(f"A future job resulted in an error: {e}")

        if not_done:
            timed_out = len(not_done)
            logging.critical(f"--- TIMEOUT: {timed_out} scrape jobs did not complete in {JOB_TIMEOUT_SECONDS}s. ---")
            for _ in not_done:
                logging.error("A thread has timed out and will be abandoned.")

    except Exception as e:
        logging.critical(f"--- CRITICAL: The entire scrape_all job failed. --- {e}")

    finally:
        logging.info("Shutting down thread pool (wait=False)...")
        executor.shutdown(wait=False)

        # Summary is built from the live counter, so sources that were still
        # running at the timeout are still counted correctly.
        with db_lock:
            counts = dict(saved_counts)
        total_saved = sum(counts.values())
        log_summary = ", ".join(f"{c} {n}" for n, c in sorted(counts.items()))
        if timed_out:
            log_summary += f", {timed_out} Timed_Out_Jobs"
        log_message = f"Scraped: {log_summary} articles. (Total saved: {total_saved}) in {time.time() - start:.0f}s"

        logging.info(log_message)
        print(log_message)
        logging.info("--- Scraping job finished ---")


# --- main() function with cleanup ---
def main():
    """Runs the scraper once (CI mode)."""
    try:
        logging.info("--- Scraper service started (CI Mode: Run Once) ---")
        print("Running single scrape for CI...")
        scrape_all()
        print("Scrape finished.")
    except Exception as e:
        logging.critical(f"A critical error occurred in the main function: {e}")
    finally:
        logging.info("--- Scraper service stopped. ---")
        print("Scraper stopped.")
        logging.info("--- Main thread finished. Forcing process exit to kill zombie threads. ---")
        os._exit(0)

if __name__ == '__main__':
    main()
