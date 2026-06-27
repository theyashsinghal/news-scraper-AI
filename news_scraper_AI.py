# ==============================================================================
# --- GLOBAL USER SETTINGS ---
#
# How many articles to get from each source (e.g., 5)
# This is a 'quota'. The script will keep scanning the feed until it saves
# this many NEW articles (or runs out of items).
MAX_ARTICLES_PER_SOURCE = 20
#
# --- NEW: PROXY CONFIGURATION ---
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
# --- FATAL FIX: Prevent underlying gspread/requests from hanging indefinitely ---
socket.setdefaulttimeout(15)
# ------------------------------------------------------------------------------

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import trafilatura
import time
import logging
import json
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, wait
import sys
import random
from datetime import datetime, timedelta

# --- Google Sheets Imports ---
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Suppress insecure request warnings for SSL bypass
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- NEW: Imports for AI/Semantics ---
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
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Headers and User-Agents
BASE_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
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

def create_selenium_driver():
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

        if PROXY_SETTINGS["use_proxies"] and PROXY_SETTINGS["proxy_url"]:
            options.add_argument(f"--proxy-server={PROXY_SETTINGS['proxy_url']}")

        chrome_bin = os.environ.get('CHROME_BIN')
        if chrome_bin:
            options.binary_location = chrome_bin

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(7) 
        logging.info("Selenium driver initialized successfully (Eager load strategy, 7s timeout).")
        return driver
    except WebDriverException as e:
        logging.critical(f"Failed to initialize Selenium driver. Error: {e}")
        return None
    except Exception as e:
        logging.critical(f"An unexpected error occurred during Selenium initialization: {e}")
        return None

# --- Central Source Configuration ---
# 17 Indian/Tech feeds added with dynamic quotas (max 10 articles each).
# Existing feeds modified: The Dawn, Al Jazeera, and The Guardian restricted to max 2.
SOURCE_CONFIG = [
    # --- Existing Core Feeds ---
    {
        'name': 'BBC',
        'rss_url': 'http://feeds.bbci.co.uk/news/world/rss.xml',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['requests_browser', 'selenium_browser'],
        'article_url_contains': None,
        'max_articles': 10,
        'referer': 'https://www.bbc.com/news',
    },
    {
        'name': 'Times of India',
        'rss_url': 'https://timesofindia.indiatimes.com/rssfeeds/296589292.cms',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['selenium_browser'],
        'article_url_contains': '.cms',
        'referer': 'https://timesofindia.indiatimes.com/',
    },
    # {
    #     'name': 'The Guardian',
    #     'rss_url': 'https://www.theguardian.com/world/rss',
    #     'rss_headers_type': 'feedfetcher',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://www.theguardian.com/',
    #     'max_articles': 2  # Restricted quota
    # },
    {
        'name': 'The Hindu',
        'rss_url': 'https://www.thehindu.com/news/national/feeder/default.rss',
        'rss_headers_type': 'browser',
        'article_strategies': ['selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://www.thehindu.com/',
    },
    # {
    #     'name': 'The Dawn',
    #     'rss_url': 'https://www.dawn.com/feeds/home',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser', 'selenium_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://www.dawn.com/',
    #     'max_articles': 1  # Restricted quota
    # },
    {
        'name': 'Al Jazeera',
        'rss_url': 'https://www.aljazeera.com/xml/rss/all.xml',
        'rss_headers_type': 'browser',
        'article_strategies': ['requests_browser', 'selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://www.aljazeera.com/',
        'max_articles': 8  # Restricted quota
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

    # --- NEW ADDITIONS (10 Articles Quota Each) ---
    {
        'name': 'Wired',
        'rss_url': 'https://www.wired.com/feed/rss',
        'rss_headers_type': 'browser',
        'article_strategies': ['requests_browser', 'selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://www.wired.com/',
        'max_articles': 10
    },
    {
        'name': 'NDTV',
        'rss_url': 'https://feeds.feedburner.com/ndtvnews-top-stories',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['requests_browser', 'selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://www.ndtv.com/',
        'max_articles': 10
    },
    {
        'name': 'Indian Express',
        'rss_url': 'https://indianexpress.com/feed/',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['selenium_browser'],
        'article_url_contains': None,
        'referer': 'https://indianexpress.com/',
        'max_articles': 10
    },
    # {
    #     'name': 'Deccan Herald',
    #     'rss_url': 'https://news.google.com/rss/search?q=site:deccanherald.com',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['selenium_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://news.google.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'The Tribune',
    #     'rss_url': 'https://news.google.com/rss/search?q=site:tribuneindia.com',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['selenium_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://news.google.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'The Telegraph',
    #     'rss_url': 'https://news.google.com/rss/search?q=site:telegraphindia.com',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['selenium_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://news.google.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'Onmanorama',
    #     'rss_url': 'https://www.onmanorama.com/news/india.feeds.onmrss.xml',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://www.onmanorama.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'EastMojo',
    #     'rss_url': 'https://www.eastmojo.com/feed/',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://www.eastmojo.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'The Assam Tribune',
    #     'rss_url': 'https://assamtribune.com/feed/',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://assamtribune.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'Odisha Bytes',
    #     'rss_url': 'https://odishabytes.com/feed/',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://odishabytes.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'The South First',
    #     'rss_url': 'https://thesouthfirst.com/feed/',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://thesouthfirst.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'Telangana Today',
    #     'rss_url': 'https://telanganatoday.com/feed',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://telanganatoday.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'Greater Kashmir',
    #     'rss_url': 'https://www.greaterkashmir.com/feed/',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://www.greaterkashmir.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'DT Next',
    #     'rss_url': 'https://www.dtnext.in/feed/',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://www.dtnext.in/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'Livemint',
    #     'rss_url': 'https://www.livemint.com/rss/news',
    #     'rss_headers_type': 'feedfetcher',
    #     'article_strategies': ['selenium_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://www.livemint.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'Firstpost',
    #     'rss_url': 'https://www.firstpost.com/commonfeeds/v1/mfp/rss/india.xml',
    #     'rss_headers_type': 'browser',
    #     'article_strategies': ['requests_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://www.firstpost.com/',
    #     'max_articles': 10
    # },
    # {
    #     'name': 'India Today',
    #     'rss_url': 'https://www.indiatoday.in/rss/home',
    #     'rss_headers_type': 'feedfetcher',
    #     'article_strategies': ['selenium_browser'],
    #     'article_url_contains': None,
    #     'referer': 'https://www.indiatoday.in/',
    #     'max_articles': 10
    # }
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
# GOOGLE SHEETS SETUP & CACHING
# ==============================================================================
sheet = None
sheet_lock = threading.Lock() # Locks access to the Google Sheet upload
ai_lock = threading.Lock()    # Locks access to PyTorch execution to prevent deadlocks
existing_urls_cache = set()   # Memory cache for fast deduplication
recent_articles_cache = []    # Memory cache for AI clustering with pre-computed embeddings
MAX_ID = 0                    # Global counter for sequential IDs

def init_google_sheets():
    """Initializes connection to the SQLite database and loads data to warm up caches instantly."""
    global existing_urls_cache, recent_articles_cache, MAX_ID
    
    try:
        logging.info("Connecting to SQLite database...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Update Max ID
        cursor.execute("SELECT MAX(id) FROM articles")
        max_id_val = cursor.fetchone()[0]
        if max_id_val:
            MAX_ID = max_id_val
            
        # 2. Populate URL cache (only last 14 days for memory deduplication)
        cutoff_url = int(time.time()) - 14 * 24 * 3600
        cursor.execute("SELECT url FROM articles WHERE scraped_at >= ?", (cutoff_url,))
        urls = cursor.fetchall()
        for r in urls:
            if r[0]:
                existing_urls_cache.add(r[0])
                
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
                
            temp_recent_items.append({
                'title': title,
                'content': content,
                'cluster_id': cluster_id
            })
            limit_chars = 700
            temp_recent_texts.append(f"{title}. {content[:limit_chars]}")
            
        conn.close()
        
        # Batch pre-calculate initial cached embeddings
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
    """Checks DB for similar articles and assigns a cluster_id (High-Speed O(1) Vector Stack comparison)."""
    if semantic_model is None or util is None or torch is None:
        return str(uuid.uuid4()), None

    try:
        # Use the cache populated at startup
        cache_list = list(recent_articles_cache)
        valid_items = [a for a in cache_list if 'embedding' in a]
        
        limit_chars = 700
        new_text = f"{new_title}. {new_summary[:limit_chars]}"
        new_embedding = semantic_model.encode(new_text, convert_to_tensor=True)

        if not valid_items:
            return str(uuid.uuid4()), new_embedding

        # Prepare stacked tensors instantly without re-encoding
        existing_embeddings = torch.stack([a['embedding'] for a in valid_items])
        existing_ids = [a['cluster_id'] for a in valid_items]

        # Calculate Cosine Similarity
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


def save_article(source, title, url, summary, image_url):
    """
    Saves a single article to the database. 
    INCLUDES: The strict 90-word minimum length check & SQLite thread-safe insert.
    """
    global existing_urls_cache, recent_articles_cache, MAX_ID
    
    # --- STEP 0: STRICT GLOBAL WORD COUNT CHECK ---
    if not summary:
        final_word_count = 0
    else:
        # Robustly calculate word count after initial cleanup
        cleaned_summary = " ".join(summary.replace('\n', ' ').replace('\r', ' ').split()).strip()
        final_word_count = len(cleaned_summary.split())

    MIN_SUMMARY_WORDS = 90
    if final_word_count < MIN_SUMMARY_WORDS:
        logging.warning(f"SKIPPED (GLOBAL WORD LIMIT): Article '{title}' from {source} has only {final_word_count} words (Min: {MIN_SUMMARY_WORDS}).")
        return False
    
    try:
        title = " ".join(title.replace('\n', ' ').replace('\r', ' ').split()).strip()
        summary = cleaned_summary # Use the cleaned summary from above
        
        if not image_url:
            image_url = "No image available"

        # --- AI PASS: Calculate embedding and cluster_id under thread-safe lock ---
        with ai_lock:
            cluster_id, new_embedding = get_cluster_id_for_article(title, summary)

        # --- THREAD-SAFE DB WRITE BLOCK ---
        with sheet_lock:
            # Check duplicate URL
            if url in existing_urls_cache:
                logging.info(f"Duplicate article skipped: {title} from {source}")
                return False 

            MAX_ID += 1
            new_id = MAX_ID

            # Connect to DB and insert
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Resolve source_id
            cursor.execute("INSERT OR IGNORE INTO sources (name) VALUES (?)", (source,))
            cursor.execute("SELECT id FROM sources WHERE name = ?", (source,))
            source_id = cursor.fetchone()[0]
            
            # Compress content
            compressed_content = zlib.compress(summary.encode('utf-8'))
            scraped_timestamp = int(time.time())
            
            try:
                cursor.execute("""
                    INSERT INTO articles (id, cluster_id, source_id, title, url, content, image_url, scraped_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'scraped')
                """, (new_id, cluster_id, source_id, title, url, compressed_content, image_url, scraped_timestamp))
                conn.commit()
            except sqlite3.IntegrityError as e:
                logging.warning(f"Database IntegrityError (likely duplicate URL): {e}")
                conn.close()
                return False
                
            conn.close()
            
            # Update Caches immediately
            existing_urls_cache.add(url)
            cache_entry = {
                'title': title, 
                'content': summary, 
                'cluster_id': cluster_id
            }
            if new_embedding is not None:
                cache_entry['embedding'] = new_embedding
            recent_articles_cache.append(cache_entry)

        # Explicit LOGGING of successful saves
        logging.info(f">>> SUCCESSFULLY SAVED [ID: {new_id}] - {title} from {source} ({final_word_count} words)")
        print(f"Saved: {title} [ID: {new_id}]") 
        return True
        
    except Exception as e:
        logging.error(f"Error saving article {title}: {e}")
        return False


# --- Generic Scraper Function with Dynamic Quota Handling ---
def scrape_source(session, selenium_driver, source_config, proxies_dict):
    """
    A generic function that scrapes any source based on its config.
    Applies per-source custom article limits dynamically.
    """
    name = source_config['name']
    rss_url = source_config['rss_url']
    
    articles_saved_list = []
    
    logging.info(f"Starting scrape for {name} RSS feed: {rss_url}")
    
    try:
        # 1. Get RSS Feed
        rss_headers = get_headers(source_config['rss_headers_type'])
        response = session.get(rss_url, headers=rss_headers, timeout=7, proxies=proxies_dict)  
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'xml')
        items = soup.find_all('item')
        
        # --- Dynamic Quota Calculation ---
        max_quota = source_config.get('max_articles', MAX_ARTICLES_PER_SOURCE)
        logging.info(f"Found {len(items)} articles in {name} RSS feed. Processing until {max_quota} new articles are saved.")

        # 2. Process each article
        for item in items:
            
            # Stop processing if we have successfully saved our target quota
            if len(articles_saved_list) >= max_quota:
                 logging.info(f"[{name}] Target reached: Successfully saved {max_quota} new articles.")
                 break

            article_url = None
            rss_title = "Title not found"
            rss_description = None

            try:
                if not item.link:
                    continue
                
                article_url = item.link.text.strip()
                
                # Check for URL filter
                if source_config['article_url_contains'] and source_config['article_url_contains'] not in article_url:
                    logging.warning(f"[{name}] Skipping non-article link: {article_url}")
                    continue
                
                # --- Early Skip Check ---
                if article_url in existing_urls_cache:
                    logging.info(f"[{name}] Early skip: URL {article_url} already exists.")
                    time.sleep(0.001)
                    continue

                rss_title = item.title.text if item.title else "Title not found"
                rss_description = None
                
                # Get the raw RSS description now, for potential fallback later
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
                    
                    try:
                        # --- STRATEGY ROUTER ---
                        if strategy.startswith('requests_'):
                            header_type = strategy.replace('requests_', '')
                            article_headers = get_headers(header_type)
                            article_headers['Referer'] = source_config['referer']
                            
                            page_response = session.get(article_url, headers=article_headers, timeout=7, proxies=proxies_dict)
                            page_response.raise_for_status()
                            raw_html = page_response.text
                        
                        elif strategy == 'selenium_browser':
                            if not selenium_driver:
                                logging.error(f"[{name}] Selenium strategy selected but driver is not available. Skipping.")
                                continue
                            
                            try:
                                try:
                                    selenium_driver.get("about:blank")
                                except Exception:
                                    pass
                                selenium_driver.get(article_url)
                                resolved_url = selenium_driver.current_url
                                if resolved_url and "news.google.com" not in resolved_url:
                                    article_url = resolved_url
                            except TimeoutException:
                                logging.warning(f"[{name}] Page get timed out (7s). Proceeding to grab partial source anyway.")
                                pass
                            
                            try:
                                WebDriverWait(selenium_driver, 3).until(
                                    EC.presence_of_element_located((By.TAG_NAME, "p"))
                                )
                                logging.info(f"[{name}] Page content loaded.")
                            except TimeoutException:
                                logging.warning(f"[{name}] Page explicit wait timed out (3s). Proceeding anyway.")
                                
                            raw_html = selenium_driver.page_source
                        
                        else:
                            logging.error(f"[{name}] Unknown strategy: {strategy}. Skipping.")
                            continue

                        # Extract Content
                        if not raw_html:
                            logging.warning(f"[{name}] FAILED with '{strategy}' (HTML was empty).")
                            continue

                        temp_summary = trafilatura.extract(raw_html, include_comments=False, include_tables=False)
                        
                        word_count = len(temp_summary.split()) if temp_summary else 0
                        
                        if word_count >= 90:
                            logging.info(f"[{name}] Success with '{strategy}'. Found content ({word_count} words).")
                            summary = temp_summary
                            
                            # Parse metadata
                            soup = BeautifulSoup(raw_html, 'html.parser')
                            page_title = soup.find('title')
                            if page_title:
                                final_title = page_title.text
                                
                            og_image = soup.find('meta', property='og:image')
                            if og_image:
                                image_url = og_image['content']
                                
                            break # Success! Exit strategy loop
                        else:
                            logging.warning(f"[{name}] FAILED with '{strategy}' (content was too short: {word_count} words).")
                    
                    except Exception as e:
                        logging.error(f"[{name}] Request failed for strategy '{strategy}' on URL {article_url}: {e}")
                        
                    if i < len(strategies) - 1:
                        time.sleep(random.uniform(0.5, 1.0))
                
                # Final Summary Assignment (If all strategies failed, use the RSS description)
                if not summary:
                    logging.error(f"[{name}] All scrape strategies failed for {article_url}. Falling back to RSS description.")
                    summary = rss_description or "No content available"

                # Save article
                was_saved = save_article(name, final_title, article_url, summary, image_url)
                if was_saved:
                    articles_saved_list.append(final_title)
                    
                time.sleep(random.uniform(0.5, 1.5))

            except Exception as e:
                logging.error(f"[{name}] Article-level Error: {e} for url {article_url}")

    except requests.RequestException as e:
        logging.error(f"Failed to fetch {name} RSS feed: {e}")
    except Exception as e:
        logging.error(f"Failed to parse {name} RSS feed: {e}")
        
    return (name, len(articles_saved_list))


# --- Thread Wrapper Function ---
def scrape_source_wrapper(source, session, proxies_dict):
    """
    A wrapper function to be run in a separate thread.
    It creates and destroys its own Selenium driver if needed.
    """
    name = source.get('name', 'Unknown')
    driver = None
    
    needs_selenium = any('selenium' in s for s in source.get('article_strategies', []))
    
    try:
        if needs_selenium and SELENIUM_AVAILABLE:
            logging.info(f"[{name}] (Thread) requires Selenium. Initializing driver...")
            driver = create_selenium_driver()
            if not driver:
                logging.error(f"[{name}] (Thread) Selenium driver failed to start. This source will fail.")
        
        return scrape_source(session, driver, source, proxies_dict)
    
    except Exception as e:
        logging.critical(f"--- CRITICAL: (Thread) Scrape job for {name} failed entirely. --- {e}")
        return (name, 0)
    
    finally:
        # --- Surgical Kill for the driver ---
        if driver:
            logging.info(f"[{name}] (Thread) Finished. Attempting to shut down its Selenium driver.")
            pid_to_kill = None
            try:
                pid_to_kill = driver.service.process.pid
            except Exception:
                pass
            
            try:
                driver.quit()
                logging.info(f"[{name}] (Thread) driver.quit() successful.")
            except Exception as e:
                logging.warning(f"[{name}] (Thread) driver.quit() failed: {e}. Attempting surgical kill.")
                if pid_to_kill:
                    try:
                        os.kill(pid_to_kill, 9)
                        logging.info(f"[{name}] (Thread) Successfully killed stuck driver process PID {pid_to_kill}.")
                    except Exception as e_kill:
                        logging.error(f"[{name}] (Thread) Failed to kill process PID {pid_to_kill}: {e_kill}")
                else:
                    logging.error(f"[{name}] (Thread) driver.quit() failed, but PID was not found. A zombie process may remain.")
            

# --- scrape_all() ---
def scrape_all():
    """Runs all scraping jobs defined in SOURCE_CONFIG in parallel."""
    logging.info("--- Starting new scraping job (Parallel Mode) ---")
    
    # --- INIT GOOGLE SHEETS FIRST ---
    init_google_sheets()

    session = create_robust_session()
    
    proxies_dict = None
    if PROXY_SETTINGS["use_proxies"] and PROXY_SETTINGS["proxy_url"]:
        logging.info(f"Proxy is ENABLED. Routing requests through: {PROXY_SETTINGS['proxy_url']}")
        proxies_dict = {
            "http": PROXY_SETTINGS["proxy_url"],
            "https": PROXY_SETTINGS["proxy_url"]
        }
    else:
        logging.info("Proxy is DISABLED.")
    
    all_counts = {}
    total_saved = 0
    futures = []
    
    executor = ThreadPoolExecutor(max_workers=len(SOURCE_CONFIG))

    try:
        # 1. Submit all jobs
        for source in SOURCE_CONFIG:
            future = executor.submit(scrape_source_wrapper, source, session, proxies_dict)
            futures.append(future)

        logging.info(f"Submitted {len(futures)} jobs to thread pool. Waiting up to 420s for completion...")
        
        # 2. Wait for jobs to complete, with a 7-minute (420s) timeout
        done, not_done = wait(futures, timeout=420)

        # 3. Process completed jobs
        for future in done:
            try:
                name, count = future.result()
                all_counts[name] = count
                total_saved += count
            except Exception as e:
                logging.error(f"A future job resulted in an error: {e}")
        
        # 4. Handle jobs that timed out
        if not_done:
            logging.critical(f"--- TIMEOUT: {len(not_done)} scrape jobs did not complete in 420s. ---")
            for future in not_done:
                logging.error("A thread has timed out and will be abandoned.")
                all_counts["Timed_Out_Jobs"] = all_counts.get("Timed_Out_Jobs", 0) + 1

    except Exception as e:
        logging.critical(f"--- CRITICAL: The entire scrape_all job failed. --- {e}")
        
    finally:
        # 5. Shut down the executor
        logging.info("Shutting down thread pool (wait=False)...")
        executor.shutdown(wait=False)
        
        # Create a dynamic log message
        log_summary = ", ".join(f"{count} {name}" for name, count in all_counts.items())
        log_message = f"Scraped: {log_summary} articles. (Total saved: {total_saved})"
        
        logging.info(log_message)
        print(log_message)
        
        logging.info("--- Scraping job finished ---")


# --- main() function with cleanup ---
def main():
    """
    Main function to run the scraper once.
    Includes robust error handling and DB connection closing.
    """
    
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
        
        # Force process exit to kill zombie threads (especially for Selenium)
        logging.info("--- Main thread finished. Forcing process exit to kill zombie threads. ---")
        os._exit(0)

if __name__ == '__main__':
    main()
