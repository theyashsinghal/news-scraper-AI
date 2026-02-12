import sqlite3
import gspread
import json
import os
import time
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURATION ---
DB_PATH = 'news_articles.db'
SHEET_NAME = 'News Scrapper AI'
WORKSHEET_NAME = 'Sheet1'
BATCH_SIZE = 500  # Upload 500 rows at a time

# --- GLOBAL TRACKING ---
TRUNCATED_IDS = []

def truncate_to_fit(article_obj):
    """
    Recursively truncates content until the JSON string is under 50,000 chars.
    """
    json_str = json.dumps(article_obj)
    
    # Check size (limit is 50,000, we use 49,000 as a safety buffer)
    if len(json_str) < 49000:
        return json_str
    
    # Only track the ID the first time we realize it's too big
    if article_obj['id'] not in TRUNCATED_IDS:
        TRUNCATED_IDS.append(article_obj['id'])

    # Truncate content
    current_content = article_obj.get('content', '')
    if len(current_content) > 1000:
        # Cut 1000 chars from the end and try again
        article_obj['content'] = current_content[:-1000] + "... [TRUNCATED]"
        return truncate_to_fit(article_obj)
    else:
        # Fallback: Content is small but something else is huge (rare)
        return json.dumps(article_obj)

def migrate():
    # 1. Connect to Google Sheets
    print("Connecting to Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    gcp_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
    if not gcp_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_JSON secret is missing!")
        
    creds_dict = json.loads(gcp_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    try:
        sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    except Exception as e:
        print(f"Error opening sheet: {e}")
        return

    # 2. Connect to SQLite
    if not os.path.exists(DB_PATH):
        print(f"Database file {DB_PATH} not found.")
        return

    print(f"Reading data from {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT id, cluster_id, source, title, url, summary, image_url, scraped_at FROM news")
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Error reading database: {e}")
        return

    # 3. Process and Batch Upload
    print(f"Found {len(rows)} articles. Processing...")
    
    batch = []
    total_uploaded = 0

    for row in rows:
        article_obj = {
            "id": row[0],
            "cluster_id": row[1],
            "source": row[2],
            "title": row[3],
            "url": row[4],
            "content": row[5],
            "image_url": row[6],
            "scraped_at": row[7]
        }
        
        # Ensure it fits
        safe_json_string = truncate_to_fit(article_obj)
        
        # Add to batch as a single-cell row
        batch.append([safe_json_string])

        # Upload when batch is full
        if len(batch) >= BATCH_SIZE:
            try:
                sheet.append_rows(batch)
                total_uploaded += len(batch)
                print(f"Progress: Uploaded {total_uploaded} rows...")
                batch = [] # Clear batch
                time.sleep(1) # Prevent API rate limits
            except Exception as e:
                print(f"Error uploading batch: {e}")
                batch = [] 

    # 4. Upload remaining rows
    if batch:
        try:
            sheet.append_rows(batch)
            total_uploaded += len(batch)
            print(f"Final batch uploaded. Total: {total_uploaded} rows.")
        except Exception as e:
            print(f"Error uploading final batch: {e}")

    conn.close()
    
    # 5. Final Report
    print("-" * 40)
    print("MIGRATION COMPLETE")
    print(f"Total Articles Processed: {len(rows)}")
    print(f"Total Truncated Articles: {len(TRUNCATED_IDS)}")
    if TRUNCATED_IDS:
        print(f"IDs of Truncated Articles: {TRUNCATED_IDS}")
    print("-" * 40)

if __name__ == "__main__":
    migrate()
