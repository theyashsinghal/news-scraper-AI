import sqlite3
import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURATION ---
DB_PATH = 'news_articles.db'
SHEET_NAME = 'News Scrapper AI'
WORKSHEET_NAME = 'Sheet1'

def migrate():
    # 1. Connect to Google Sheets
    print("Connecting to Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Load credentials from Environment Variable
    gcp_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
    if not gcp_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_JSON secret is missing!")
        
    creds_dict = json.loads(gcp_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # Open the sheet
    try:
        sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    except Exception as e:
        print(f"Error opening sheet '{SHEET_NAME}' / '{WORKSHEET_NAME}': {e}")
        return

    # 2. Connect to SQLite Database
    if not os.path.exists(DB_PATH):
        print(f"Database file {DB_PATH} not found. Nothing to migrate.")
        return

    print(f"Reading data from {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Select columns matching your desired JSON structure
    # Note: We map 'summary' from DB to 'content' in your JSON requirement
    try:
        cursor.execute("SELECT id, cluster_id, source, title, url, summary, image_url, scraped_at FROM news")
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Error reading database: {e}")
        return

    # 3. Format data as Single-Column JSON
    rows_to_upload = []
    
    print(f"Found {len(rows)} articles. Preparing JSON objects...")
    
    for row in rows:
        # Construct the dictionary object
        article_obj = {
            "id": row[0],
            "cluster_id": row[1],
            "source": row[2],
            "title": row[3],
            "url": row[4],
            "content": row[5],  # Mapping DB 'summary' to JSON 'content'
            "image_url": row[6],
            "scraped_at": row[7]
        }
        
        # Convert to JSON string
        json_string = json.dumps(article_obj)
        
        # Append as a single-element list (representing one cell in a row)
        rows_to_upload.append([json_string])

    # 4. Upload to Google Sheets
    if rows_to_upload:
        print(f"Uploading {len(rows_to_upload)} rows to Google Sheets...")
        # append_rows is more efficient than adding one by one
        sheet.append_rows(rows_to_upload)
        print("Migration complete!")
    else:
        print("No data to upload.")

    conn.close()

if __name__ == "__main__":
    migrate()
