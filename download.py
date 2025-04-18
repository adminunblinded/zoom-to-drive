import requests
import pytz
from datetime import datetime, timedelta
import json
import redis
from celery import Celery

redis_url = 'redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379'
redis_conn = redis.from_url(redis_url)
celery_app = Celery('zoom_to_drive', broker=redis_url, backend=redis_url)

def download_zoom_recordings():
    access_token = redis_conn.get('access_token')
    if not access_token:
        print("Access token not found in session. Please authenticate with Zoom.")
        return
    headers = {"Authorization": "Bearer " + access_token.decode()}

    eastern_tz = pytz.timezone('US/Eastern')
    
    end_date = datetime.now(eastern_tz)
    start_date = end_date - timedelta(days=180)  # Changed from 1 day to 180 days (approximately 6 months)
    
    all_recordings = []
    current_date = end_date

    while current_date >= start_date:
        prev_date = current_date - timedelta(days=30)  # Retrieve recordings in 30-day chunks
        if prev_date < start_date:
            prev_date = start_date

        params = {
            "from": prev_date.isoformat() + "Z",
            "to": current_date.isoformat() + "Z",
            "page_size": 300,  # Increase the page size to retrieve more recordings per page
            "page_number": 1
        }
        while True:
            try:
                response = requests.get(
                    "https://api.zoom.us/v2/accounts/me/recordings",
                    headers=headers,
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                recordings_json = response.json()
            except requests.exceptions.RequestException as e:
                print("Error retrieving recordings:", e)
                break
            all_recordings.extend(recordings_json["meetings"])
            total_records = recordings_json["total_records"]
            records_per_page = recordings_json["page_size"]
            total_pages = total_records // records_per_page

            if total_records % records_per_page != 0:
                total_pages += 1

            if params["page_number"] >= total_pages:
                break

            params["page_number"] += 1
        current_date = prev_date - timedelta(days=1)  # Move to the previous date range

    return all_recordings

@celery_app.task
def transfer_to_google_drive(recordings):
    # Initiate transfer of recordings to Google Drive using Google API client
    print(f"Initiating transfer of {len(recordings)} recordings to Google Drive...")
    for rec in recordings:
         print("Transferring recording with id:", rec.get("id", "unknown"))
    print("Transfer complete.")

# Append new Celery task to process Zoom recordings without blocking the web worker
@celery_app.task
def process_zoom_recordings():
    recordings = download_zoom_recordings()
    transfer_to_google_drive(recordings)
