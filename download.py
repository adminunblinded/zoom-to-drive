import requests
import pytz
from datetime import datetime, timedelta
import json
import redis
import time
from requests.exceptions import RequestException, Timeout

redis_url = 'redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379'
redis_conn = redis.from_url(redis_url)

def download_zoom_recordings():
    access_token = redis_conn.get('access_token')
    if not access_token:
        print("Access token not found in session. Please authenticate with Zoom.")
        return []
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
            "page_size": 100,  # Reduced from 300 to decrease response payload size
            "page_number": 1
        }
        
        try:
            fetch_all_pages(headers, params, all_recordings)
        except Exception as e:
            print(f"Error fetching recordings for date range {prev_date} to {current_date}: {str(e)}")
            # Continue with next date range instead of failing completely
        
        current_date = prev_date - timedelta(days=1)  # Move to the previous date range

    return all_recordings

def fetch_all_pages(headers, params, all_recordings):
    while True:
        try:
            # Add timeout to prevent hanging requests
            response = requests.get(
                "https://api.zoom.us/v2/accounts/me/recordings",
                headers=headers,
                params=params,
                timeout=(10, 30)  # Connection timeout, read timeout
            )
            
            # Check for rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"Rate limited. Waiting for {retry_after} seconds.")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            recordings_json = response.json()
            
            # Check if we actually got any meetings data
            if "meetings" not in recordings_json:
                print(f"Unexpected API response: {recordings_json}")
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
            
            # Short pause between requests to be respectful of API limits
            time.sleep(0.5)
            
        except Timeout:
            print(f"Timeout occurred while fetching page {params['page_number']}. Moving to next page.")
            params["page_number"] += 1
            # If we've tried too many pages, break to avoid infinite loop
            if params["page_number"] > 20:
                print("Too many timeouts. Breaking pagination loop.")
                break
        except RequestException as e:
            print(f"Request error on page {params['page_number']}: {str(e)}")
            # Don't retry immediately, add some backoff
            time.sleep(2)
            # If it's the first page and we got an error, we might want to re-raise
            # to signal a fundamental problem
            if params["page_number"] == 1:
                raise
            params["page_number"] += 1
