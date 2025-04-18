import requests
import pytz
from datetime import datetime, timedelta
import json
import redis
import logging
from requests.exceptions import ConnectionError, ChunkedEncodingError, Timeout

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_url = 'redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379'
redis_conn = redis.from_url(redis_url)

def download_zoom_recordings():
    access_token = redis_conn.get('access_token')
    if not access_token:
        logger.warning("Access token not found in session. Please authenticate with Zoom.")
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
            "page_size": 300,  # Increase the page size to retrieve more recordings per page
            "page_number": 1
        }
        
        max_retries = 3
        retry_count = 0
        
        while True:
            try:
                response = requests.get(
                    "https://api.zoom.us/v2/accounts/me/recordings",
                    headers=headers,
                    params=params,
                    timeout=(10, 30)  # 10s connect, 30s read timeout
                )
                response.raise_for_status()
                recordings_json = response.json()
                all_recordings.extend(recordings_json["meetings"])
                total_records = recordings_json["total_records"]
                records_per_page = recordings_json["page_size"]
                total_pages = total_records // records_per_page

                if total_records % records_per_page != 0:
                    total_pages += 1

                if params["page_number"] >= total_pages:
                    break

                params["page_number"] += 1
                
            except (ConnectionError, ChunkedEncodingError, Timeout) as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Failed to fetch Zoom recordings after {max_retries} attempts: {str(e)}")
                    break
                    
                logger.warning(f"Zoom API request failed (attempt {retry_count}/{max_retries}): {str(e)}")
                continue
                
        current_date = prev_date - timedelta(days=1)  # Move to the previous date range

    logger.info(f"Downloaded metadata for {len(all_recordings)} Zoom recordings")
    return all_recordings
