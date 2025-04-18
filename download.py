import requests
import pytz
from datetime import datetime, timedelta
import json
import redis
import logging
import os
from requests.exceptions import ConnectionError, ChunkedEncodingError, Timeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import time
import backoff

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Use environment variable or default
redis_url = os.environ.get('REDIS_URL', 'redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379')
redis_conn = redis.from_url(redis_url, socket_timeout=10)

# Retry decorator for API requests
@retry(
    retry=retry_if_exception_type((ConnectionError, ChunkedEncodingError, Timeout)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60)
)
def fetch_zoom_recordings_page(headers, params):
    """Fetch a single page of Zoom recordings with retry logic"""
    response = requests.get(
        "https://api.zoom.us/v2/accounts/me/recordings",
        headers=headers,
        params=params,
        timeout=(10, 30)  # 10s connect, 30s read timeout
    )
    response.raise_for_status()
    return response.json()

def download_zoom_recordings():
    """Download metadata for Zoom recordings from the past 6 months"""
    start_time = time.time()
    logger.info("Starting Zoom recordings download")
    
    access_token = redis_conn.get('access_token')
    if not access_token:
        logger.warning("Access token not found in session. Please authenticate with Zoom.")
        return []
        
    headers = {"Authorization": "Bearer " + access_token.decode()}

    eastern_tz = pytz.timezone('US/Eastern')
    
    end_date = datetime.now(eastern_tz)
    start_date = end_date - timedelta(days=2)  # Fetch recordings from the past 6 months
    
    all_recordings = []
    current_date = end_date
    date_ranges_processed = 0
    
    try:
        while current_date >= start_date:
            # Process in 30-day chunks to avoid timeouts
            prev_date = current_date - timedelta(days=30)
            if prev_date < start_date:
                prev_date = start_date

            date_range_str = f"{prev_date.strftime('%Y-%m-%d')} to {current_date.strftime('%Y-%m-%d')}"
            logger.info(f"Fetching recordings for date range: {date_range_str}")
            
            params = {
                "from": prev_date.isoformat() + "Z",
                "to": current_date.isoformat() + "Z",
                "page_size": 100,  # Reduced from 300 to avoid timeout issues
                "page_number": 1
            }
            
            # Track pages for this date range
            total_pages = 1
            page_count = 0
            recordings_in_range = 0
            
            while params["page_number"] <= total_pages:
                try:
                    logger.info(f"Fetching page {params['page_number']} of {total_pages} for date range {date_range_str}")
                    
                    # Use retry-wrapped function
                    recordings_json = fetch_zoom_recordings_page(headers, params)
                    
                    # Extract recordings
                    meetings = recordings_json.get("meetings", [])
                    all_recordings.extend(meetings)
                    recordings_in_range += len(meetings)
                    
                    # Update pagination info
                    total_records = recordings_json.get("total_records", 0)
                    records_per_page = recordings_json.get("page_size", 100)
                    total_pages = (total_records + records_per_page - 1) // records_per_page
                    
                    logger.info(f"Retrieved {len(meetings)} recordings from page {params['page_number']}/{total_pages}")
                    
                    # Move to next page
                    params["page_number"] += 1
                    page_count += 1
                    
                    # Add a brief pause between pages to prevent rate limiting
                    if params["page_number"] <= total_pages:
                        time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error fetching page {params['page_number']} for date range {date_range_str}: {str(e)}")
                    
                    if params["page_number"] < total_pages:
                        # Skip to next page if we encounter an error
                        params["page_number"] += 1
                        logger.info(f"Skipping to page {params['page_number']}")
                        time.sleep(2)  # Longer pause after error
                    else:
                        break
            
            logger.info(f"Completed date range {date_range_str}: retrieved {recordings_in_range} recordings across {page_count} pages")
            date_ranges_processed += 1
            
            # Move to the previous date range
            current_date = prev_date - timedelta(days=1)
            
            # Add a pause between date ranges to avoid rate limiting
            if current_date >= start_date:
                time.sleep(2)
    
    except Exception as e:
        logger.error(f"Error during Zoom recordings download: {str(e)}", exc_info=True)
        # Return what we have so far rather than failing completely
        logger.info(f"Returning {len(all_recordings)} recordings collected before error")
        
    # Filter any duplicate recordings
    unique_recordings = []
    seen_ids = set()
    
    for recording in all_recordings:
        recording_id = recording.get('uuid', '')
        if recording_id and recording_id not in seen_ids:
            seen_ids.add(recording_id)
            unique_recordings.append(recording)
            
    duration = time.time() - start_time
    logger.info(f"Zoom download complete: {len(unique_recordings)} unique recordings from {date_ranges_processed} date ranges in {duration:.2f}s")
    
    return unique_recordings
