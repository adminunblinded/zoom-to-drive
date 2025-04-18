import requests
import io
import redis
import json
import pickle
from celery import Celery
from datetime import datetime
import urllib.parse
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import credentials as google_credentials
from requests.exceptions import ConnectionError, ChunkedEncodingError, Timeout
import importlib.metadata
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# More comprehensive monkey patch for EntryPoints issue in Python 3.12 with Celery
def patch_celery_entry_points():
    try:
        # Direct patch for celery.utils.imports.load_extension_class_names
        from celery.utils import imports
        original_load_extension = imports.load_extension_class_names
        
        def patched_load_extension_class_names(namespace):
            try:
                eps = importlib.metadata.entry_points()
                result = {}
                # Filter entry points for the requested namespace
                for ep in eps:
                    if ep.group == namespace:
                        result[ep.name] = ep.value
                return result
            except Exception as e:
                logger.error(f"EntryPoints patch error: {e}")
                return {}
                
        # Replace the original function with our patched version
        imports.load_extension_class_names = patched_load_extension_class_names
        
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to apply EntryPoints patch: {e}")

# Apply the patch before Celery is initialized
patch_celery_entry_points()

redis_url = 'redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379'

# Initialize Celery with explicit broker and no backend
celery = Celery('tasks')
celery.conf.update(
    broker_url=redis_url,
    # Explicitly disable result backend
    result_backend=None,
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
    broker_connection_timeout=30,
    accept_content=['pickle', 'json'],
    task_serializer='pickle',
    result_serializer='pickle',
    worker_proc_alive_timeout=120.0,  # Increased from 60 to 120 seconds
    # Explicitly disable result tracking
    task_ignore_result=True,
    task_store_errors_even_if_ignored=False,
    task_track_started=False,
    task_time_limit=1800,  # Increased from 3600 to 1800 seconds (30 minutes)
    worker_hijack_root_logger=False,
    worker_max_tasks_per_child=10,  # Add memory leak protection
    broker_heartbeat=10,  # Add heartbeat to keep connection alive
    broker_pool_limit=3,  # Limit connections to avoid Redis overload
)

redis_client = redis.from_url(redis_url)

# Separate task to handle folder creation and sharing
@celery.task(bind=True, max_retries=3)
def setup_folders(self, serialized_credentials, recordings):
    try:
        credentials = pickle.loads(serialized_credentials)
        API_VERSION = 'v3'
        drive_service = build('drive', API_VERSION, credentials=credentials)

        # Check if the "Automated Zoom Recordings" folder already exists
        results = drive_service.files().list(
            q="name='Automated Zoom Recordings' and mimeType='application/vnd.google-apps.folder'",
            fields='files(id)',
            spaces='drive'
        ).execute()

        if len(results['files']) > 0:
            recordings_folder_id = results['files'][0]['id']
        else:
            # Create the "Automated Zoom Recordings" folder if it doesn't exist
            file_metadata = {
                'name': 'Automated Zoom Recordings',
                'mimeType': 'application/vnd.google-apps.folder'
            }
            recordings_folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            recordings_folder_id = recordings_folder['id']
        
        folder_ids = {}
        
        for recording in recordings:
            topics = recording['topic']
            folder_name = topics.replace(" ", "_")  # Replacing spaces with underscores
            folder_name = folder_name.replace("'", "\\'")  # Escape single quotation mark
            
            folder_urls_data = redis_client.get("folder_urls")
            if folder_urls_data:
                existing_folder_urls = json.loads(folder_urls_data)
            else:
                existing_folder_urls = {}
                
            # Retrieve the stored_params dictionary from Redis
            stored_params_data = redis_client.get("stored_params")
            if stored_params_data:
                stored_params = json.loads(stored_params_data)
            else:
                stored_params = {}
                
            # Check if the accountName is in the topic
            for accountName, email in stored_params.items():
                if accountName is not None and email is not None:
                    if accountName in topics and accountName not in existing_folder_urls:
                        # Share the folder with the email
                        folder_url = share_folder_with_email(drive_service, folder_name, email, recordings_folder_id)
                        existing_folder_urls[accountName] = folder_url
                
            # Store the updated data back into the Redis database
            redis_client.set("folder_urls", json.dumps(existing_folder_urls))

            # Check if the folder already exists within "Automated Zoom Recordings"
            results = drive_service.files().list(
                q=f"name='{folder_name}' and '{recordings_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'",
                fields='files(id)',
                spaces='drive'
            ).execute()

            if len(results['files']) > 0:
                folder_id = results['files'][0]['id']
            else:
                # Create the folder within "Automated Zoom Recordings" if it doesn't exist
                file_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [recordings_folder_id]
                }
                folder = drive_service.files().create(body=file_metadata, fields='id').execute()
                folder_id = folder['id']
            
            folder_ids[topics] = folder_id
            
        # Save folder IDs to Redis for the upload task
        redis_client.set("folder_ids", json.dumps(folder_ids))
        
        # Queue the upload tasks for each recording
        for i, recording in enumerate(recordings):
            upload_recording.apply_async(
                args=[serialized_credentials, recording, recordings_folder_id],
                countdown=i * 5  # Stagger tasks by 5 seconds to avoid overwhelming the API
            )
            
    except Exception as e:
        logger.error(f"An error occurred in setup_folders: {str(e)}")
        raise self.retry(exc=e, countdown=30, max_retries=3)

# Task to handle a single recording - prevents single task from taking too long
@celery.task(bind=True, max_retries=5, rate_limit='2/m')
def upload_recording(self, serialized_credentials, recording, recordings_folder_id):
    try:
        credentials = pickle.loads(serialized_credentials)
        API_VERSION = 'v3'
        drive_service = build('drive', API_VERSION, credentials=credentials)
        
        topics = recording['topic']
        folder_name = topics.replace(" ", "_")
        folder_name = folder_name.replace("'", "\\'")
        
        # Get folder ID from Redis if available
        folder_ids_data = redis_client.get("folder_ids")
        if folder_ids_data:
            folder_ids = json.loads(folder_ids_data)
            folder_id = folder_ids.get(topics)
        
        # If not found in Redis, look up directly
        if not folder_id:
            results = drive_service.files().list(
                q=f"name='{folder_name}' and '{recordings_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'",
                fields='files(id)',
                spaces='drive'
            ).execute()

            if len(results['files']) > 0:
                folder_id = results['files'][0]['id']
            else:
                # Create folder if needed
                file_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [recordings_folder_id]
                }
                folder = drive_service.files().create(body=file_metadata, fields='id').execute()
                folder_id = folder['id']
                
        # Process each file in the recording
        for file_data in recording['recording_files']:
            # Queue a separate task for each file
            process_file.apply_async(
                args=[serialized_credentials, recording, file_data, folder_id],
                countdown=5  # Small delay to prevent API rate limiting
            )
            
    except Exception as e:
        logger.error(f"Error processing recording {recording.get('topic', 'Unknown')}: {str(e)}")
        raise self.retry(exc=e, countdown=30)

# Task to process a single file
@celery.task(bind=True, max_retries=5, soft_time_limit=900, time_limit=1000)
def process_file(self, serialized_credentials, recording, file_data, folder_id):
    try:
        if file_data['status'] != 'completed' or file_data['file_extension'] != 'MP4' or recording['duration'] < 10:
            return  # Skip non-video files or short recordings
            
        credentials = pickle.loads(serialized_credentials)
        API_VERSION = 'v3'
        drive_service = build('drive', API_VERSION, credentials=credentials)
        
        topics = recording['topic']
        start_time = recording['start_time']
        start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
        date_string = start_datetime.strftime("%Y-%m-%d_%H-%M-%S")
        video_filename = f"{topics}_{date_string}.mp4"
        video_filename = video_filename.replace("'", "\\'")
        download_url = file_data['download_url']
        
        # Check if file already exists to avoid duplicate uploads
        query = f"name='{video_filename}' and '{folder_id}' in parents"
        existing_files = drive_service.files().list(
            q=query,
            fields='files(id)',
            spaces='drive'
        ).execute()

        if len(existing_files['files']) > 0:
            logger.info(f"Skipping upload of '{video_filename}' as it already exists.")
            return
            
        # Download with timeouts and retries
        download_with_timeouts_and_upload(self, drive_service, download_url, video_filename, folder_id)
            
    except Exception as e:
        logger.error(f"Error processing file {file_data.get('id', 'Unknown')}: {str(e)}")
        raise self.retry(exc=e, countdown=30)

def download_with_timeouts_and_upload(task, drive_service, download_url, video_filename, folder_id):
    max_retries = 3
    retry_count = 0
    chunk_size = 1024 * 1024  # 1MB chunks for better handling
    
    while retry_count < max_retries:
        try:
            # Use timeouts for the request
            response = requests.get(
                download_url, 
                stream=True,
                timeout=(10, 60)  # 10s connect, 60s read
            )
            response.raise_for_status()
            
            # Capture the video content in a BytesIO object
            video_content = io.BytesIO()
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    video_content.write(chunk)
                    # Add a slight pause every 10MB to prevent timeouts
                    if video_content.tell() % (10 * chunk_size) == 0:
                        time.sleep(0.1)
            
            # Reset the file pointer to the beginning of the content
            video_content.seek(0)
            
            # Upload the video to the folder in Google Drive
            file_metadata = {
                'name': video_filename,
                'parents': [folder_id]
            }
            
            # Use resumable upload for more reliable transfers
            media = MediaIoBaseUpload(
                video_content, 
                mimetype='video/mp4', 
                resumable=True,
                chunksize=chunk_size
            )
            
            request = drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            )
            
            response = None
            backoff = 1
            while response is None:
                try:
                    status, response = request.next_chunk()
                    if status:
                        logger.info(f"Uploaded {int(status.progress() * 100)}% of {video_filename}")
                except Exception as e:
                    if backoff > 64:
                        raise e
                    logger.warning(f"Upload error, retrying in {backoff}s: {str(e)}")
                    time.sleep(backoff)
                    backoff *= 2
            
            logger.info(f"Successfully uploaded {video_filename}")
            return
            
        except (ConnectionError, ChunkedEncodingError, Timeout) as e:
            retry_count += 1
            wait_time = 5 * retry_count
            logger.warning(f"Download/upload attempt {retry_count} failed: {str(e)}. Retrying in {wait_time}s")
            time.sleep(wait_time)
    
    # If we've exhausted retries, raise an exception
    raise Exception(f"Failed to download or upload {video_filename} after {max_retries} attempts")

@celery.task(bind=True, max_retries=3)
def uploadFiles(self_or_serialized_credentials=None, recordings_or_self=None, recordings=None):
    """
    This function is designed to work both as a Celery task and as a direct function call
    """
    # Handle both direct calls and Celery task calls
    if recordings is None:
        # Direct function call - first arg is credentials, second is recordings
        serialized_credentials = self_or_serialized_credentials
        recordings = recordings_or_self
    else:
        # Celery task call - first arg is self, second is credentials, third is recordings
        serialized_credentials = recordings_or_self
    
    # Instead of processing everything in one task, delegate to smaller tasks
    try:
        # Start with folder setup which will then queue individual recording uploads
        setup_folders.delay(serialized_credentials, recordings)
        return "Processing started"
    except Exception as e:
        logger.error(f"Error starting processing: {str(e)}")
        raise
        
def share_folder_with_email(drive_service, folder_name, email, recordings_folder_id):
    # Check if the folder already exists within "Automated Zoom Recordings"
    results = drive_service.files().list(
        q=f"name='{folder_name}' and '{recordings_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'",
        fields='files(id, webViewLink)',
        spaces='drive'
    ).execute()

    if len(results['files']) > 0:
        folder_id = results['files'][0]['id']
        folder_web_view_link = results['files'][0]['webViewLink']
    else:
        # Create the folder within "Automated Zoom Recordings" if it doesn't exist
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [recordings_folder_id]
        }
        folder = drive_service.files().create(body=file_metadata, fields='id, webViewLink').execute()
        folder_id = folder['id']
        folder_web_view_link = folder['webViewLink']

    # Share the folder with the email
    permission_metadata = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': email
    }
    drive_service.permissions().create(
        fileId=folder_id,
        body=permission_metadata,
        fields='id'
    ).execute()

    return folder_web_view_link
