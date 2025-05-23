import requests
import io
import redis
import json
import pickle
from celery import Celery, signals
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
import os
import backoff
import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure Sentry for error tracking
sentry_dsn = os.environ.get('SENTRY_DSN')
if sentry_dsn:
    sentry_sdk.init(
        dsn=sentry_dsn,
        integrations=[CeleryIntegration()],
        traces_sample_rate=0.2,
        environment=os.environ.get('ENVIRONMENT', 'production')
    )

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
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
                # Handle different entry_points() return types between Python versions
                if hasattr(eps, 'select'):  # Python 3.10+ with importlib_metadata >= 3.6.0
                    selected_eps = eps.select(group=namespace)
                    for ep in selected_eps:
                        result[ep.name] = ep.value
                elif hasattr(eps, 'get'):  # Old style (Python < 3.10)
                    for ep in eps.get(namespace, []):
                        result[ep.name] = ep.value
                else:  # Python 3.10+ with newer importlib.metadata
                    for ep in eps:
                        if ep.group == namespace:
                            result[ep.name] = ep.value
                return result
            except Exception as e:
                logger.error(f"EntryPoints patch error: {e}")
                return {}
                
        # Replace the original function with our patched version
        imports.load_extension_class_names = patched_load_extension_class_names
        logger.info("Successfully applied EntryPoints patch for Celery")
        
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to apply EntryPoints patch: {e}")

# Apply the patch before Celery is initialized
patch_celery_entry_points()

redis_url = os.environ.get('REDIS_URL', 'redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379')

# Initialize Celery with explicit broker and backend for task status tracking
celery = Celery('tasks')
celery.conf.update(
    broker_url=redis_url,
    result_backend=redis_url,
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
    broker_connection_timeout=30,
    accept_content=['pickle', 'json'],
    task_serializer='pickle',
    result_serializer='pickle',
    worker_proc_alive_timeout=180.0,  # Increased from 120 to 180 seconds
    task_ignore_result=False,  # Enable result tracking for monitoring
    task_store_errors_even_if_ignored=True,
    task_track_started=True,
    task_time_limit=1800,  # 30 minutes
    worker_hijack_root_logger=False,
    worker_max_tasks_per_child=5,  # Reduced from 10 to 5 for better memory management
    broker_heartbeat=10,
    broker_pool_limit=5,
    worker_concurrency=4,  # Set number of worker processes
    worker_prefetch_multiplier=1,  # Process one task at a time per worker
    task_acks_late=True,  # Only acknowledge tasks after they're completed
    task_create_missing_queues=True,
    task_default_queue='default',
    worker_pool='gevent',  # Use gevent pool for non-blocking I/O
    worker_pool_restarts=True,
    task_routes={
        'tasks.setup_folders': {'queue': 'setup'},
        'tasks.upload_recording': {'queue': 'upload'},
        'tasks.process_file': {'queue': 'process'},
    }
)

# Register Celery signals for better monitoring
@signals.task_failure.connect
def task_failure_handler(task_id, exception, traceback, *args, **kwargs):
    logger.error(f"Task {task_id} failed: {exception}")
    
    # Update task status in Redis
    try:
        redis_client = redis.from_url(redis_url, socket_timeout=10)
        redis_client.set(f"task_error:{task_id}", f"Error: {str(exception)}")
    except Exception as e:
        logger.error(f"Error updating task failure status: {str(e)}")
    
@signals.task_success.connect
def task_success_handler(sender=None, **kwargs):
    logger.info(f"Task {sender.request.id} completed successfully")

@signals.worker_ready.connect
def worker_ready_handler(**kwargs):
    logger.info("Worker is ready to receive tasks")
    
    # Debug info about Celery configuration
    try:
        from celery import current_app
        logger.info(f"Celery broker URL: {current_app.conf.broker_url}")
        logger.info(f"Celery result backend: {current_app.conf.result_backend}")
    except Exception as e:
        logger.error(f"Error getting Celery debug info: {str(e)}")

redis_client = redis.from_url(redis_url, socket_timeout=10)

# Separate task to handle folder creation and sharing
@celery.task(
    bind=True, 
    max_retries=5,
    default_retry_delay=60,
    rate_limit='5/m',
    queue='setup'
)
def setup_folders(self, serialized_credentials, recordings):
    task_id = self.request.id
    logger.info(f"Starting setup_folders task {task_id} with {len(recordings)} recordings")
    
    try:
        # Update task status
        update_task_status(task_id, "PROCESSING_FOLDERS", f"Setting up folders for {len(recordings)} recordings")
        
        credentials = pickle.loads(serialized_credentials)
        API_VERSION = 'v3'
        
        # Verify credentials are valid and not expired
        if not credentials or not credentials.valid:
            logger.error(f"Invalid or expired credentials in setup_folders task {task_id}")
            update_task_status(task_id, "ERROR", "Invalid or expired credentials")
            return "Error: Invalid credentials"
            
        # Test token by making a small API call
        try:
            test_service = build('drive', API_VERSION, credentials=credentials, cache_discovery=False)
            test_about = test_service.about().get(fields="user").execute()
            user_email = test_about.get("user", {}).get("emailAddress")
            logger.info(f"Successfully connected to Google Drive as {user_email}")
        except Exception as e:
            logger.error(f"Failed to connect to Google Drive API: {str(e)}")
            update_task_status(task_id, "ERROR", f"Failed to connect to Google Drive: {str(e)}")
            if hasattr(e, 'error_details'):
                logger.error(f"Error details: {e.error_details}")
            return f"Error connecting to Google Drive: {str(e)}"
        
        # Build drive service with timeout settings
        drive_service = build(
            'drive', 
            API_VERSION, 
            credentials=credentials,
            cache_discovery=False  # Avoid caching issues
        )

        # Check if the "Automated Zoom Recordings" folder already exists
        results = drive_service.files().list(
            q="name='Automated Zoom Recordings' and mimeType='application/vnd.google-apps.folder'",
            fields='files(id)',
            spaces='drive'
        ).execute()

        if len(results['files']) > 0:
            recordings_folder_id = results['files'][0]['id']
            logger.info(f"Found existing Automated Zoom Recordings folder: {recordings_folder_id}")
        else:
            # Create the main folder if it doesn't exist
            file_metadata = {
                'name': 'Automated Zoom Recordings',
                'mimeType': 'application/vnd.google-apps.folder'
            }
            recordings_folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            recordings_folder_id = recordings_folder['id']
            logger.info(f"Created new Automated Zoom Recordings folder: {recordings_folder_id}")
        
        # Store folder IDs for each recording topic
        folder_ids = {}
        
        # Process recordings in smaller batches to avoid timeouts
        batch_size = 10
        for i in range(0, len(recordings), batch_size):
            batch = recordings[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} recordings")
            
            for recording in batch:
                topics = recording['topic']
                folder_name = topics.replace(" ", "_")
                folder_name = folder_name.replace("'", "\\'")
                
                # Get existing folder URLs
                try:
                    folder_urls_data = redis_client.get("folder_urls")
                    if folder_urls_data:
                        existing_folder_urls = json.loads(folder_urls_data)
                    else:
                        existing_folder_urls = {}
                        
                    # Get stored parameters
                    stored_params_data = redis_client.get("stored_params")
                    if stored_params_data:
                        stored_params = json.loads(stored_params_data)
                    else:
                        stored_params = {}
                    
                    # Check for account names in the topic
                    for accountName, email in stored_params.items():
                        if accountName and email:
                            if accountName in topics and accountName not in existing_folder_urls:
                                # Share folder with the email
                                folder_url = share_folder_with_email(drive_service, folder_name, email, recordings_folder_id)
                                existing_folder_urls[accountName] = folder_url
                                logger.info(f"Shared folder {folder_name} with {email}")
                    
                    # Save updated folder URLs
                    redis_client.set("folder_urls", json.dumps(existing_folder_urls))
                except Exception as e:
                    logger.warning(f"Error processing folder sharing: {str(e)}")
                    # Continue with the task, don't fail if sharing has issues

                # Check if folder exists
                results = drive_service.files().list(
                    q=f"name='{folder_name}' and '{recordings_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'",
                    fields='files(id)',
                    spaces='drive'
                ).execute()

                if len(results['files']) > 0:
                    folder_id = results['files'][0]['id']
                else:
                    # Create folder if it doesn't exist
                    file_metadata = {
                        'name': folder_name,
                        'mimeType': 'application/vnd.google-apps.folder',
                        'parents': [recordings_folder_id]
                    }
                    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
                    folder_id = folder['id']
                    logger.info(f"Created new folder: {folder_name} with ID: {folder_id}")
                
                folder_ids[topics] = folder_id
                
            # Pause briefly between batches to avoid API rate limits
            time.sleep(1)
            
        # Save folder IDs to Redis
        redis_client.set("folder_ids", json.dumps(folder_ids))
        logger.info(f"Saved {len(folder_ids)} folder IDs to Redis")
        
        # Update task status after folder creation
        update_task_status(task_id, "FOLDERS_CREATED", f"Created/found {len(folder_ids)} folders")

        # Queue recording uploads with staggered delays
        queued_count = 0
        for i, recording in enumerate(recordings):
            # Use countdown to stagger the tasks
            delay = i * 5 % 300  # Spread tasks over a 5-minute window
            upload_recording.apply_async(
                args=[serialized_credentials, recording, recordings_folder_id],
                countdown=delay,
                queue='upload'
            )
            queued_count += 1
            
        logger.info(f"Queued {queued_count} upload_recording tasks")
        
        # Final task status update
        update_task_status(task_id, "UPLOADS_QUEUED", f"Queued {queued_count} recordings for upload")
        return f"Setup complete - {queued_count} recordings queued for processing"
            
    except Exception as e:
        logger.error(f"Error in setup_folders task: {str(e)}", exc_info=True)
        # Update task status
        update_task_status(task_id, "ERROR", f"Error setting up folders: {str(e)}")
        # Use exponential backoff for retries
        retry_delay = 30 * (2 ** self.request.retries)
        raise self.retry(exc=e, countdown=retry_delay, max_retries=5)

# Task to handle a single recording
@celery.task(
    bind=True, 
    max_retries=5,
    rate_limit='2/m',
    default_retry_delay=60,
    queue='upload'
)
def upload_recording(self, serialized_credentials, recording, recordings_folder_id):
    task_id = self.request.id
    recording_topic = recording.get('topic', 'Unknown')
    recording_id = recording.get('uuid', 'Unknown')
    
    logger.info(f"Starting upload_recording task {task_id} for '{recording_topic}' (ID: {recording_id})")
    
    try:
        # Update task status
        update_task_status(
            task_id, 
            "PROCESSING_RECORDING", 
            f"Processing recording: {recording_topic}"
        )
        
        credentials = pickle.loads(serialized_credentials)
        
        # Verify credentials are valid
        if not credentials or not credentials.valid:
            logger.error(f"Invalid or expired credentials in upload_recording task {task_id}")
            update_task_status(task_id, "ERROR", "Invalid or expired credentials")
            return "Error: Invalid credentials"
        
        API_VERSION = 'v3'
        drive_service = build('drive', API_VERSION, credentials=credentials, cache_discovery=False)
        
        topics = recording['topic']
        folder_name = topics.replace(" ", "_")
        folder_name = folder_name.replace("'", "\\'")
        
        # Try to get folder ID from Redis
        folder_id = None
        try:
            folder_ids_data = redis_client.get("folder_ids")
            if folder_ids_data:
                folder_ids = json.loads(folder_ids_data)
                folder_id = folder_ids.get(topics)
        except Exception as e:
            logger.warning(f"Error retrieving folder ID from Redis: {str(e)}")
        
        # If folder ID not found in Redis, look up directly
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
                logger.info(f"Created new folder during upload: {folder_name}")
        
        # Count valid files for processing
        valid_files = [
            f for f in recording['recording_files'] 
            if f['status'] == 'completed' and f['file_extension'] == 'MP4' and recording['duration'] >= 10
        ]
        
        if not valid_files:
            logger.warning(f"No valid files found for recording '{recording_topic}'")
            update_task_status(
                task_id, 
                "COMPLETED", 
                f"No valid files found for recording: {recording_topic}"
            )
            return f"No valid files found for recording: {recording_topic}"
        
        logger.info(f"Processing {len(valid_files)} valid files for recording '{recording_topic}'")
        update_task_status(
            task_id, 
            "QUEUING_FILES", 
            f"Queuing {len(valid_files)} files for recording: {recording_topic}"
        )
                
        # Process each file in the recording
        queued_files = 0
        for i, file_data in enumerate(recording['recording_files']):
            if (file_data['status'] == 'completed' and 
                file_data['file_extension'] == 'MP4' and 
                recording['duration'] >= 10):
                
                # Queue with small delay between files from same recording
                process_file.apply_async(
                    args=[serialized_credentials, recording, file_data, folder_id],
                    countdown=i * 10,  # 10-second delay between files
                    queue='process'
                )
                queued_files += 1
        
        update_task_status(
            task_id, 
            "COMPLETED", 
            f"Queued {queued_files} files for upload from recording '{recording_topic}'"
        )
        return f"Queued {queued_files} files for processing from recording '{recording_topic}'"
            
    except Exception as e:
        logger.error(f"Error processing recording '{recording_topic}': {str(e)}")
        update_task_status(
            task_id, 
            "ERROR", 
            f"Error processing recording '{recording_topic}': {str(e)}"
        )
        retry_delay = 60 * (2 ** self.request.retries)
        raise self.retry(exc=e, countdown=retry_delay)

# Process a single file with tenacity retry
@retry(
    retry=retry_if_exception_type((ConnectionError, ChunkedEncodingError, Timeout)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60)
)
def download_with_retry(download_url):
    """Download file with retry logic using tenacity"""
    response = requests.get(
        download_url, 
        stream=True,
        timeout=(10, 60)  # 10s connect, 60s read
    )
    response.raise_for_status()
    return response

# Task to process a single file
@celery.task(
    bind=True, 
    max_retries=5, 
    soft_time_limit=900, 
    time_limit=1000,
    queue='process'
)
def process_file(self, serialized_credentials, recording, file_data, folder_id):
    task_id = self.request.id
    file_id = file_data.get('id', 'Unknown')
    topics = recording.get('topic', 'Unknown')
    
    logger.info(f"Starting process_file task {task_id} for file {file_id} from '{topics}'")
    
    try:
        # Update task status
        update_task_status(
            task_id, 
            "PROCESSING_FILE", 
            f"Processing file {file_id} from recording '{topics}'"
        )
        
        # Skip non-video files or short recordings
        if file_data['status'] != 'completed' or file_data['file_extension'] != 'MP4' or recording['duration'] < 10:
            logger.info(f"Skipping file {file_id} - not a completed MP4 or too short")
            update_task_status(
                task_id, 
                "SKIPPED", 
                f"Skipped file {file_id} - not a valid video file"
            )
            return "Skipped - not a valid video file"
            
        credentials = pickle.loads(serialized_credentials)
        
        # Verify credentials are valid
        if not credentials or not credentials.valid:
            logger.error(f"Invalid or expired credentials in process_file task {task_id}")
            update_task_status(
                task_id, 
                "ERROR", 
                "Invalid or expired credentials"
            )
            return "Error: Invalid credentials"
        
        API_VERSION = 'v3'
        drive_service = build('drive', API_VERSION, credentials=credentials, cache_discovery=False)
        
        start_time = recording['start_time']
        start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
        date_string = start_datetime.strftime("%Y-%m-%d_%H-%M-%S")
        video_filename = f"{topics}_{date_string}.mp4"
        video_filename = video_filename.replace("'", "\\'")
        download_url = file_data['download_url']
        
        # Check if file already exists
        query = f"name='{video_filename}' and '{folder_id}' in parents"
        existing_files = drive_service.files().list(
            q=query,
            fields='files(id)',
            spaces='drive'
        ).execute()

        if len(existing_files['files']) > 0:
            logger.info(f"Skipping upload of '{video_filename}' as it already exists in Drive")
            update_task_status(
                task_id, 
                "SKIPPED_EXISTS", 
                f"Skipped - file already exists: {video_filename}"
            )
            return f"Skipped - file already exists: {video_filename}"
        
        # Update status to downloading    
        update_task_status(
            task_id, 
            "DOWNLOADING", 
            f"Downloading file: {video_filename}"
        )
            
        # Download and upload with enhanced error handling
        result = download_and_upload_file(self, drive_service, download_url, video_filename, folder_id, task_id)
        logger.info(f"Successfully processed file {file_id}: {result}")
        
        # Update final status
        update_task_status(
            task_id, 
            "COMPLETED", 
            f"Successfully uploaded: {video_filename}"
        )
        return result
            
    except Exception as e:
        logger.error(f"Error processing file {file_id}: {str(e)}", exc_info=True)
        update_task_status(
            task_id, 
            "ERROR", 
            f"Error processing file {file_id}: {str(e)}"
        )
        retry_delay = 60 * (2 ** self.request.retries)
        raise self.retry(exc=e, countdown=retry_delay)

def download_and_upload_file(task, drive_service, download_url, video_filename, folder_id, task_id=None):
    """Download and upload a file with enhanced error handling and monitoring"""
    chunk_size = 1024 * 1024  # 1MB chunks
    
    # Use tenacity retry for downloads
    try:
        logger.info(f"Starting download of {video_filename}")
        start_time = time.time()
        
        # Update task status if task_id is provided
        if task_id:
            update_task_status(
                task_id, 
                "DOWNLOADING", 
                f"Downloading file: {video_filename}"
            )
        
        # Download file with retry
        response = download_with_retry(download_url)
        
        # Track file size for logging
        content_length = int(response.headers.get('content-length', 0))
        
        # Capture video in memory buffer
        video_content = io.BytesIO()
        bytes_downloaded = 0
        
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                video_content.write(chunk)
                bytes_downloaded += len(chunk)
                
                # Log progress for large files
                if content_length > 50 * 1024 * 1024 and bytes_downloaded % (20 * 1024 * 1024) == 0:  # Log every 20MB
                    percent = (bytes_downloaded / content_length) * 100 if content_length else 0
                    logger.info(f"Downloaded {bytes_downloaded/(1024*1024):.1f}MB of {video_filename} ({percent:.1f}%)")
                
                # Brief pause every 10MB to prevent timeouts
                if bytes_downloaded % (10 * chunk_size) == 0:
                    time.sleep(0.1)
        
        download_time = time.time() - start_time
        logger.info(f"Downloaded {bytes_downloaded/(1024*1024):.1f}MB in {download_time:.2f}s ({bytes_downloaded/download_time/1024/1024:.2f}MB/s)")
        
        # Reset file pointer for upload
        video_content.seek(0)
        
        # Update task status to uploading
        if task_id:
            update_task_status(
                task_id, 
                "UPLOADING", 
                f"Uploading file to Google Drive: {video_filename}"
            )
        
        # Upload to Google Drive with resumable upload
        file_metadata = {
            'name': video_filename,
            'parents': [folder_id]
        }
        
        # Use resumable upload with appropriate chunk size
        media = MediaIoBaseUpload(
            video_content, 
            mimetype='video/mp4', 
            resumable=True,
            chunksize=chunk_size
        )
        
        # Create the upload request
        logger.info(f"Starting upload of {video_filename} to Google Drive")
        upload_start_time = time.time()
        
        request = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        )
        
        # Process the upload in chunks with exponential backoff for errors
        response = None
        backoff = 1
        retries = 0
        max_retries = 10
        
        while response is None and retries < max_retries:
            try:
                status, response = request.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    # Only log every 20% for large files
                    if progress % 20 == 0:
                        logger.info(f"Uploaded {progress}% of {video_filename}")
                        
            except Exception as e:
                retries += 1
                if retries >= max_retries:
                    raise Exception(f"Failed to upload {video_filename} after {max_retries} retries: {str(e)}")
                
                logger.warning(f"Upload error, attempt {retries}/{max_retries}, retrying in {backoff}s: {str(e)}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)  # Cap backoff at 60 seconds
        
        upload_time = time.time() - upload_start_time
        file_id = response.get('id', 'unknown')
        
        # Update task status to completed
        if task_id:
            update_task_status(
                task_id, 
                "UPLOAD_COMPLETE", 
                f"Successfully uploaded {video_filename} (ID: {file_id})"
            )
        
        logger.info(f"Successfully uploaded {video_filename} to Google Drive in {upload_time:.2f}s, file ID: {file_id}")
        return f"Success: {video_filename} uploaded to Drive (ID: {file_id})"
            
    except Exception as e:
        logger.error(f"Error in download_and_upload_file for {video_filename}: {str(e)}", exc_info=True)
        
        # Update task status to error
        if task_id:
            update_task_status(
                task_id, 
                "ERROR", 
                f"Error uploading {video_filename}: {str(e)}"
            )
        
        raise

@celery.task(bind=True, max_retries=3)
def uploadFiles(self_or_serialized_credentials=None, recordings_or_self=None, recordings=None):
    """
    Legacy entry point task that delegates to setup_folders
    """
    # Handle both direct calls and Celery task calls
    if recordings is None:
        # Direct function call - first arg is credentials, second is recordings
        serialized_credentials = self_or_serialized_credentials
        recordings = recordings_or_self
    else:
        # Celery task call - first arg is self, second is credentials, third is recordings
        serialized_credentials = recordings_or_self
    
    # Check if we have recordings
    if not recordings:
        logger.warning("No recordings provided to uploadFiles")
        return "No recordings to process"
        
    logger.info(f"Starting uploadFiles for {len(recordings)} recordings")
    
    # Start processing pipeline with setup_folders
    try:
        task = setup_folders.delay(serialized_credentials, recordings)
        return f"Processing started with task ID: {task.id}"
    except Exception as e:
        logger.error(f"Error starting processing pipeline: {str(e)}", exc_info=True)
        raise
        
@retry(
    retry=retry_if_exception_type((ConnectionError, ChunkedEncodingError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60)
)
def share_folder_with_email(drive_service, folder_name, email, recordings_folder_id):
    """Share a folder with a user via email with retry logic"""
    # Check if the folder already exists
    results = drive_service.files().list(
        q=f"name='{folder_name}' and '{recordings_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'",
        fields='files(id, webViewLink)',
        spaces='drive'
    ).execute()

    if len(results['files']) > 0:
        folder_id = results['files'][0]['id']
        folder_web_view_link = results['files'][0]['webViewLink']
    else:
        # Create the folder if it doesn't exist
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
        fields='id',
        sendNotificationEmail=True
    ).execute()

    logger.info(f"Shared folder {folder_name} with {email}")
    return folder_web_view_link

# Add a helper function to update task status in Redis
def update_task_status(task_id, status, message=None):
    """Update task status in Redis for monitoring"""
    try:
        task_info_json = redis_client.get(f"task:{task_id}")
        if task_info_json:
            task_info = json.loads(task_info_json)
        else:
            task_info = {"task_id": task_id}
            
        task_info["status"] = status
        if message:
            task_info["message"] = message
        task_info["update_time"] = datetime.now().isoformat()
        
        redis_client.set(f"task:{task_id}", json.dumps(task_info))
        logger.info(f"Updated task {task_id} status: {status} - {message}")
    except Exception as e:
        logger.error(f"Error updating task status: {str(e)}")

# Add synchronous versions of the processing functions for use when Celery workers aren't available
def setup_folders_sync(serialized_credentials, recordings):
    """Synchronous version of setup_folders for direct execution"""
    logger.info(f"Starting synchronous setup_folders with {len(recordings)} recordings")
    
    try:
        # Update an in-memory status
        status = {
            "status": "PROCESSING_FOLDERS",
            "message": f"Setting up folders for {len(recordings)} recordings",
            "start_time": datetime.now().isoformat()
        }
        
        credentials = pickle.loads(serialized_credentials)
        API_VERSION = 'v3'
        
        # Verify credentials are valid and not expired
        if not credentials or not credentials.valid:
            logger.error("Invalid or expired credentials in synchronous setup_folders")
            status["status"] = "ERROR"
            status["message"] = "Invalid or expired credentials"
            return status
            
        # Test token by making a small API call
        try:
            test_service = build('drive', API_VERSION, credentials=credentials, cache_discovery=False)
            test_about = test_service.about().get(fields="user").execute()
            user_email = test_about.get("user", {}).get("emailAddress")
            logger.info(f"Successfully connected to Google Drive as {user_email}")
        except Exception as e:
            logger.error(f"Failed to connect to Google Drive API: {str(e)}")
            status["status"] = "ERROR"
            status["message"] = f"Failed to connect to Google Drive: {str(e)}"
            return status
        
        # Build drive service
        drive_service = build(
            'drive', 
            API_VERSION, 
            credentials=credentials,
            cache_discovery=False
        )

        # Check if the "Automated Zoom Recordings" folder already exists
        results = drive_service.files().list(
            q="name='Automated Zoom Recordings' and mimeType='application/vnd.google-apps.folder'",
            fields='files(id)',
            spaces='drive'
        ).execute()

        if len(results['files']) > 0:
            recordings_folder_id = results['files'][0]['id']
            logger.info(f"Found existing Automated Zoom Recordings folder: {recordings_folder_id}")
        else:
            # Create the main folder if it doesn't exist
            file_metadata = {
                'name': 'Automated Zoom Recordings',
                'mimeType': 'application/vnd.google-apps.folder'
            }
            recordings_folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            recordings_folder_id = recordings_folder['id']
            logger.info(f"Created new Automated Zoom Recordings folder: {recordings_folder_id}")
        
        # Store folder IDs for each recording topic
        folder_ids = {}
        
        # Process recordings in smaller batches to avoid timeouts
        batch_size = 10
        for i in range(0, len(recordings), batch_size):
            batch = recordings[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} recordings")
            
            for recording in batch:
                topics = recording['topic']
                folder_name = topics.replace(" ", "_")
                folder_name = folder_name.replace("'", "\\'")
                
                # Get existing folder URLs
                try:
                    folder_urls_data = redis_client.get("folder_urls")
                    if folder_urls_data:
                        existing_folder_urls = json.loads(folder_urls_data)
                    else:
                        existing_folder_urls = {}
                        
                    # Get stored parameters
                    stored_params_data = redis_client.get("stored_params")
                    if stored_params_data:
                        stored_params = json.loads(stored_params_data)
                    else:
                        stored_params = {}
                    
                    # Check for account names in the topic
                    for accountName, email in stored_params.items():
                        if accountName and email:
                            if accountName in topics and accountName not in existing_folder_urls:
                                # Share folder with the email
                                folder_url = share_folder_with_email(drive_service, folder_name, email, recordings_folder_id)
                                existing_folder_urls[accountName] = folder_url
                                logger.info(f"Shared folder {folder_name} with {email}")
                    
                    # Save updated folder URLs
                    redis_client.set("folder_urls", json.dumps(existing_folder_urls))
                except Exception as e:
                    logger.warning(f"Error processing folder sharing: {str(e)}")
                    # Continue with the task, don't fail if sharing has issues

                # Check if folder exists
                results = drive_service.files().list(
                    q=f"name='{folder_name}' and '{recordings_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'",
                    fields='files(id)',
                    spaces='drive'
                ).execute()

                if len(results['files']) > 0:
                    folder_id = results['files'][0]['id']
                else:
                    # Create folder if it doesn't exist
                    file_metadata = {
                        'name': folder_name,
                        'mimeType': 'application/vnd.google-apps.folder',
                        'parents': [recordings_folder_id]
                    }
                    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
                    folder_id = folder['id']
                    logger.info(f"Created new folder: {folder_name} with ID: {folder_id}")
                
                folder_ids[topics] = folder_id
                
                # Process files for this recording synchronously
                process_recording_sync(serialized_credentials, recording, recordings_folder_id, folder_id)
                
            # Pause briefly between batches to avoid API rate limits
            time.sleep(1)
            
        # Save folder IDs to Redis
        redis_client.set("folder_ids", json.dumps(folder_ids))
        logger.info(f"Saved {len(folder_ids)} folder IDs to Redis")
        
        # Update final status
        status["status"] = "COMPLETED"
        status["message"] = f"Successfully processed {len(recordings)} recordings"
        status["end_time"] = datetime.now().isoformat()
        status["folder_ids"] = list(folder_ids.values())
        
        return status
            
    except Exception as e:
        logger.error(f"Error in synchronous setup_folders: {str(e)}", exc_info=True)
        return {
            "status": "ERROR",
            "message": f"Error in synchronous processing: {str(e)}",
            "time": datetime.now().isoformat()
        }

def process_recording_sync(serialized_credentials, recording, recordings_folder_id, folder_id):
    """Process a recording synchronously"""
    recording_topic = recording.get('topic', 'Unknown')
    recording_id = recording.get('uuid', 'Unknown')
    
    logger.info(f"Processing recording synchronously: '{recording_topic}' (ID: {recording_id})")
    
    try:
        credentials = pickle.loads(serialized_credentials)
        
        # Verify credentials are valid
        if not credentials or not credentials.valid:
            logger.error(f"Invalid or expired credentials when processing recording '{recording_topic}'")
            return {
                "status": "ERROR",
                "message": "Invalid credentials"
            }
        
        # Count valid files for processing
        valid_files = [
            f for f in recording['recording_files'] 
            if f['status'] == 'completed' and f['file_extension'] == 'MP4' and recording['duration'] >= 10
        ]
        
        if not valid_files:
            logger.warning(f"No valid files found for recording '{recording_topic}'")
            return {
                "status": "COMPLETED",
                "message": f"No valid files for recording: {recording_topic}"
            }
        
        logger.info(f"Processing {len(valid_files)} valid files for recording '{recording_topic}'")
        
        # Process each file individually
        processed_files = []
        for file_data in valid_files:
            result = process_file_sync(serialized_credentials, recording, file_data, folder_id)
            processed_files.append(result)
            
        return {
            "status": "COMPLETED",
            "message": f"Processed {len(processed_files)} files for recording: {recording_topic}",
            "files": processed_files
        }
        
    except Exception as e:
        logger.error(f"Error processing recording '{recording_topic}': {str(e)}")
        return {
            "status": "ERROR",
            "message": f"Error processing recording: {str(e)}"
        }

def process_file_sync(serialized_credentials, recording, file_data, folder_id):
    """Process a single file synchronously"""
    file_id = file_data.get('id', 'Unknown')
    topics = recording.get('topic', 'Unknown')
    
    logger.info(f"Processing file synchronously: {file_id} from '{topics}'")
    
    try:
        # Skip non-video files or short recordings
        if file_data['status'] != 'completed' or file_data['file_extension'] != 'MP4' or recording['duration'] < 10:
            logger.info(f"Skipping file {file_id} - not a completed MP4 or too short")
            return {
                "status": "SKIPPED",
                "message": "Not a valid video file",
                "file_id": file_id
            }
            
        credentials = pickle.loads(serialized_credentials)
        
        # Verify credentials are valid
        if not credentials or not credentials.valid:
            logger.error(f"Invalid or expired credentials when processing file {file_id}")
            return {
                "status": "ERROR",
                "message": "Invalid credentials",
                "file_id": file_id
            }
        
        API_VERSION = 'v3'
        drive_service = build('drive', API_VERSION, credentials=credentials, cache_discovery=False)
        
        start_time = recording['start_time']
        start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
        date_string = start_datetime.strftime("%Y-%m-%d_%H-%M-%S")
        video_filename = f"{topics}_{date_string}.mp4"
        video_filename = video_filename.replace("'", "\\'")
        download_url = file_data['download_url']
        
        # Check if file already exists
        query = f"name='{video_filename}' and '{folder_id}' in parents"
        existing_files = drive_service.files().list(
            q=query,
            fields='files(id)',
            spaces='drive'
        ).execute()

        if len(existing_files['files']) > 0:
            logger.info(f"Skipping upload of '{video_filename}' as it already exists in Drive")
            return {
                "status": "SKIPPED_EXISTS",
                "message": f"File already exists: {video_filename}",
                "file_id": file_id
            }
            
        logger.info(f"Downloading and uploading file: {video_filename}")
        
        # Download and upload the file
        result = download_and_upload_file_sync(drive_service, download_url, video_filename, folder_id)
        
        logger.info(f"Successfully processed file {file_id}: {result}")
        return {
            "status": "COMPLETED",
            "message": f"Successfully uploaded: {video_filename}",
            "file_id": file_id,
            "drive_file_id": result.get("drive_file_id")
        }
        
    except Exception as e:
        logger.error(f"Error processing file {file_id}: {str(e)}")
        return {
            "status": "ERROR",
            "message": f"Error processing file: {str(e)}",
            "file_id": file_id
        }

def download_and_upload_file_sync(drive_service, download_url, video_filename, folder_id):
    """Download and upload a file synchronously"""
    chunk_size = 1024 * 1024  # 1MB chunks
    
    try:
        logger.info(f"Starting download of {video_filename}")
        start_time = time.time()
        
        # Download file with retry
        response = download_with_retry(download_url)
        
        # Track file size for logging
        content_length = int(response.headers.get('content-length', 0))
        
        # Capture video in memory buffer
        video_content = io.BytesIO()
        bytes_downloaded = 0
        
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                video_content.write(chunk)
                bytes_downloaded += len(chunk)
                
                # Log progress for large files
                if content_length > 50 * 1024 * 1024 and bytes_downloaded % (20 * 1024 * 1024) == 0:  # Log every 20MB
                    percent = (bytes_downloaded / content_length) * 100 if content_length else 0
                    logger.info(f"Downloaded {bytes_downloaded/(1024*1024):.1f}MB of {video_filename} ({percent:.1f}%)")
                
                # Brief pause every 10MB to prevent timeouts
                if bytes_downloaded % (10 * chunk_size) == 0:
                    time.sleep(0.1)
        
        download_time = time.time() - start_time
        logger.info(f"Downloaded {bytes_downloaded/(1024*1024):.1f}MB in {download_time:.2f}s ({bytes_downloaded/download_time/1024/1024:.2f}MB/s)")
        
        # Reset file pointer for upload
        video_content.seek(0)
        
        logger.info(f"Starting upload of {video_filename} to Google Drive")
        upload_start_time = time.time()
        
        # Upload to Google Drive
        file_metadata = {
            'name': video_filename,
            'parents': [folder_id]
        }
        
        # Use resumable upload with appropriate chunk size
        media = MediaIoBaseUpload(
            video_content, 
            mimetype='video/mp4', 
            resumable=True,
            chunksize=chunk_size
        )
        
        # Create and execute the upload request
        request = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        )
        
        # Process the upload in chunks with exponential backoff for errors
        response = None
        backoff = 1
        retries = 0
        max_retries = 10
        
        while response is None and retries < max_retries:
            try:
                status, response = request.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    # Only log every 20% for large files
                    if progress % 20 == 0:
                        logger.info(f"Uploaded {progress}% of {video_filename}")
                        
            except Exception as e:
                retries += 1
                if retries >= max_retries:
                    raise Exception(f"Failed to upload {video_filename} after {max_retries} retries: {str(e)}")
                
                logger.warning(f"Upload error, attempt {retries}/{max_retries}, retrying in {backoff}s: {str(e)}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)  # Cap backoff at 60 seconds
        
        upload_time = time.time() - upload_start_time
        file_id = response.get('id', 'unknown')
        
        logger.info(f"Successfully uploaded {video_filename} to Google Drive in {upload_time:.2f}s, file ID: {file_id}")
        
        return {
            "status": "success",
            "message": f"Successfully uploaded {video_filename}",
            "drive_file_id": file_id,
            "download_time": download_time,
            "upload_time": upload_time,
            "file_size_mb": bytes_downloaded/(1024*1024)
        }
        
    except Exception as e:
        logger.error(f"Error in download_and_upload_file_sync for {video_filename}: {str(e)}")
        raise
