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
from requests.exceptions import ConnectionError, ChunkedEncodingError
import importlib.metadata

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
                print(f"EntryPoints patch error: {e}")
                return {}
                
        # Replace the original function with our patched version
        imports.load_extension_class_names = patched_load_extension_class_names
        
    except (ImportError, AttributeError) as e:
        print(f"Failed to apply EntryPoints patch: {e}")

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
    worker_proc_alive_timeout=60.0,
    # Explicitly disable result tracking
    task_ignore_result=True,
    task_store_errors_even_if_ignored=False,
    task_track_started=False,
    task_time_limit=3600,
    worker_hijack_root_logger=False
)

redis_client = redis.from_url(redis_url)


@celery.task(bind=True, max_retries=3, name='tasks.process_single_recording')
def process_single_recording(self, serialized_credentials, recording):
    """
    Processes a single Zoom recording: creates necessary folders in Google Drive,
    handles sharing, downloads the video file, and uploads it to the correct folder.
    """
    try:
        credentials = pickle.loads(serialized_credentials)
        API_VERSION = 'v3'
        drive_service = build('drive', API_VERSION, credentials=credentials)

        # --- Get or Create 'Automated Zoom Recordings' Folder ---
        # (This check can be done once, maybe cache the ID in Redis or pass it?)
        # For simplicity now, we keep it, but it's redundant per task.
        # Consider optimizing later by passing recordings_folder_id as an argument.
        main_folder_id_key = "gdrive_main_recordings_folder_id"
        recordings_folder_id = redis_client.get(main_folder_id_key)
        if recordings_folder_id:
            recordings_folder_id = recordings_folder_id.decode('utf-8')
        else:
            results = drive_service.files().list(
                q="name='Automated Zoom Recordings' and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields='files(id)',
                spaces='drive'
            ).execute()
            if len(results['files']) > 0:
                recordings_folder_id = results['files'][0]['id']
            else:
                file_metadata = {
                    'name': 'Automated Zoom Recordings',
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                recordings_folder = drive_service.files().create(body=file_metadata, fields='id').execute()
                recordings_folder_id = recordings_folder['id']
            # Cache the main folder ID for future tasks
            redis_client.set(main_folder_id_key, recordings_folder_id)
        # --- End Get or Create 'Automated Zoom Recordings' Folder ---

        # --- Process This Specific Recording ---
        topics = recording.get('topic', 'Untitled Recording') # Use .get for safety
        # Sanitize folder name for Drive API (avoiding issues with quotes, slashes etc.)
        # Keep alphanumeric, spaces, underscores, hyphens. Replace others.
        folder_name = "".join(c if c.isalnum() or c in (' ', '_', '-') else '_' for c in topics)
        folder_name = folder_name.strip() # Remove leading/trailing whitespace
        if not folder_name: # Handle empty topic case
            folder_name = "Untitled_Recording"

        # --- Handle Folder Sharing (if applicable) ---
        folder_urls_data = redis_client.get("folder_urls")
        existing_folder_urls = json.loads(folder_urls_data) if folder_urls_data else {}

        stored_params_data = redis_client.get("stored_params")
        stored_params = json.loads(stored_params_data) if stored_params_data else {}

        folder_shared_this_run = False # Flag to prevent redundant Redis set
        for accountName, email in stored_params.items():
            if accountName is not None and email is not None:
                # Check if accountName is in the original topic and folder not already shared/recorded
                # Using original topic for matching, but sanitized name for Drive folder
                if accountName in recording.get('topic', '') and accountName not in existing_folder_urls:
                    # Share the folder (it will be created if needed within share_folder_with_email)
                    # Pass sanitized folder_name
                    folder_url = share_folder_with_email(drive_service, folder_name, email, recordings_folder_id)
                    if folder_url: # Only update if sharing succeeded
                        existing_folder_urls[accountName] = folder_url
                        folder_shared_this_run = True

        # Store updated sharing info back to Redis if changes were made
        if folder_shared_this_run:
            redis_client.set("folder_urls", json.dumps(existing_folder_urls))
        # --- End Handle Folder Sharing ---


        # --- Get or Create Specific Recording Topic Folder ---
        # Use sanitized folder_name for query and creation
        # Add trashed=false to avoid matching deleted folders
        query = f"name='{folder_name}' and '{recordings_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields='files(id)',
            spaces='drive'
        ).execute()

        if len(results['files']) > 0:
            folder_id = results['files'][0]['id']
        else:
            file_metadata = {
                'name': folder_name, # Use sanitized name
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [recordings_folder_id]
            }
            folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            folder_id = folder['id']
        # --- End Get or Create Specific Recording Topic Folder ---


        # --- Process Recording Files (Download and Upload) ---
        for file_info in recording.get('recording_files', []): # Use .get for safety
            start_time_str = recording.get('start_time')
            # Use original topic for filename basis, but sanitize parts of it
            original_topic = recording.get('topic', 'Untitled Recording')
            filename_base = "".join(c if c.isalnum() or c in (' ', '_', '-') else '_' for c in original_topic)
            filename_base = filename_base.strip()
            if not filename_base:
                filename_base = "Untitled_Recording"

            date_string = "unknown_date"
            if start_time_str:
                try:
                    start_datetime = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%SZ")
                    date_string = start_datetime.strftime("%Y-%m-%d_%H-%M-%S")
                except ValueError:
                    print(f"Warning: Could not parse start_time: {start_time_str}")

            # Construct filename - ensuring it's safe
            video_filename = f"{filename_base}_{date_string}.mp4"
            # Further sanitize filename (though Drive is more permissive than local filesystems)
            video_filename = video_filename.replace("/", "_").replace("\\", "_") # Replace slashes


            download_url = file_info.get('download_url')
            status = file_info.get('status')
            file_extension = file_info.get('file_extension')
            duration = recording.get('duration', 0)

            # Check conditions for processing this file
            if status == 'completed' and file_extension == 'MP4' and duration >= 10 and download_url:
                try:
                    # Check if file already exists in Drive (use sanitized filename)
                    # Add trashed=false to avoid matching deleted files
                    query = f"name='{video_filename}' and '{folder_id}' in parents and trashed=false"
                    existing_files = drive_service.files().list(
                        q=query,
                        fields='files(id)',
                        spaces='drive'
                    ).execute()

                    if len(existing_files['files']) > 0:
                        print(f"Skipping upload of '{video_filename}' as it already exists.")
                        continue # Skip to next file in this recording

                    # Download the video
                    print(f"Downloading: {video_filename} from {download_url[:50]}...") # Log start
                    response = requests.get(download_url, stream=True, timeout=300) # Add timeout
                    response.raise_for_status()

                    video_content = io.BytesIO()
                    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024): # Increased chunk size to 8MB
                        if chunk:
                            video_content.write(chunk)
                    video_content.seek(0)
                    print(f"Download complete. Uploading: {video_filename} to Drive folder ID: {folder_id}") # Log before upload

                    # Upload the video to Google Drive
                    file_metadata = {
                        'name': video_filename, # Use sanitized filename
                        'parents': [folder_id]
                    }
                    media = MediaIoBaseUpload(
                        video_content,
                        mimetype='video/mp4',
                        resumable=True,
                        chunksize=8 * 1024 * 1024 # Use consistent chunk size
                    )

                    request = drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id'
                    )

                    # Execute upload with progress (optional but good for logs)
                    upload_response = None
                    while upload_response is None:
                        status, upload_response = request.next_chunk()
                        if status:
                            print(f"Uploaded {int(status.progress() * 100)}% of {video_filename}")
                    print(f"Upload complete for {video_filename}, File ID: {upload_response.get('id')}")


                except (requests.exceptions.RequestException, ConnectionError, ChunkedEncodingError) as e:
                    # More specific error handling for network/download issues
                    print(f"Network/Download error for {video_filename}: {str(e)}. Retrying task.")
                    # Use exponential backoff for retries
                    retry_delay = 10 * (self.request.retries + 1)
                    self.retry(exc=e, countdown=retry_delay)

                except Exception as e: # Catch other potential errors during file processing
                    print(f"Error processing file {video_filename}: {str(e)}. Skipping file, but task continues.")
                    # Decide if this error warrants retrying the whole task or just skipping the file
                    # For now, just log and continue to the next file. Add retry logic if needed.
                    # Consider self.retry(exc=e, countdown=...) if file errors should retry the task.
            # --- End Process Recording Files ---

    except Exception as e:
        # Catch errors related to credentials, Drive API setup, folder creation etc.
        print(f"An error occurred processing recording task for topic '{recording.get('topic', 'N/A')}': {str(e)}")
        # Use exponential backoff for retries on general task errors
        retry_delay = 10 * (self.request.retries + 1)
        try:
            # Retry the task if retries are left
            self.retry(exc=e, countdown=retry_delay)
        except self.MaxRetriesExceededError:
            print(f"Max retries exceeded for recording task (topic: '{recording.get('topic', 'N/A')}')")
        except Exception as retry_e: # Catch potential errors during retry itself
             print(f"Error attempting to retry task: {retry_e}")


def share_folder_with_email(drive_service, folder_name, email, recordings_folder_id):
    # Ensure folder_name is the sanitized version
    # Add trashed=false to queries
    try:
        # Check if the folder already exists within "Automated Zoom Recordings"
        query = f"name='{folder_name}' and '{recordings_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields='files(id, webViewLink)',
            spaces='drive'
        ).execute()

        if len(results['files']) > 0:
            folder_id = results['files'][0]['id']
            folder_web_view_link = results['files'][0].get('webViewLink', '') # Use .get for safety
        else:
            # Create the folder within "Automated Zoom Recordings" if it doesn't exist
            print(f"Creating folder '{folder_name}' for sharing with {email}")
            file_metadata = {
                'name': folder_name, # Use sanitized name
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [recordings_folder_id]
            }
            folder = drive_service.files().create(body=file_metadata, fields='id, webViewLink').execute()
            folder_id = folder['id']
            folder_web_view_link = folder.get('webViewLink', '') # Use .get for safety

        # Share the folder with the email
        print(f"Sharing folder '{folder_name}' (ID: {folder_id}) with {email}")
        permission_metadata = {
            'type': 'user',
            'role': 'writer', # Consider 'viewer' if write access isn't needed
            'emailAddress': email
        }
        drive_service.permissions().create(
            fileId=folder_id,
            body=permission_metadata,
            fields='id',
            sendNotificationEmail=False # Optional: set to True to notify user
        ).execute()

        return folder_web_view_link
    except Exception as e:
        print(f"Error sharing folder '{folder_name}' with {email}: {e}")
        return None # Return None to indicate failure

# Remove the old uploadFiles function if it's no longer needed or keep if used elsewhere non-asynchronously
# For now, let's assume it's replaced by the Celery task. If direct calls are needed, keep a separate non-task function.
