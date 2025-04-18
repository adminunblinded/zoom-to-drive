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
import time
import sys

# More comprehensive monkey patch for EntryPoints issue in Python 3.12 with Celery
def patch_celery_entry_points():
    try:
        # Direct patch for celery.utils.imports.load_extension_class_names
        from celery.utils import imports
        original_load_extension = imports.load_extension_class_names
        
        def patched_load_extension_class_names(namespace):
            try:
                # Different handling based on Python version
                if sys.version_info >= (3, 10):
                    # For Python 3.10+
                    eps = importlib.metadata.entry_points()
                    if hasattr(eps, 'select'):
                        # Python 3.10 - 3.11 style
                        eps_filtered = eps.select(group=namespace)
                        result = {ep.name: ep.value for ep in eps_filtered}
                    else:
                        # Python 3.12+ style
                        result = {}
                        if namespace in eps:
                            for ep in eps[namespace]:
                                result[ep.name] = ep.value
                else:
                    # For Python < 3.10
                    try:
                        eps = importlib.metadata.entry_points()
                        result = {}
                        if hasattr(eps, 'get'):
                            namespace_eps = eps.get(namespace, [])
                            for ep in namespace_eps:
                                result[ep.name] = ep.value
                        else:
                            # Fallback for older importlib.metadata versions
                            for ep in eps:
                                if ep.group == namespace:
                                    result[ep.name] = ep.value
                    except Exception:
                        # Legacy fallback using pkg_resources if available
                        try:
                            import pkg_resources
                            result = {}
                            for ep in pkg_resources.iter_entry_points(namespace):
                                result[ep.name] = f"{ep.module_name}:{'.'.join(ep.attrs)}"
                        except ImportError:
                            result = {}
                
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

# Add a failsafe task wrapper that doesn't depend on the entry points patch
def create_failsafe_task():
    """
    Creates a simpler task implementation that doesn't rely on complex Celery features
    to ensure at least basic functionality works even if the patch fails
    """
    @celery.task(name='tasks.upload_safe', bind=True, max_retries=3)
    def upload_safe(self, serialized_credentials, recordings):
        """Simplified version of uploadFiles that serves as a fallback"""
        print("Using failsafe upload task implementation")
        try:
            # Direct function call bypassing any complex Celery machinery
            uploadFiles(serialized_credentials, recordings)
            return "Completed failsafe upload task"
        except Exception as e:
            print(f"Error in failsafe task: {str(e)}")
            if self.request.retries < 3:
                self.retry(countdown=60, exc=e)
            else:
                raise
    return upload_safe

# Create the failsafe task
upload_safe = create_failsafe_task()

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
        
        # Process recordings in batches to prevent timeouts
        batch_size = 5
        total_recordings = len(recordings)
        
        for i in range(0, total_recordings, batch_size):
            batch = recordings[i:i+batch_size]
            
            # Process each recording in the batch
            for recording in batch:
                try:
                    process_recording(drive_service, recording, recordings_folder_id)
                except Exception as e:
                    print(f"Error processing recording {recording.get('topic', 'unknown')}: {str(e)}")
                    # Continue with next recording instead of failing the entire batch

    except Exception as e:
        print(f"An error occurred in uploadFiles: {str(e)}")
        # For Celery task, retry with exponential backoff
        if hasattr(self_or_serialized_credentials, 'retry'):
            self_or_serialized_credentials.retry(exc=e, countdown=min(2 ** self_or_serialized_credentials.request.retries, 60))

def process_recording(drive_service, recording, recordings_folder_id):
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

    for files in recording['recording_files']:
        start_time = recording['start_time']
        start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
        date_string = start_datetime.strftime("%Y-%m-%d_%H-%M-%S")  # Updated format
        video_filename = f"{topics}_{date_string}.mp4"
        video_filename = video_filename.replace("'", "\\'")  # Escape single quotation mark
        download_url = files['download_url']
        
        if files['status'] == 'completed' and files['file_extension'] == 'MP4' and recording['duration'] >= 10:
            upload_recording_file(drive_service, files, download_url, video_filename, folder_id)

def upload_recording_file(drive_service, file_info, download_url, video_filename, folder_id):
    try:
        # Check if a file with the same name already exists in the folder
        query = f"name='{video_filename}' and '{folder_id}' in parents"
        existing_files = drive_service.files().list(
            q=query,
            fields='files(id)',
            spaces='drive'
        ).execute()

        if len(existing_files['files']) > 0:
            # File with the same name already exists, skip uploading
            print(f"Skipping upload of '{video_filename}' as it already exists.")
            return

        # Use a smaller chunk size for streaming to avoid memory issues
        chunk_size = 1024 * 1024  # 1MB chunks
        
        # Add timeout to prevent hanging downloads
        response = requests.get(download_url, stream=True, timeout=(30, 300))
        response.raise_for_status()

        # Capture the video content in a BytesIO object
        video_content = io.BytesIO()
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                video_content.write(chunk)

        # Reset the file pointer to the beginning of the content
        video_content.seek(0)

        # Upload the video to the folder in Google Drive
        file_metadata = {
            'name': video_filename,
            'parents': [folder_id]
        }
        # Use resumable upload for large files
        media = MediaIoBaseUpload(
            video_content, 
            mimetype='video/mp4', 
            resumable=True,
            chunksize=chunk_size
        )

        # Upload with progress monitoring
        request = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        )
        
        response = None
        while response is None:
            try:
                # Use smaller chunk for each upload step
                status, response = request.next_chunk()
            except Exception as e:
                print(f"Error during upload for {video_filename}: {str(e)}")
                # Retry a few times before giving up
                for retry in range(5):
                    try:
                        time.sleep(2 ** retry)  # Exponential backoff
                        status, response = request.next_chunk()
                        if response:
                            break
                    except Exception as retry_e:
                        print(f"Retry {retry} failed: {str(retry_e)}")
                
                if response is None:
                    print(f"Failed to upload {video_filename} after retries")
                    return

        print(f"Successfully uploaded {video_filename}")

    except (ConnectionError, ChunkedEncodingError) as e:
        print(f"Network error while downloading or uploading recording: {str(e)}")
        raise
    except Exception as e:
        print(f"Error uploading file {video_filename}: {str(e)}")
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
