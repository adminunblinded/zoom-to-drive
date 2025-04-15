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

# Monkey patch for EntryPoints compatibility issue in Python 3.12
def patch_entry_points():
    try:
        import pkg_resources
        if not hasattr(importlib.metadata.EntryPoints, 'get'):
            old_entry_points = importlib.metadata.entry_points

            def entry_points_with_get():
                eps = old_entry_points()
                if hasattr(eps, '_from_text_for'):  # python >= 3.10
                    return _PatchedEntryPoints(eps)
                return eps  # older python versions

            class _PatchedEntryPoints:
                def __init__(self, entry_points):
                    self._entry_points = entry_points
                
                def __iter__(self):
                    return iter(self._entry_points)
                
                def get(self, group, name=None):
                    if name is None:
                        return [ep for ep in self._entry_points if ep.group == group]
                    return next((ep for ep in self._entry_points if ep.group == group and ep.name == name), None)
                
                def select(self, **kwargs):
                    return self._entry_points.select(**kwargs)
            
            importlib.metadata.entry_points = entry_points_with_get
    except (ImportError, AttributeError):
        pass

# Apply the patch before Celery is initialized
patch_entry_points()

redis_url = 'redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379'

# Initialize Celery with explicit broker and backend configuration
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
    worker_proc_alive_timeout=60.0,
    task_ignore_result=False,
    task_track_started=True,
    task_time_limit=3600,
    worker_hijack_root_logger=False
)

redis_client = redis.from_url(redis_url)


@celery.task(bind=True, max_retries=3)
def uploadFiles(self, serialized_credentials, recordings):
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

            for files in recording['recording_files']:
                start_time = recording['start_time']
                start_datetime = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
                date_string = start_datetime.strftime("%Y-%m-%d_%H-%M-%S")  # Updated format
                video_filename = f"{topics}_{date_string}.mp4"
                video_filename = video_filename.replace("'", "\\'")  # Escape single quotation mark
                download_url = files['download_url']
                
                if files['status'] == 'completed' and files['file_extension'] == 'MP4' and recording['duration'] >= 10:
                    try:
                        response = requests.get(download_url, stream=True)
                        response.raise_for_status()

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
                            continue

                        # Capture the video content in a BytesIO object
                        video_content = io.BytesIO()
                        for chunk in response.iter_content(chunk_size=2048 * 2048):  # Chunks of 2MB
                            if chunk:
                                video_content.write(chunk)

                        # Reset the file pointer to the beginning of the content
                        video_content.seek(0)

                        # Upload the video to the folder in Google Drive
                        file_metadata = {
                            'name': video_filename,
                            'parents': [folder_id]
                        }
                        media = MediaIoBaseUpload(video_content, mimetype='video/mp4', resumable=True)

                        drive_service.files().create(
                            body=file_metadata,
                            media_body=media,
                            fields='id'
                        ).execute()

                    except (ConnectionError, ChunkedEncodingError) as e:
                        print(f"Error occurred while downloading or uploading recording: {str(e)}")
                        self.retry(countdown=10)  # Retry after 10 seconds

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
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
