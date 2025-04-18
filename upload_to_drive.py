from flask import Flask, redirect, request, Blueprint
from google_auth_oauthlib.flow import Flow
from download import download_zoom_recordings
from tasks import uploadFiles
import pickle
import os
import redis
import json
import requests
import traceback

upload_blueprint = Blueprint('upload', __name__)
upload_blueprint.secret_key = '@unblinded2018'

os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
stored_params = {}

# Google OAuth 2.0 configuration
CLIENT_SECRETS_FILE = 'client_secrets.json'
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile'
]

# Create the Flow instance
flow = Flow.from_client_secrets_file(
    CLIENT_SECRETS_FILE,
    scopes=SCOPES,
    redirect_uri='https://flask-production-0cd3.up.railway.app/upload_callback'  # Replace with your domain
)

# Create a Redis client instance
redis_url = 'redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379'
redis_client = redis.from_url(redis_url)

def store_parameters(accountName, email):
    global stored_params
    stored_params = {accountName : email}
    stored_params_json = redis_client.get("stored_params")
    if stored_params_json:
        stored_params = json.loads(stored_params_json)
    
    # Merge the new parameters with the existing ones
    stored_params[accountName] = email
    
    # Store the updated parameters in the database
    redis_client.set("stored_params", json.dumps(stored_params))

def retrieve_parameters():
    global stored_params
    return stored_params

def use_failsafe_if_needed(credentials, recordings, batch=None):
    """Helper function to try the main task first and fallback to the failsafe if needed"""
    try:
        # Try the standard task first
        from tasks import uploadFiles
        if batch is not None:
            return uploadFiles.delay(credentials, batch)
        else:
            return uploadFiles.delay(credentials, recordings)
    except AttributeError as attr_err:
        # If there's an EntryPoints error, try the failsafe implementation
        if "'EntryPoints' object has no attribute" in str(attr_err):
            print(f"EntryPoints error detected, using failsafe task implementation")
            from tasks import upload_safe
            if batch is not None:
                return upload_safe.delay(credentials, batch)
            else:
                return upload_safe.delay(credentials, recordings)
        else:
            # Re-raise other attribute errors
            raise

@upload_blueprint.route('/')
def index():
    access_token = redis_client.get('google_access_token')
    if access_token:
        try:
            # Download recordings but with a timeout
            recordings = download_zoom_recordings()
            
            if not recordings:
                return "No recordings found or error retrieving recordings"
                
            total_recordings = len(recordings)
            if total_recordings > 0:
                serialized_credentials = redis_client.get('credentials')
                
                # Limit batch size for large datasets
                max_batch_size = 20
                if total_recordings > max_batch_size:
                    # Process larger datasets in multiple batches
                    try:
                        # Start processing the first batch immediately
                        first_batch = recordings[:max_batch_size]
                        use_failsafe_if_needed(serialized_credentials, recordings, first_batch)
                        
                        # Queue the remaining batches
                        for i in range(max_batch_size, total_recordings, max_batch_size):
                            batch = recordings[i:min(i+max_batch_size, total_recordings)]
                            use_failsafe_if_needed(serialized_credentials, recordings, batch)
                            
                        return f"Processing {total_recordings} recordings in multiple batches"
                    except Exception as e:
                        error_details = f"Error starting batch upload tasks: {str(e)}\n{traceback.format_exc()}"
                        print(error_details)
                        return f"Error starting batch uploads: {str(e)}"
                else:
                    try:
                        # For smaller datasets, use a single task
                        use_failsafe_if_needed(serialized_credentials, recordings)
                        return f"Processing {total_recordings} recordings"
                    except Exception as e:
                        error_details = f"Error starting upload task: {str(e)}\n{traceback.format_exc()}"
                        print(error_details)
                        return f"Error starting upload: {str(e)}"
            else:
                return "No recordings found to process"
        except Exception as e:
            error_details = f"Error in upload process: {str(e)}\n{traceback.format_exc()}"
            print(error_details)
            return f"Error in upload process: {str(e)}"
    else:
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'
        )
        # Store the state in Redis
        redis_client.set('oauth_state', state)
        return redirect(authorization_url)

@upload_blueprint.route('/upload_callback')
def upload_callback():
    try:
        # Retrieve the stored state from Redis
        stored_state = redis_client.get('oauth_state')
        if stored_state:
            stored_state = stored_state.decode('utf-8')
            # Pass the state to fetch_token
            flow.fetch_token(
                authorization_response=request.url,
                state=stored_state
            )
        else:
            # Handle the case where state is missing
            return "Authorization failed: OAuth state parameter missing"

        # Refresh the access token
        credentials = flow.credentials
        refresh_token = credentials.refresh_token
        token_url = 'https://oauth2.googleapis.com/token'
        
        with open(CLIENT_SECRETS_FILE, 'r') as secrets_file:
            client_secrets = json.load(secrets_file)
        
        token_params = {
            'client_id': client_secrets['web']['client_id'],
            'client_secret': client_secrets['web']['client_secret'],
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token
        }
        
        try:
            response = requests.post(token_url, data=token_params, timeout=30)
            if response.status_code == 200:
                new_credentials = response.json()
                new_access_token = new_credentials['access_token']

                # Store the new access token in Redis
                redis_client.set('google_access_token', new_access_token)

                # Update the existing credentials with the new access token
                credentials.token = new_access_token

                recordings = download_zoom_recordings()
                
                if not recordings or len(recordings) == 0:
                    return "No recordings found or error retrieving recordings"
                    
                serialized_credentials = pickle.dumps(credentials)
                redis_client.set('credentials', serialized_credentials)

                # Use Celery task for background processing
                try:
                    use_failsafe_if_needed(serialized_credentials, recordings)
                    return f"Authorization successful. Processing {len(recordings)} recordings in the background."
                except Exception as e:
                    error_details = f"Error starting upload task in callback: {str(e)}\n{traceback.format_exc()}"
                    print(error_details)
                    return f"Authorization successful but error starting upload: {str(e)}"
            else:
                error_msg = f"Failed to refresh access token: {response.text}"
                print(error_msg)
                return error_msg
        except Exception as e:
            error_msg = f"Error during token refresh: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            return error_msg
    except Exception as e:
        error_details = f"Unexpected error in upload callback: {str(e)}\n{traceback.format_exc()}"
        print(error_details)
        return f"Unexpected error in authorization process: {str(e)}"
