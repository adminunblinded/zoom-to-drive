from flask import Flask, redirect, request, Blueprint
from google_auth_oauthlib.flow import Flow
from download import download_zoom_recordings
from tasks import uploadFiles
import pickle
import os
import redis
import json
import requests

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

@upload_blueprint.route('/')
def index():
    access_token = redis_client.get('google_access_token')
    if access_token:
        recordings = download_zoom_recordings()

        serialized_credentials = redis_client.get('credentials')
        
        try:
            # Call the task directly with apply_async instead of delay
            task = uploadFiles.apply_async(args=[serialized_credentials, recordings], ignore_result=True)
            return "Recordings are being uploaded"
        except Exception as e:
            # Log the exception and return an error message
            print(f"Error starting upload task: {str(e)}")
            return f"Error starting upload: {str(e)}"
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
    
    response = requests.post(token_url, data=token_params)
    if response.status_code == 200:
        new_credentials = response.json()
        new_access_token = new_credentials['access_token']

        # Store the new access token in Redis
        redis_client.set('google_access_token', new_access_token)

        # Update the existing credentials with the new access token
        credentials.token = new_access_token

        recordings = download_zoom_recordings()
        serialized_credentials = pickle.dumps(credentials)
        redis_client.set('credentials', serialized_credentials)

        try:
            # Call the task directly with apply_async instead of delay
            task = uploadFiles.apply_async(args=[serialized_credentials, recordings], ignore_result=True)
            return "Recordings are being uploaded"
        except Exception as e:
            # Log the exception and return an error message
            print(f"Error starting upload task: {str(e)}")
            return f"Error starting upload: {str(e)}"
    else:
        return "Failed to refresh access token"
