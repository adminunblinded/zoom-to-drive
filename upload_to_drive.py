from flask import Flask, redirect, request, Blueprint, jsonify
from google_auth_oauthlib.flow import Flow
import pickle
import os
import redis
import json
import requests
import logging
import time
import importlib.metadata
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import ConnectionError, ChunkedEncodingError, Timeout

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Patch for Celery EntryPoints issue in Python 3.10+
def patch_celery_entry_points():
    try:
        from celery.utils import imports
        
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
                
        # Replace the original function
        imports.load_extension_class_names = patched_load_extension_class_names
        logger.info("Successfully applied EntryPoints patch for Celery in upload_to_drive.py")
        
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to apply EntryPoints patch: {e}")

# Apply the patch before importing Celery-dependent modules
patch_celery_entry_points()

# Import modules that depend on Celery after the patch
from download import download_zoom_recordings
from tasks import setup_folders

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

# Callback URL - use environment variable or default
CALLBACK_URL = os.environ.get(
    'CALLBACK_URL', 
    'https://flask-production-0cd3.up.railway.app/upload_callback'
)

# Create the Flow instance
flow = Flow.from_client_secrets_file(
    CLIENT_SECRETS_FILE,
    scopes=SCOPES,
    redirect_uri=CALLBACK_URL
)

# Create a Redis client instance with timeout
redis_url = os.environ.get('REDIS_URL', 'redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379')
redis_client = redis.from_url(redis_url, socket_timeout=10)

def store_parameters(accountName, email):
    """Store account parameters in Redis"""
    if not accountName or not email:
        logger.warning("Invalid parameters: accountName and email are required")
        return
    
    logger.info(f"Storing parameters for account: {accountName}")
    
    # Get existing parameters
    try:
        stored_params_json = redis_client.get("stored_params")
        if stored_params_json:
            stored_params = json.loads(stored_params_json)
        else:
            stored_params = {}
        
        # Merge the new parameters with the existing ones
        stored_params[accountName] = email
        
        # Store the updated parameters in the database
        redis_client.set("stored_params", json.dumps(stored_params))
        logger.info(f"Successfully stored parameters for account: {accountName}")
    except Exception as e:
        logger.error(f"Error storing parameters: {str(e)}")
        raise

def retrieve_parameters():
    """Retrieve account parameters from Redis"""
    try:
        stored_params_json = redis_client.get("stored_params")
        if stored_params_json:
            return json.loads(stored_params_json)
        return {}
    except Exception as e:
        logger.error(f"Error retrieving parameters: {str(e)}")
        return {}

@upload_blueprint.route('/')
def index():
    """Main upload route - starts the Zoom to Drive transfer process"""
    logger.info("Upload process initiated")
    
    access_token = redis_client.get('google_access_token')
    if access_token:
        try:
            # Get recordings from Zoom
            logger.info("Fetching Zoom recordings")
            recordings = download_zoom_recordings()
            if not recordings:
                logger.warning("No recordings found to upload")
                return "No recordings found to upload"
                
            serialized_credentials = redis_client.get('credentials')
            if not serialized_credentials:
                logger.error("Google credentials not found")
                return "Google credentials not found, please authenticate again"
            
            # Verify credentials are valid before proceeding
            try:
                credentials = pickle.loads(serialized_credentials)
                if credentials.expired and credentials.refresh_token:
                    logger.info("Refreshing expired Google credentials")
                    refresh_google_token()
                    serialized_credentials = redis_client.get('credentials')
                    if not serialized_credentials:
                        logger.error("Failed to refresh Google credentials")
                        return "Failed to refresh Google credentials, please authenticate again"
            except Exception as e:
                logger.error(f"Error checking Google credentials: {str(e)}", exc_info=True)
                return jsonify({
                    'status': 'error',
                    'message': f"Invalid Google credentials: {str(e)}"
                })
                
            # Start the folder setup and processing pipeline
            logger.info(f"Starting task with {len(recordings)} recordings, first recording topic: {recordings[0].get('topic', 'Unknown') if recordings else 'None'}")
            task = setup_folders.delay(serialized_credentials, recordings)
            task_id = task.id
            
            # Store task info in Redis for status tracking
            task_info = {
                'task_id': task_id,
                'status': 'STARTED',
                'recordings_count': len(recordings),
                'start_time': datetime.now().isoformat(),
            }
            redis_client.set(f"task:{task_id}", json.dumps(task_info))
            
            logger.info(f"Started processing with task ID: {task_id} for {len(recordings)} recordings")
            return jsonify({
                'status': 'success', 
                'message': f"Processing started with task ID: {task_id}",
                'task_id': task_id,
                'recordings_count': len(recordings)
            })
            
        except Exception as e:
            logger.error(f"Error starting upload process: {str(e)}", exc_info=True)
            return jsonify({
                'status': 'error',
                'message': f"Error starting upload: {str(e)}"
            })
    else:
        # Need to authenticate with Google first
        logger.info("Google authentication required - redirecting to OAuth flow")
        try:
            authorization_url, state = flow.authorization_url(
                access_type='offline',
                include_granted_scopes='true',
                prompt='consent'
            )
            # Store the state in Redis
            redis_client.set('oauth_state', state)
            logger.info("Redirecting to Google OAuth")
            return redirect(authorization_url)
        except Exception as e:
            logger.error(f"Error during OAuth initialization: {str(e)}", exc_info=True)
            return jsonify({
                'status': 'error',
                'message': f"Authentication error: {str(e)}"
            })

@upload_blueprint.route('/upload_callback')
def upload_callback():
    """Handle the OAuth callback from Google"""
    logger.info("Received OAuth callback from Google")
    try:
        # Retrieve the stored state from Redis
        stored_state = redis_client.get('oauth_state')
        if not stored_state:
            logger.error("OAuth state parameter missing")
            return "Authorization failed: OAuth state parameter missing"
            
        stored_state = stored_state.decode('utf-8')
        
        # Complete the OAuth flow
        flow.fetch_token(
            authorization_response=request.url,
            state=stored_state
        )
        
        # Get credentials
        credentials = flow.credentials
        refresh_token = credentials.refresh_token
        
        # Store credentials in Redis
        serialized_credentials = pickle.dumps(credentials)
        redis_client.set('credentials', serialized_credentials)
        redis_client.set('google_access_token', credentials.token)
        redis_client.set('google_refresh_token', refresh_token)
        
        logger.info("Successfully authenticated with Google")
        
        try:
            # Download and process recordings
            recordings = download_zoom_recordings()
            if not recordings:
                logger.warning("No recordings found to upload after authentication")
                return "Authentication successful but no recordings found to upload"
            
            # Start processing
            task = setup_folders.delay(serialized_credentials, recordings)
            task_id = task.id
            
            # Store task info
            task_info = {
                'task_id': task_id,
                'status': 'STARTED',
                'recordings_count': len(recordings),
                'start_time': datetime.now().isoformat(),
            }
            redis_client.set(f"task:{task_id}", json.dumps(task_info))
            
            logger.info(f"Started processing with task ID: {task_id} for {len(recordings)} recordings")
            return f"Authentication successful. Processing {len(recordings)} recordings with task ID: {task_id}"
            
        except Exception as e:
            logger.error(f"Error starting processing after authentication: {str(e)}", exc_info=True)
            return f"Authentication successful but error starting upload: {str(e)}"
            
    except Exception as e:
        logger.error(f"OAuth callback error: {str(e)}", exc_info=True)
        return f"OAuth error: {str(e)}"

@upload_blueprint.route('/status/<task_id>')
def task_status(task_id):
    """Check the status of a processing task"""
    try:
        task_info_json = redis_client.get(f"task:{task_id}")
        if task_info_json:
            task_info = json.loads(task_info_json)
            return jsonify(task_info)
        else:
            # Try to get info from Celery
            from tasks import celery
            task = celery.AsyncResult(task_id)
            if task.state:
                return jsonify({
                    'task_id': task_id,
                    'status': task.state,
                    'info': str(task.info) if task.info else None
                })
            return jsonify({'status': 'not_found'})
    except Exception as e:
        logger.error(f"Error checking task status: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@retry(
    retry=retry_if_exception_type((ConnectionError, ChunkedEncodingError, Timeout)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60)
)
def refresh_google_token():
    """Refresh the Google access token with retries"""
    try:
        refresh_token = redis_client.get('google_refresh_token')
        if not refresh_token:
            logger.error("No refresh token found")
            return False
            
        refresh_token = refresh_token.decode('utf-8')
        token_url = 'https://oauth2.googleapis.com/token'
        
        with open(CLIENT_SECRETS_FILE, 'r') as secrets_file:
            client_secrets = json.load(secrets_file)
        
        token_params = {
            'client_id': client_secrets['web']['client_id'],
            'client_secret': client_secrets['web']['client_secret'],
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token
        }
        
        response = requests.post(token_url, data=token_params, timeout=30)
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data['access_token']
            redis_client.set('google_access_token', access_token)
            logger.info("Successfully refreshed Google access token")
            return True
        else:
            logger.error(f"Failed to refresh token: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error refreshing token: {str(e)}")
        raise

@upload_blueprint.route('/refresh_token')
def refresh_token_route():
    """Manual route to refresh the Google access token"""
    try:
        success = refresh_google_token()
        if success:
            return jsonify({'status': 'success', 'message': 'Token refreshed successfully'})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to refresh token'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})
