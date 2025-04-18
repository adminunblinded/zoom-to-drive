from flask import Flask, Blueprint, session, request, jsonify
import importlib.metadata
import redis
import json
import logging
import os
from datetime import datetime

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
        logger.info("Successfully applied EntryPoints patch for Celery in main.py")
        
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to apply EntryPoints patch: {e}")

# Apply the patch before importing Celery-dependent modules
patch_celery_entry_points()

# Import modules that depend on Celery after the patch
from upload_to_drive import upload_blueprint, store_parameters, retrieve_parameters
from zoom_authorize import zoom_blueprint
import requests

redis_url = "redis://default:cZwwwfMhMjpiwoBIUoGCJrsrFBowGRrn@redis.railway.internal:6379"
redis_client = redis.from_url(redis_url)

# Create Flask app
app = Flask(__name__)
app.secret_key = '@unblinded2018'

# Register blueprints
app.register_blueprint(zoom_blueprint)
app.register_blueprint(upload_blueprint)

@app.route('/test', methods=['GET', 'POST'])
def test():
    if request.method == "GET":
        return jsonify({"response": "GET"})
    elif request.method == "POST":
        try:
            data = request.get_json(force=True)
            accountName = data.get('accountName')
            email=data.get('email')

            store_parameters(accountName,email)
            stored_folder_urls = redis_client.get("folder_urls")
            
            if stored_folder_urls is not None:
                stored_folder_urls = json.loads(stored_folder_urls)
                accountName = accountName.strip()
                share_url = stored_folder_urls.get(accountName)
            else:
                share_url="The folder hasn't been shared yet."
            
            return jsonify(share_url)
        except Exception as e:
            return jsonify({"error": str(e)})

@app.route('/debug')
def debug_info():
    """Debug route to check system status and connections"""
    try:
        # Check Redis connection
        redis_status = "OK" if redis_client.ping() else "Failed"
        
        # Get Google auth status
        google_access_token = redis_client.get('google_access_token')
        google_refresh_token = redis_client.get('google_refresh_token')
        google_auth_status = {
            "has_access_token": google_access_token is not None,
            "has_refresh_token": google_refresh_token is not None
        }
        
        # Get Zoom auth status
        zoom_token = redis_client.get('access_token')
        zoom_auth_status = {
            "has_token": zoom_token is not None
        }
        
        # Get recent task statuses
        task_keys = redis_client.keys('task:*')
        tasks = []
        for key in task_keys[:20]:  # Limit to 20 most recent tasks
            task_data = redis_client.get(key)
            if task_data:
                try:
                    task_info = json.loads(task_data)
                    task_info['key'] = key.decode('utf-8')
                    tasks.append(task_info)
                except:
                    tasks.append({"key": key.decode('utf-8'), "error": "Failed to parse task data"})
        
        # Get system info
        system_info = {
            "environment": os.environ.get('ENVIRONMENT', 'production'),
            "python_version": os.sys.version,
            "app_startup_time": os.environ.get('APP_STARTUP_TIME', 'unknown')
        }
        
        # Try to import Celery and get info
        try:
            from tasks import celery
            celery_status = {
                "broker": celery.conf.broker_url,
                "backend": celery.conf.result_backend,
                "active": True
            }
        except Exception as e:
            celery_status = {
                "error": str(e),
                "active": False
            }
        
        return jsonify({
            "timestamp": datetime.now().isoformat(),
            "redis_status": redis_status,
            "google_auth": google_auth_status,
            "zoom_auth": zoom_auth_status,
            "celery": celery_status,
            "system": system_info,
            "recent_tasks": tasks
        })
    except Exception as e:
        logger.error(f"Error in debug endpoint: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)})
