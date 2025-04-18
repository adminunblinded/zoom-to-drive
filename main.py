from flask import Flask, Blueprint, session, request, jsonify
import importlib.metadata
import redis
import json
import logging
import os
import sys
import threading
import time
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

# Set environment variable for app start time
os.environ['APP_STARTUP_TIME'] = datetime.now().isoformat()

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

def start_celery_worker():
    """Start a Celery worker in a separate thread"""
    logger.info("Starting Celery worker thread...")
    from tasks import celery
    
    # Configure worker
    worker_args = [
        'worker',
        '--loglevel=INFO',
        '--concurrency=2',
        '-Q', 'setup,upload,process',  # Listen to all queues
        '--without-heartbeat',
        '--without-gossip',
        '--without-mingle'
    ]
    
    try:
        # Monkey patch the worker to run in a thread
        from threading import Thread
        
        class CeleryWorkerThread(Thread):
            def run(self):
                try:
                    logger.info("Celery worker thread started")
                    celery.worker_main(worker_args)
                except Exception as e:
                    logger.error(f"Celery worker thread error: {str(e)}", exc_info=True)
        
        # Start the worker thread
        worker_thread = CeleryWorkerThread()
        worker_thread.daemon = True  # Allow the thread to exit when the main program exits
        worker_thread.start()
        logger.info("Celery worker thread initialized")
        
        # Set a flag to indicate the worker is running
        app.config['CELERY_WORKER_RUNNING'] = True
        
    except Exception as e:
        logger.error(f"Failed to start Celery worker thread: {str(e)}", exc_info=True)
        app.config['CELERY_WORKER_RUNNING'] = False

# Check command line arguments for worker option
if __name__ == '__main__':
    # Check if we should start a worker
    if len(sys.argv) > 1 and sys.argv[1] == '--with-worker':
        # Start a Celery worker in a separate thread
        start_celery_worker()
        logger.info("Application started with embedded Celery worker")
    else:
        logger.info("Application started without embedded Celery worker")
        
    # Start the Flask app
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
else:
    # When running under gunicorn, check for the ENABLE_WORKER env var
    if os.environ.get('ENABLE_WORKER', 'false').lower() == 'true':
        # Start a Celery worker in a separate thread with a delay
        def delayed_worker_start():
            time.sleep(5)  # Wait for the app to fully initialize
            start_celery_worker()
            
        threading.Thread(target=delayed_worker_start, daemon=True).start()
        logger.info("Application initialized with delayed Celery worker start")
    else:
        logger.info("Application initialized without embedded Celery worker")

@app.route('/worker/start', methods=['POST'])
def start_worker():
    """API route to start a Celery worker"""
    if app.config.get('CELERY_WORKER_RUNNING', False):
        return jsonify({"status": "already_running", "message": "Celery worker is already running"})
    
    try:
        start_celery_worker()
        return jsonify({"status": "success", "message": "Celery worker started"})
    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to start Celery worker: {str(e)}"})

@app.route('/worker/status', methods=['GET'])
def worker_status():
    """API route to check Celery worker status"""
    is_running = app.config.get('CELERY_WORKER_RUNNING', False)
    
    # If we think it's running, verify by pinging
    if is_running:
        try:
            from tasks import celery
            ping_result = celery.control.ping(timeout=2.0)
            is_responding = bool(ping_result)
        except Exception:
            is_responding = False
    else:
        is_responding = False
    
    return jsonify({
        "running": is_running,
        "responding": is_responding
    })
