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

@app.route('/uploads/status')
def uploads_status():
    """Get current status of uploads"""
    try:
        # Get successful uploads
        successful_uploads = redis_client.get("successful_uploads")
        uploads_data = {}
        if successful_uploads:
            uploads_data = json.loads(successful_uploads)
        
        # Get active tasks
        active_tasks = {}
        task_key_pattern = "task:*"
        task_keys = redis_client.keys(task_key_pattern)
        
        for key in task_keys:
            task_data = redis_client.get(key)
            if task_data:
                task_id = key.decode().split(":", 1)[1]
                active_tasks[task_id] = json.loads(task_data)
        
        # Get folder URLs
        folder_urls_data = redis_client.get("folder_urls")
        folder_urls = {}
        if folder_urls_data:
            folder_urls = json.loads(folder_urls_data)

        # Summary statistics
        upload_count = len(uploads_data)
        active_task_count = len(active_tasks)
        
        return jsonify({
            "status": "success",
            "summary": {
                "total_uploads_completed": upload_count,
                "active_tasks": active_task_count,
                "folder_count": len(folder_urls)
            },
            "uploads": uploads_data,
            "active_tasks": active_tasks,
            "folder_urls": folder_urls
        })
        
    except Exception as e:
        logger.error(f"Error getting upload status: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e)
        })

@app.route('/health')
def health_check():
    """Simple health check endpoint"""
    try:
        # Check Redis connection
        redis_client.ping()
        
        return jsonify({
            "status": "healthy",
            "redis": "connected",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500
