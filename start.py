#!/usr/bin/env python
"""
Startup script for the Zoom to Drive application
"""
import os
import logging
import sys

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)
logger = logging.getLogger("startup")

logger.info("Starting Zoom to Drive application")

try:
    # Apply gevent monkey patching before any other imports
    logger.info("Applying gevent monkey patching")
    from gevent import monkey
    monkey.patch_all()
    
    # Set the default timeout for socket operations
    import socket
    socket.setdefaulttimeout(60)
    
    # Initialize the app
    logger.info("Initializing Flask application")
    from main import app
    
    logger.info("Initialization complete")
except Exception as e:
    logger.error(f"Error during startup: {str(e)}", exc_info=True)
    sys.exit(1) 