web: gunicorn --workers=4 --worker-class=gevent --worker-connections=1000 --timeout=120 --keep-alive=5 --log-level=info --access-logfile=- start:app
worker: celery -A tasks worker --pool=gevent --concurrency=8 --loglevel=info --max-tasks-per-child=5 --time-limit=1800 --queues=setup,upload,process,default
monitor: celery -A tasks flower --port=$PORT --basic_auth=$FLOWER_USER:$FLOWER_PASSWORD
