web: gunicorn main:app
worker: celery -A tasks worker --loglevel=info --concurrency=4 --max-tasks-per-child=10 --time-limit=1800
