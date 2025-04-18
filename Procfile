web: gunicorn main:app --timeout 120 --workers 2 --threads 4
worker: celery -A tasks worker --loglevel=info --concurrency=2 --timeout 3600
