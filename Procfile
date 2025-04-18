web: gunicorn main:app
worker: celery -A tasks worker --loglevel=info
