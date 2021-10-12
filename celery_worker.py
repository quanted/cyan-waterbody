from wb_flask import app
from celery_tasks import celery_instance as celery

app.app_context().push()

# Note: to run worker in local dev in windows:
# celery -A celery_worker.celery worker --pool=solo -l INFO
