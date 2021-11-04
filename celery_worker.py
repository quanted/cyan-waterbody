from wb_flask import app
from celery_tasks import celery_instance as celery
from scheduler import Scheduler


app.app_context().push()

scheduler = Scheduler()  # initiates scheduler

# NOTE: to run worker in local dev in windows:
# celery -A celery_worker.celery worker --pool=solo -l INFO
