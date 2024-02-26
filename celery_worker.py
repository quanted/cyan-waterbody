from wb_flask import app
from celery_tasks import celery_instance as celery
from scheduled_tasks.main_scheduler import MainScheduler

app.app_context().push()

main_scheduler = MainScheduler()  # initiates scheduler

# NOTE: to run worker in local dev in windows:
# celery -A celery_worker.celery worker --pool=solo -l INFO
