from wb_flask import app
from celery_tasks import celery_instance as celery
from scheduled_tasks.agg_scheduler import AggScheduler


app.app_context().push()

agg_scheduler = AggScheduler()  # initiates scheduled aggregation task

# NOTE: to run worker in local dev in windows:
# celery -A celery_worker.celery worker --pool=solo -l INFO
