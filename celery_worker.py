from wb_flask import app
from celery_tasks import celery_instance as celery
from scheduled_tasks.agg_scheduler import AggScheduler
from scheduled_tasks.report_scheduler import ReportScheduler


app.app_context().push()

agg_scheduler = AggScheduler()  # initiates scheduled aggregation task

report_scheduler = ReportScheduler()  # initiates scheduled report generation task

# NOTE: to run worker in local dev in windows:
# celery -A celery_worker.celery worker --pool=solo -l INFO
