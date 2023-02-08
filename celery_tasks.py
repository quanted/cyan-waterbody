#####################################################################################
# Defines Celery tasks for cyan-waterbody (reports and eventually image aggregation)
#####################################################################################
from time import sleep
import os
import logging
from celery import Celery
import uuid
from datetime import datetime
import json
import requests

from flaskr import report
from main import async_aggregate



redis_hostname = os.environ.get("REDIS_HOSTNAME", "localhost")
redis_port = os.environ.get("REDIS_PORT", 6379)

logging.info("REDIS_HOSTNAME: {}".format(redis_hostname))
logging.info("REDIS_PORT: {}".format(redis_port))

celery_instance = Celery(
    "tasks",
    broker="redis://{}:{}/0".format(redis_hostname, redis_port),
    backend="redis://{}:{}/0".format(redis_hostname, redis_port),
)

# TODO: Update parameters for recent versions of Celery (throws warnings):
celery_instance.conf.update(
    broker_url="redis://{}:{}/0".format(redis_hostname, redis_port),
    result_backend="redis://{}:{}/0".format(redis_hostname, redis_port),
    accept_content=["json"],
    task_serializer="json",
    result_serializer="json",
    task_ignore_result=False,
    task_track_started=True,
    worker_max_tasks_per_child=50000000,
)


@celery_instance.task(bind=True)
def generate_report(self, request_obj):

    token = request_obj.pop('token')
    origin = request_obj.pop('origin')
    app_name = request_obj.pop('app_name')

    response = None
    try:
        report_response = report.generate_report(**request_obj)  # returns report id
    except Exception as e:
        logging.warning("Exception generating report: {}".format(e))


@celery_instance.task(bind=True)
def test_celery(*args):
    logging.warning("Testing celery: {}".format(args))
    sleep(5)
    logging.warning("Celery successfully called.")
    return {"status": "celery task finished."}


@celery_instance.task(bind=True)
def run_aggregation(self, year, day, daily):
    async_aggregate(year, day, daily)


class CeleryHandler:

    def __init__(self):
        self.states = [
            "FAILURE",
            "REVOKED",
            "RETRY",
            "PENDING",
            "RECEIVED",
            "STARTED",
            "SUCCESS",
        ]
        self.pending_states = ["RETRY", "PENDING", "RECEIVED", "STARTED"]
        self.fail_states = ["FAILURE", "REVOKED"]
        # self.cyano_request_timeout = 30  # seconds

    def test_celery(self):
        logging.warning("CALLING CELERY TASK")
        celery_job = test_celery.apply_async(queue="celery")
        return {"status": "test celery called"}

    def start_task(self, request_obj):
        """
        Starts celery task and saves job/task ID to job table.
        """
        task_id = request_obj['report_id']
        # Runs job on celery worker:
        celery_job = generate_report.apply_async(
            args=[request_obj], queue="celery", task_id=task_id
        )
        return celery_job

    def check_celery_job_status(self, report_id):
        """
        Checks the status of a celery job and returns
        its status.
        Celery States: FAILURE, PENDING, RECEIVED, RETRY,
        REVOKED, STARTED, SUCCESS
        """
        task = celery_instance.AsyncResult(report_id)
        return task.status

    def revoke_task(self, report_id):
        """
        Revokes/cancels a celery task.
        """
        try:
            result = celery_instance.AsyncResult(report_id).revoke()
            logging.warning("Task '{}' revoked: {}".format(report_id, result))
            return {"status": "Job canceled"}
        except Exception as e:
            logging.error("revoke_task error: {}".format(e))
            return {"status": "Failed to cancel job"}

    def start_aggregation(self, year: int, day: int, daily: bool):
        """
        Runs aggregation on celery worker.
        """
        celery_job = run_aggregation.apply_async(
            args=[year, day, daily], queue="celery"
        )
        return celery_job
