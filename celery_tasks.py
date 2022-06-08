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

from flaskr import report
import requests


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

        # TODO: Make request to cyanweb flask to update report table with
        # and error/failed state.

        cyanweb_request_obj = {
            "report_id": request_obj["report_id"],
            "report_status": "FAILURE",
            "finished_datetime": datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
        }

        response = make_update_report_request(
            cyanweb_request_obj, 
            token,
            origin,
            app_name
        )

        return json.loads(response)


    # Updates report status
    cyanweb_request_obj = {
        "report_id": report_response,
        "report_status": "SUCCESS",
        "finished_datetime": datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
    }

    response = make_update_report_request(
        cyanweb_request_obj, 
        token,
        origin,
        app_name
    )

    return json.loads(response)


@celery_instance.task(bind=True)
def test_celery(*args):
    logging.warning("Testing celery: {}".format(args))
    sleep(5)
    logging.warning("Celery successfully called.")
    return {"status": "celery task finished."}


def make_update_report_request(request_obj, token, origin, app_name):
    """
    Makes request to cyanweb flask to update report table for
    a user's report.
    """
    url = origin + "/cyan/app/api/report/update"

    # NOTE: For troubleshooting only:
    # url = "http://host.docker.internal:5001/cyan/app/api/report/update"

    headers = {
        "Access-Control-Expose-Headers": "Authorization",
        "Access-Control-Allow-Headers": "Authorization",
        "Authorization": "{}".format(token),
        "Origin": origin,
        "App-Name": app_name
    }
    response = requests.post(url=url, headers=headers, json=request_obj)
    return response.content


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
