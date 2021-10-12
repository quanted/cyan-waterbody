#####################################################################################
# Defines Celery tasks for cyan-waterbody (reports and eventually image aggregation)
#####################################################################################
from time import sleep
import os
import logging
from celery import Celery
import uuid

from flaskr.report import generate_report, get_report_path


redis_hostname = os.environ.get("REDIS_HOSTNAME", "localhost")
redis_port = os.environ.get("REDIS_PORT", 6379)

logging.info("REDIS_HOSTNAME: {}".format(redis_hostname))
logging.info("REDIS_PORT: {}".format(redis_port))

celery_instance = Celery(
    "tasks",
    broker="redis://{}:{}/0".format(redis_hostname, redis_port),
    backend="redis://{}:{}/0".format(redis_hostname, redis_port),
)

celery_instance.conf.update(
    CELERY_BROKER_URL="redis://{}:{}/0".format(redis_hostname, redis_port),
    CELERY_RESULT_BACKEND="redis://{}:{}/0".format(redis_hostname, redis_port),
    CELERY_ACCEPT_CONTENT=["json"],
    CELERY_TASK_SERIALIZER="json",
    CELERY_RESULT_SERIALIZER="json",
    CELERY_IGNORE_RESULT=False,
    CELERY_TRACK_STARTED=True,
    CELERYD_MAX_MEMORY_PER_CHILD=50000000,
)


@celery_instance.task(bind=True)
def generate_report(self, request_obj):
    # print("Generating report on celery task.")
    # sleep(5)
    # print("Report 'generated'.")
    # return {"status": "report data goes here"}

    logging.warning("GENERATE REPORT CELERY TASK CALLED: {}".format(request_obj))

    response = generate_report(request_obj)

    logging.warning("GEN REPORT CELERY TASK RESPONSE: {}".format(response))

    return response


@celery_instance.task(bind=True)
def test_celery(*args):
    logging.warning("Testing celery: {}".format(args))
    sleep(5)
    logging.warning("Celery successfully called.")
    return {"status": "celery task finished."}


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
        # th = threading.Thread(target=generate_report, kwargs={'year': year, 'day': day, 'objectids': objectids,
        #                                                       'tribes': tribes, 'counties': county, 'ranges': ranges,
        #                                                       'report_id': report_id})

        logging.warning("Starting task, request: {}".format(request_obj))

        # job_id = str(uuid.uuid4())  # creates job ID for celery task
        job_id = request_obj['report_id']

        # Runs job on celery worker:
        celery_job = generate_report.apply_async(
            args=[request_obj], queue="celery", task_id=job_id
        )

        return celery_job
