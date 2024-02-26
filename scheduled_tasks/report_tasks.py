import logging
import requests
import os
import json
from json.decoder import JSONDecodeError
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime, timedelta
import time
from pytz import utc
import dateutil.relativedelta

from celery_tasks import CeleryHandler


class ReportTasks:

	def __init__(self):
		self.celery_handler = CeleryHandler()
		self.scheduler = None

	def run_scheduled_state_reports(self, year, day, parallel):
		logging.info("Running scheduled state reports.")
		self.celery_handler.start_state_reports(year=int(year), day=int(day), parallel=parallel)

	def run_scheduled_alpinelake_report(self, year, day, parallel):
		logging.info("Running scheduled alpine lake report.")
		self.celery_handler.start_alpine_reports(year=int(year), day=int(day), parallel=parallel)

	def execute_scheduled_reports(self):
		"""
		Scheduled Python task that runs state and alpine lakes reports.
		"""
		current_date = datetime.utcnow()

		print("Initiating execute_scheduled_reports()\nCurrent Date: {}".format(current_date))

		prev_month_date = current_date + dateutil.relativedelta.relativedelta(months=-1)

		print("Generating reports for the previous month: {}".format(prev_month_date))

		year = prev_month_date.year
		first_day_date = prev_month_date.replace(day=1)  # date of first day of previous month
		day = first_day_date.timetuple().tm_yday  # day of year

		self.run_scheduled_state_reports(year, day, False)  # runs state report on celery worker

		# self.run_scheduled_alpinelake_report(year, day, False)  # runs alpine lake report on celery worker
