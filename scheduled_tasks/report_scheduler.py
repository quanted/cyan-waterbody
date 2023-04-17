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


class ReportScheduler:

	def __init__(self):
		self.celery_handler = CeleryHandler()
		self.scheduler = None
		self.start_scheduler()

	def start_scheduler(self):

		# TODO: Create a scheduled task that runs first day of every month that
		# generates reports for the previous month.

		self.scheduler = BackgroundScheduler(daemon=True, timezone=utc)
		# self.scheduler.add_job(self.execute_scheduled_reports, trigger="cron", minute="*")  # testing job
		self.scheduler.add_job(self.execute_scheduled_reports, trigger="cron",
			second=0,
			minute=15,
			hour=4,
			day=1,
			month="*",
			year="*",
			day_of_week="*"
		)  # Executes 4:15AM (UTC) on 1st day of every month of every year on any day of the week
		self.scheduler.start()

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
		day = current_date.timetuple().tm_yday  # day of year

		self.run_scheduled_state_reports(year, day, False)  # runs state report on celery worker

		self.run_scheduled_alpinelake_report(year, day, False)  # runs alpine lake report on celery worker
