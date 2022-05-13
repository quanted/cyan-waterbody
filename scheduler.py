import logging
import requests
import os
import json
from json.decoder import JSONDecodeError
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import datetime
import time


class Scheduler:

	def __init__(self):
		self.scheduler = None
		self.schedule_type = "interval"
		self.schedule_interval_hours = float(os.getenv("SCHEDULER_INTERVAL_HOURS", 4))

		self.start_scheduler()

		logging.warning("Scheduler content: {}".format(self.scheduler))

	def start_scheduler(self):
		self.scheduler = BackgroundScheduler(daemon=True)
		self.scheduler.add_job(self.execute_scheduled_aggregation, self.schedule_type, hours=self.schedule_interval_hours)
		self.scheduler.start()

	def execute_scheduled_aggregation(self):
		"""
		Gets called by scheduler. Checks if new image
		exists for aggregation.
		"""
		logging.warning("Executing scheduled aggregation!")

		agg_status_endpoint = "/waterbody/aggregate/status/"
		agg_endpoint = "/waterbody/aggregate/"

		# Gets the previous day's day of year (e.g., "2021 250").
		prev_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)  # previous day's date 
		# day = str(datetime.utcnow().timetuple().tm_yday)  # gets day of year
		# year = str(datetime.utcnow().year)
		day = str(prev_date.timetuple().tm_yday)
		year = str(prev_date.year)
		date = year + " " + day
		logging.warning("Current day of year: {} {}".format(year, day))

		# daily = "True"  # TODO: Account for daily/weekly data types


		# NOTE: Would running it like this cause an issue, like if daily and weekly
		# were executed at the same time???


		for daily in ["True", "False"]:

			# Checks if aggregation has been performed for current day.
			url = "http://" + os.getenv("FLASK_HOSTNAME") + ":" + os.getenv("FLASK_PORT") + agg_status_endpoint
			logging.warning("Request URL: {}".format(url))
			response = requests.get(url, params={"day": day, "year": year, "daily": daily})
			logging.warning("Status response: {}".format(response))
			if response.status_code != 200:
				logging.warning("Error making request to Flask API. Skipping remaining task.")
				continue

			# Checks for error messages with 200 response:
			results = None
			try:
				results = json.loads(response.content)
				logging.warning("Status results: {}".format(results))
			except JSONDecodeError as e:
				logging.warning("Could not decode status result: {}".format(e))
				results = response.content.decode("utf-8")  # gets error message
				logging.warning("Message returned from cyan-waterbody status request: {}".format(results))
				logging.warning("Skipping aggregation for {}".format(date))
				continue

			if results.get("completed") != "0%" or results.get("status") == "COMPLETED":    
				logging.warning("Aggregation has already been performed for today's date: {}\nSkipping aggregation.".format(date))
				continue

			# Executes aggregation endpoint.
			logging.warning("Initiating aggregation for {}".format(date))
			url = "http://" + os.getenv("FLASK_HOSTNAME") + ":" + os.getenv("FLASK_PORT") + agg_endpoint
			response = requests.get(url, params={"day": day, "year": year, "daily": daily})

			# Checks response from starting aggregation.
			try:
				results = json.loads(response.content)
				logging.warning("Aggregation start results: {}".format(results))
			except JSONDecodeError as e:
				logging.warning("Could not decode aggregation result: {}".format(e))
				results = response.content.decode("utf-8")  # gets error message
				logging.warning("Message returned from cyan-waterbody aggregation request: {}".format(results))
				logging.warning("Skipping aggregation for {}".format(date))
				continue

			time.sleep(5 * 60)  # sleeps between daily and weekly aggregation

		# TODO: After that all works, work on the process "looking back" and ensuring
		# previous days have been aggregated if missing.


#####################################################################