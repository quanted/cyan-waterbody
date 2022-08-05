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

from scheduled_tasks.nasa_image_downloader import NasaImageDownloads
from scheduled_tasks.upload_images import AdminLogin


class AggScheduler:

	def __init__(self):
		self.scheduler = None
		self.flask_url = os.getenv("WB_FLASK_URL")
		self.agg_status_endpoint = "/waterbody/aggregate/status/"
		self.agg_endpoint = "/waterbody/aggregate/"
		self.data_type_map = {
			"daily": "True",
			"weekly": "False"
		}
		self.image_path = os.getenv("NASA_IMAGE_PATH")  # path where images reside for WB and Cyano
		self.nasa_image_downloader = NasaImageDownloads()
		self.image_uploader = AdminLogin()

		self.start_scheduler()

	def start_scheduler(self):
		self.scheduler = BackgroundScheduler(daemon=True, timezone=utc)
		# self.scheduler.add_job(self.scheduled_aggregation, trigger="cron", args=["daily"], minute="*")  # testing job
		self.scheduler.add_job(self.scheduled_aggregation, trigger="cron", args=["daily"], hour="*/4", minute="30")  # every 4 hours at minute 30
		self.scheduler.add_job(self.scheduled_aggregation, trigger="cron", args=["weekly"], hour="4", minute="0")  # 1x/day at 4:00
		self.scheduler.start()

	def run_nasa_image_downloader(self, year, day, data_type):
		"""
		Downloads missing images.
		"""
		if data_type == "daily":
			period = 2
		elif data_type == "weekly":
			period = 1
			
		start_date = self.convert_doy_to_date(year, day)  # YYYY-MM-DD
		end_date = self.convert_doy_to_date(year, day)  # YYYY-MM-DD

		logging.info("Downloading image for \
			start date: {} {} ({}), end date: {} {} ({}), data type: {}".format(
				year, day, start_date, year, day, end_date, data_type
			)
		)

		# TODO: Return result from nasa_image_downloader to indicate success or failure???
		self.nasa_image_downloader.main(period, start_date, end_date)

		logging.info("Uploading image for {} {}, {}".format(year, day, data_type))

		self.image_uploader.upload(directory_path=output_path)

	def convert_doy_to_date(self, year, day):
		"""
		Converts year + day of year to date object.
		"""
		return (datetime(int(year), 1, 1) + timedelta(int(day) - 1)).strftime("%Y-%m-%d")

	def determine_daily_date(self):
		"""
		Determines date to use for daily aggregation check.
		"""
		# Gets the previous day's day of year (e.g., "2021 250").
		prev_date = datetime.utcnow() - timedelta(days=1)  # previous day's date 
		day = str(prev_date.timetuple().tm_yday)
		year = str(prev_date.year)
		return year, day

	def determine_weekly_date(self):
		"""
		Determines date to use for weekly aggregation check.
		Uses the Sunday of the current week.
		"""
		current_date = datetime.utcnow()
		previous_sunday = current_date - timedelta(days = current_date.weekday() + 1)
		day = str(previous_sunday.timetuple().tm_yday)
		year = str(previous_sunday.year)
		return year, day

	def make_agg_status_request(self, daily, year, day):
		"""
		Performs aggregation status request.
		"""
		logging.info("\n\n> Making agg status request.")
		url = self.flask_url + self.agg_status_endpoint
		params={"day": day, "year": year, "daily": daily}
		response = requests.get(url, params=params, timeout=10)
		logging.info("Agg status response: {}".format(response))
		if response.status_code != 200:
			logging.warning("Error making request to Flask API. Skipping remaining task.")
			return False
		return response

	def check_agg_status_response(self, response, year, day):
		"""
		Checks response content from aggregation status response.
		"""
		logging.info("\n\n> Checking agg status response")
		try:
			results = json.loads(response.content)
			logging.info("Agg status results: {}".format(results))
		except JSONDecodeError as e:
			logging.warning("Could not decode agg status result: {}".format(e))
			results = response.content.decode("utf-8")  # gets error message
			logging.warning("Message returned from cyan-waterbody agg status request: {}".format(results))
			logging.warning("Skipping aggregation for {}".format(year + " " + day))
			return False

		if results.get("completed") != "0%" or results.get("status") == "COMPLETED":    
			logging.info("Aggregation has already been performed for today's date: {} {}\nSkipping aggregation.".format(year, day))
			return False

		return results

	def make_agg_request(self, daily, year, day):
		"""
		Makes request to initiate aggregation.
		"""
		logging.info("\n\n> Initiating aggregation for {} {}".format(year, day))
		url = self.flask_url + self.agg_endpoint
		params={"day": day, "year": year, "daily": daily}
		try:
			response = requests.get(url, params=params, timeout=10)
		except Exception as e:
			logging.warning("Exception making aggregation request: {}".format(e))
			return False
		return response

	def check_agg_response(self, response, year, day, data_type):
		"""
		Checks aggregation response.
		NOTE: Responses currently do not return JSON.
		Example 1: "Unable to execute waterbody aggregation for year: 2022, day: 204, weekly, no images found"
		Example 2: "Waterbody aggregation initiated for year: 2022, day: 204, daily"
		"""
		logging.info("\n\n> Checking aggregation response.")

		no_images_msg = "no images found"
		agg_started_msg = "aggregation initiated"

		try:
			results = response.content.decode("utf-8")  # gets error message
		except Exception as e:
			logging.warning("Error getting results from agg response: {}".format(e))
			return False

		if agg_started_msg in results:
			logging.info("Aggregation started: {}".format(results))
			return True
		elif no_images_msg in results:
			logging.warning("No images found: {}".format(results))
			logging.info("Initiating image download for missing date: {} {}".format(year, day))

			# Starts image download process and admintool upload for year, day, data_type:
			if data_type == "weekly":
				# NOTE: Only performing this for weekly for now as daily images are already
				# downloaded and processed in the EPA-Cyano tomcat backend.
				self.run_nasa_image_downloader(year, day, data_type)

		else:
			logging.warning("Unaccounted for response: {}".format(results))
			return False

	def scheduled_aggregation(self, *args):
		"""
		Gets called by scheduler. Checks if new image
		exists for aggregation.
		"""
		data_type = args[0]  # daily or weekly

		logging.info("\n\n+++++\nInitiating scheduled aggregation routine for {} data.\n+++++\n\n".format(data_type))

		year, day = None, None
		if data_type == "daily":
			year, day = self.determine_daily_date()
		elif data_type == "weekly":
			year, day = self.determine_weekly_date()
		else:
			logging.error("Error determining data_type. Exiting scheduled task.")
			return

		daily = self.data_type_map[data_type]  # daily="True", weekly="False"

		logging.info("\n\nSelected day of year for aggregation: {} {}, {}".format(year, day, data_type))

		# Executes aggregation status endpoint,
		# checks if aggregation has been performed for current day:
		agg_status_response = self.make_agg_status_request(daily, year, day)
		if not agg_status_response:
			return

		# Checks aggregation status response:
		agg_status_results = self.check_agg_status_response(agg_status_response, year, day)
		if not agg_status_results:
			return

		# Executes aggregation endpoint:
		agg_response = self.make_agg_request(daily, year, day)
		if not agg_response:
			return

		# Checks response from starting aggregation.
		agg_results = self.check_agg_response(agg_response, year, day, data_type)
		if not agg_results:
			return
