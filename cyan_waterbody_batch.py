import sys
import os
import logging
import requests
import json
import time


class WaterbodyBatch:
	"""
	Batch processing of cyan-waterbody aggregation.
	"""

	def __init__(self, start_year, end_year, start_day, end_day, data_type):
		self.start_year = start_year  # year to start
		self.end_year = end_year  # year to end (up to 366)
		self.start_day = start_day  # day-of-year to start
		self.end_day = end_day  # day-of-year to end
		self.data_type = data_type  # daily or weekly

		self.data_type_options = ["daily", "weekly"]

		self.status_retry_sleep = 10
		self.attempt = 0

		self.host_domain = os.getenv("WB_HOST_DOMAIN", "http://localhost:8085")  # server cyan-waterbody is running on
		self.aggregation_endpoint = "/waterbody/aggregate/"  # Ex: "/waterbody/aggregate/?year=2021&day=187&daily=True"
		self.status_endpoint = "/waterbody/aggregate/status/"  # Ex: "/waterbody/aggregate/status/?year=2021&day=187&daily=True"

		print("WB Host Domain is set to: {}".format(self.host_domain))

		# Example response:
		self.status_response = {
		    "completed": "100.0%",
		    "daily": True,
		    "day": 187,
		    "failed": [],
		    "status": "COMPLETED",
		    "total": 2000,
		    "year": 2021
		}

	def _validate_response(self, response):
		print("Validating response: {}".format(response))
		print("Response content: {}".format(response.content))
		
		content = response.content
		if isinstance(content, bytes):
			content = content.decode("utf-8")

		if "no images found" in content:
			return False
		elif response.status_code != 200:
			raise Exception("Error making response")
		return response

	def _make_aggregation_request(self, year: int, day: int, daily: bool):
		url = self.host_domain + self.aggregation_endpoint
		response = requests.get(url, params={"year": year, "day": day, "daily": daily})
		return self._validate_response(response)

	def _make_status_request(self, year: int, day: int, daily: bool):
		url = self.host_domain + self.status_endpoint
		response = requests.get(url, params={"year": year, "day": day, "daily": daily})
		return json.loads(self._validate_response(response).content)

	# def _check_aggregation_status(self, year: int, day: int, daily: bool):

	# 	try:
	# 		print("Checking aggregation status for: {} {}, {}".format(year, day, self.data_type))
	# 		response_obj = self._make_status_request(year, day, daily)
	# 		print("Aggregation status response: {}".format(response_obj))

	# 		status = response_obj["status"]
	# 		if status != "COMPLETED":
	# 			print("Aggregation job not completed.\nStatus: {}\nAttempt: {}".format(status, self.attempt))
	# 			time.sleep(self.status_retry_sleep)
	# 			self._check_aggregation_status(year, day, daily)  # recurses to re-check status
	# 			self.attempt += 1
	# 		else:
	# 			print("Aggregation job is complete. Moving onto next job.")
	# 			self.attempt = 0
	# 			return response_obj
	# 	except Exception as e:
	# 		logging.critical("Exception in _check_aggregation_status(): {}".format(e))
	# 		self.attempt = 0
	# 		return

	def _check_aggregation_status(self, year: int, day: int, daily: bool):
		self.attempt = 0
		try:
			status = ""
			while status != "COMPLETED":
				print("Checking aggregation status for: {} {}, {}".format(year, day, self.data_type))
				response_obj = self._make_status_request(year, day, daily)
				print("Aggregation status response: {}".format(response_obj))

				status = response_obj["status"]
				print("Aggregation job not completed.\nStatus: {}\nAttempt: {}".format(status, self.attempt))
				
				time.sleep(self.status_retry_sleep)
				self.attempt += 1
				
		except Exception as e:
			logging.critical("Exception in _check_aggregation_status(): {}".format(e))
			raise
		finally:
			self.attempt = 0

	def main(self):

		years = [i for i in range(self.start_year, self.end_year + 1)]
		print("Years for aggregation: {}".format(years))

		days = [i for i in range(self.start_day, self.end_day + 1)]
		print("Days for aggregation: {}".format(days))

		daily = True
		if not self.data_type in self.data_type_options:
			raise Exception("Data type must be one of the following: {}".format(self.data_type_options))
		elif self.data_type != "daily":
			daily = False

		for year in years:
			for day in days:
				print("Initiating aggregation processing for: {} {}, {}".format(year, day, self.data_type))
				result = self._make_aggregation_request(year, day, daily)
				if not result:
					print("No image found for {} {}, {}. Skipping.".format(year, day, daily))
					continue  # skips job
				self._check_aggregation_status(year, day, daily)


if __name__ == "__main__":
	
	start_year = None
	end_year = None
	start_day = None
	end_day = None
	data_type = None

	try:
		start_year = int(sys.argv[1])
		end_year = int(sys.argv[2])
		start_day = int(sys.argv[3])
		end_day = int(sys.argv[4])
		data_type = sys.argv[5]
	except IndexError:
		raise Exception("Order of arguments: start_year, end_year, start_day, end_day, data_type")

	agg_batch = WaterbodyBatch(start_year, end_year, start_day, end_day, data_type)
	agg_batch.main()
