import sys
import os
import logging
import requests
import json
import time
import sqlite3
import fiona
import datetime
import calendar


PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "cyan_rare", "mounts", "database")
DEFAULT_DBF_PATH = os.path.join(PROJECT_ROOT, "cyan_rare", "mounts", "geometry")
DB_FILE = os.path.join(os.getenv("WATERBODY_DB", DEFAULT_DB_PATH), "waterbody-data_0.2.sqlite")
WATERBODY_DBF = os.path.join(os.getenv("WATERBODY_DBF", DEFAULT_DBF_PATH), "waterbodies_9.dbf")


class WaterbodyBatch:
	"""
	Batch processing of cyan-waterbody aggregation.
	"""
	def __init__(self, start_date, end_date, data_type):

		self.start_date = start_date  # YYYY-MM-DD 
		self.end_date = end_date  # YYYY-MM-DD
		self.data_type = data_type  # daily or weekly

		self.sdate = self._convert_datestring_to_dateobj(self.start_date)
		self.edate = self._convert_datestring_to_dateobj(self.end_date)

		self.start_year = self.sdate.year  # year to start
		self.start_day = int(self._get_day_of_year(self.sdate))  # day-of-year to start
		self.end_year = self.edate.year  # year to end (up to 366)
		self.end_day = int(self._get_day_of_year(self.edate))  # day-of-year to end

		self.data_type_options = ["daily", "weekly"]

		self.status_retry_sleep = 30  # seconds
		self.attempt = 0

		self.host_domain = os.getenv("WB_HOST_DOMAIN", "http://localhost:8085")  # server cyan-waterbody is running on
		self.aggregation_endpoint = "/waterbody/aggregate/"  # Ex: "/waterbody/aggregate/?year=2021&day=187&daily=True"
		self.status_endpoint = "/waterbody/aggregate/status/"  # Ex: "/waterbody/aggregate/status/?year=2021&day=187&daily=True"

		print("WB_HOST_DOMAIN is set to: {}".format(self.host_domain))
		print("WATERBODY_DB is set to: {}".format(DB_FILE))
		print("WATERBODY_DBF is set to: {}".format(WATERBODY_DBF))

		if not DB_FILE:
			raise Exception("Need to set WATERBODY_DB env var to point to WB sqlite file.")

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

		if self.data_type == "weekly":
			self.table = "WeeklyStatus"
		elif self.data_type == "daily":
			self.table = "DailyStatus"
		else:
			raise Exception("Data type must be one of the following: {}".format(self.data_type_options))

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

	def _initate_status_check_loop(self, year: int, day: int, daily: bool):
		self.attempt = 0
		start_time = time.time()
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
			logging.critical("Exception in _initate_status_check_loop(): {}".format(e))
			raise
		finally:
			self.attempt = 0
			print("Execution time: {}".format(time.time() - start_time))

	def _get_waterbodies(self):
		waterbody_list = []
		with fiona.open(WATERBODY_DBF) as waterbodies:
			for f in waterbodies:
				waterbody_list.append(int(f["properties"]["OBJECTID"]))
		return sorted(waterbody_list)

	def _doy_to_date(self, year, day):
		"""
		Converts year + day of year to date.
		"""
		return datetime.datetime(year, 1, 1) + datetime.timedelta(day - 1)

	def _get_day_of_year(self, date_obj):
		"""
		Returns the day number out of the year (1..365/366) from a date (DD-MM-YYYY).
		"""
		return str(date_obj.timetuple().tm_yday).zfill(3)

	def _convert_datestring_to_dateobj(self, date_string):
			"""
			Format: 2020-12-31
			"""
			year = int(date_string.split("-")[0])  
			month = int(date_string.split("-")[1])
			day = int(date_string.split("-")[2])
			return datetime.date(year, month, day)
			

	def main(self):
		"""
		Runs batch aggregation for waterbodies.
		"""
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

				print("Checking aggregation status for: {} {}, {} before initiating.".format(year, day, self.data_type))
				response_obj = self._make_status_request(year, day, daily)
				print("Aggregation status response: {}".format(response_obj))

				if response_obj.get("status") == "COMPLETED":
					print("Aggregation already performed for {} {}, {}".format(year, day, self.data_type))
					print("Skipping to next date.")
					continue

				print("Initiating aggregation processing for: {} {}, {}".format(year, day, self.data_type))
				result = self._make_aggregation_request(year, day, daily)
				if not result:
					print("No image found for {} {}, {}. Skipping.".format(year, day, daily))
					continue  # skips job
				self._initate_status_check_loop(year, day, daily)

	def _get_all_data(self, cur):
		"""
		Gets all DB data of a specified data type.
		"""
		print("Querying all data from {} table".format(self.table))
		start_time = time.time()
		query = "SELECT * FROM {}".format(self.table)
		cur.execute(query)
		all_rows = cur.fetchall()
		print("All data query complete.")
		print("Exec time: {}s".format(time.time() - start_time))
		return all_rows

	def run_full_status_check(self, all_rows):
		"""
		Gets all data from DailyStatus or WeeklyStatus and
		returns number of PROCESSED and FAILED data.
		"""
		print("Running full status check.")
		start_time = time.time()
		processed_rows = []
		failed_rows = []
		for row in all_rows:
			if row[3] == "PROCESSED":
				processed_rows.append(row)
			elif row[3] == "FAILED":
				failed_rows.append(row)
		print("Full status check complete.")
		print("{} table size: {}".format(self.table, len(all_rows)))
		print("Total number of rows PROCESSED in {}: {}".format(self.table, len(processed_rows)))
		print("Total number of rows FAILED in {}: {}".format(self.table, len(failed_rows)))
		print("Exec time: {}s".format(time.time() - start_time))
		return processed_rows, failed_rows

	def run_quick_check(self, year, days, waterbodies, cur):
		"""
		Checks that number of rows matches the expected size
		given the requested date ranges, etc.
		"""
		print("Running quick check.")

		start_time = time.time()

		total_days = len(days)

		print("Total days in requested date range: {}".format(total_days))

		query = "SELECT * FROM {} WHERE year=? AND day IN ({}) AND OBJECTID IN ({}) AND status=?".format(
			self.table,
			','.join(['?']*len(days)),
			','.join(['?']*len(waterbodies))
		)

		values = ([year] + days + waterbodies + ["PROCESSED"])
		cur.execute(query, values)
		rows = cur.fetchall()

		print("Rows Found: {}".format(rows))

		# TODO: Could query without status and separate from PROCESSED, FAILED, and anything missing

		results_size = len(rows)
		expected_size = total_days * len(waterbodies)

		print("Quick check complete.")
		print("Size of all matching rows: {}".format(results_size))
		print("Expected number of matched rows: {}".format(expected_size))
		print("Exec time: {}s".format(time.time() - start_time))

		if results_size == expected_size:
			print("Quick check result: valid")
			return True
		elif results_size < expected_size:
			print("Missing or FAILED images found from quick check.")
			return False
		else:
			raise Exception("More results returned from DB than expected, which is unexpected.")

	def run_slow_check(self, year, days, waterbodies, all_rows):
		"""
		Slow/deep check to see which individual dates, etc. may be
		missing from the DailyStatus or WeeklyStatus tables.

		TODO: Could probably be refactored to be faster with some
		updated queries.
		"""
		print("Running slow check.")
		processed_rows = []
		failed_rows = []
		start_time = time.time()
		counter = 1

		rows_to_check = [row for row in all_rows if row[0] == year and row[1] in days]

		for objectid in waterbodies:
			# Looping list of expected waterbodies from waterbodies DBF.

			objectid_rows = [row for row in rows_to_check if row[2] == objectid]

			print("Progress: {}%".format(round(100.0 * (counter/len(waterbodies)), 2)))

			for day in days:

				current_date = str(year) + str(day)
				datum_obj = {
					"objectid": objectid,
					"date": current_date
				}

				found_rows = []
				for row in objectid_rows:
					# Looping all rows from DB table that contain a given objectid:
					if row[0] == year and row[1] == day and row[3] == "PROCESSED":
						processed_rows.append(datum_obj)
						found_rows.append(datum_obj)

				if len(found_rows) < 1:
					print("Found FAILED or missing row: {}".format(datum_obj))
					failed_rows.append(datum_obj)

			counter += 1


		total_rows = processed_rows + failed_rows
		print("Slow check complete.")
		print("Processed images: {}/{}".format(len(processed_rows), len(total_rows)))
		print("Failed/missing images: {}/{}".format(len(failed_rows), len(total_rows)))
		print("Exec time: {}s".format(time.time() - start_time))

	def validate_database(self):
		"""
		Checks DB to ensure all expected data in tables exist.

		Tables to check: DailyStatus and WeeklyStatus.
		What to check:
			* Checks that all OBJECTIDs exist.
			* Checks that for every OBJECTID, all expected days exist.
			* Checks that images are PROCESSED (status column) for every OBJECTID for every expected day.
		"""

		# What about building an object that contains all days to
		# look for for each year?
		# Example: {"2020": [5, 12, ...], "2021": [3, 10, ...]} for weekly
		# Example: {"2020": [1,2,3,...366], "2021": [1,2,3...365]} for daily

		day_inc = 1 if self.data_type == "daily" else 7
		years = [i for i in range(self.start_year, self.end_year + 1)]
		
		dates_obj = {}
		if len(years) == 1:
			days = [*range(self.start_day, self.end_day + 1, day_inc)]
			dates_obj[years[0]] = days
		elif len(years) > 1:
			dates_obj[years[0]] = [*range(self.start_day, 366, day_inc)] if not calendar.isleap(year) else [*range(self.start_day, 367, day_inc)]
			dates_obj[years[-1]] = [*range(1, self.end_day + 1, day_inc)]
			for year in years[1:-2]:
				dates_obj[year] = [*range(1, 366, day_inc)] if not calendar.isleap(year) else [*range(1, 367, day_inc)]
		else:
			raise Exception("Invalid years or days.")

		print("Dates object for agg validation: {}".format(dates_obj))

		try:

			conn = sqlite3.connect(DB_FILE)
			cur = conn.cursor()

			all_rows = self._get_all_data(cur)

			processed_rows, failed_rows = self.run_full_status_check(all_rows)  # builds lists of processed and failed rows

			waterbodies = self._get_waterbodies()  # gets WB list from waterbodies DBF file
			print("Total number of objectids/waterbodies to check: {}".format(len(waterbodies)))

			print("Checking all days for all objectids have been processed for requested date ranges.")
			print("Start date: {}\nEnd date: {}\nData type: {}".format(
				str(self.start_year) + str(self.start_day),
				str(self.end_year) + str(self.end_day),
				self.data_type
			))

			quick_results = {}
			for year, days in dates_obj.items():
				# Runs quick check for all years:
				quick_results[year] = self.run_quick_check(year, days, waterbodies, cur)

			for year, result in quick_results.items():
				if result == False:
					# Runs slow check for any years deemed invalid by quick check:
					self.run_slow_check(year, days, waterbodies, all_rows)

		except Exception as e:
			logging.error("Exception validating database: {}".format(e))
			return

		finally:
			conn.close()



if __name__ == "__main__":

	func_type = None
	start_date = None
	end_date = None
	data_type = None

	message = "\n\nOrder of arguments: func_type (main or db), " + \
		"start_date (YYYY-MM-DD), end_date (YYYY-MM-DD), data_type (daily or weekly)\n"

	try:
		func_type = sys.argv[1]
		start_date = sys.argv[2]
		end_date = sys.argv[3]
		data_type = sys.argv[4]
	except IndexError:
		raise Exception(message)

	agg_batch = WaterbodyBatch(start_date, end_date, data_type)

	if func_type == "main":
		agg_batch.main()  # runs aggregation batch

	elif func_type == "db":
		agg_batch.validate_database()  # runs DB validation

	else:
		raise Exception(message)
