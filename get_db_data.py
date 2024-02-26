"""
202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212,202213,202214,202215,202216,202217,202218,
202219,202220,202221,202222,202247,202277,2022111,2022112,2022113,2022121,2022150,2022160,2022161,2022162,2022169,2022190,
2022192,2022193,2022194,2022195,2022196,2022203,2022214,2022217,2022224,2022225,2022227,2022261,2022282,2022283,
2022284,2022285,2022286,2022287,2022288,2022305,2022349,2022350,2022351,2022352,2022353,2022354,2022355,2022356,2022357,
2022358,2022359,2022360,2022361,2022362,2022363,2022364,2022365

select all rows that from DailyData that match "year" and "day" columns from the above list
"""
import sqlite3
import os
import sys
import json


PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "cyan_rare", "mounts", "database")
DB_FILE = os.path.join(os.getenv("WATERBODY_DB", DEFAULT_DB_PATH), "waterbody-data_0.2.sqlite")


days = [
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 
	19, 20, 21, 22, 47, 77, 111, 112, 113, 121, 150, 160, 161, 162, 169, 190, 
	192, 193, 194, 195, 196, 203, 214, 217, 224, 225, 227, 261, 282, 283, 
	284, 285, 286, 287, 288, 305, 349, 350, 351, 352, 353, 354, 355, 356, 357, 
	358, 359, 360, 361, 362, 363, 364, 365
]

years = [2022]

# days = [299]
# years = [2020]

# rows = [[2020, 299, 18830173, 0, 14], [2020, 299, 18830173, 254, 23]]  # example rows structure
# rows = []


def get_data():
	conn = sqlite3.connect(DB_FILE)
	cur = conn.cursor()
	daily_data = []
	for year in years:
		for day in days:
			print("Year: {}\nDay: {}".format(year, day))
			# NOTE: Columns are (year, day, OBJECTID, value, count)
			query = "SELECT * FROM DailyData WHERE day=? AND year=?"
			values = (day, year,)
			cur.execute(query, values)
			row_data = cur.fetchall()
			for row in row_data:
				daily_data.append(list(row))
	print("daily_data: {}".format(daily_data))
	file = open("rows.json", "w")
	file.write(json.dumps(daily_data))
	file.close()
	conn.close()


def insert_data():
	file = open("rows.json", "r")
	rows = json.loads(file.read())
	conn = sqlite3.connect(DB_FILE)
	cur = conn.cursor()
	for row in rows:
		print("row: {}".format(row))
		query = "INSERT OR REPLACE INTO DailyData(year, day, OBJECTID, value, count) VALUES(?,?,?,?,?)"
		values = (row[0], row[1], row[2], row[3], row[4])
		print("year: {}\nday: {}\nOBJECTID: {}\nvalue: {}\ncount: {}\n".format(row[0], row[1], row[2], row[3], row[4]))
		cur.execute(query, values)
	conn.close()
	file.close()


def get_status_data():
	conn = sqlite3.connect(DB_FILE)
	cur = conn.cursor()
	daily_data = []
	for year in years:
		for day in days:
			print("Year: {}\nDay: {}".format(year, day))
			# NOTE: Columns are (year, day, OBJECTID, value, count)
			query = "SELECT * FROM DailyStatus WHERE day=? AND year=?"
			values = (day, year,)
			cur.execute(query, values)
			row_data = cur.fetchall()
			for row in row_data:
				daily_data.append(list(row))
	print("daily_data: {}".format(daily_data))
	file = open("status_rows.json", "w")
	file.write(json.dumps(daily_data))
	file.close()
	conn.close()


def insert_status_data():
	file = open("status_rows.json", "r")
	rows = json.loads(file.read())
	conn = sqlite3.connect(DB_FILE)
	cur = conn.cursor()
	for row in rows:
		print("row: {}".format(row))
		query = "INSERT OR REPLACE INTO DailyStatus(year, day, OBJECTID, status, timestamp, comments) VALUES(?,?,?,?,?,?)"
		values = (row[0], row[1], row[2], row[3], row[4], row[5])
		print("year: {}\nday: {}\nOBJECTID: {}\nstatus: {}\ntimestamp: {}\ncomments: {}".format(
			row[0], row[1], row[2], row[3], row[4], row[5])
		)
		cur.execute(query, values)
	conn.close()
	file.close()



if __name__ == "__main__":

	func_type = None

	error_message = "\nMust add func_type argument ('get_data', 'insert_data', 'get_status_data', or 'insert_status_data').\nExample: python get_db_data.py get_data\n"

	if len(sys.argv) < 2:
		print("No enough args.")
		print(error_message)
	elif not sys.argv[1] in ["get_data", "insert_data", "get_status_data", "insert_status_data"]:
		print("Args don't match: {}".format(sys.argv[1]))
		print(error_message)
	else:
		func_type = sys.argv[1]

	if func_type == "get_data":
		print("Getting data.")
		get_data()
	elif func_type == "insert_data":
		print("Inserting data.")
		insert_data()
	elif func_type == "get_status_data":
		print("Getting status data.")
		get_status_data()
	elif func_type == "insert_status_data":
		print("Inserting status data.")
		insert_status_data()

