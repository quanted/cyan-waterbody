import requests
import json
import logging
import sys
import subprocess
import os
import time
import argparse
from datetime import datetime, timedelta
from scheduled_tasks.upload_images import AdminLogin

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cyan-downloader")

parser = argparse.ArgumentParser(description="Download tool for retrieving CyAN imagery from NASA OceanData service.")
parser.add_argument('--start_date', default='', type=str, help='Start date of imagery to download. Format: YYYY-MM-DD')
parser.add_argument('--end_date', default='', type=str, help='End date of imagery to download. Format: YYYY-MM-DD')
parser.add_argument('--daily', action='store_true', help='Data product to download, weekly=1 and daily=2')
parser.add_argument('--output_path', default='output', type=str, help='Path to save downloaded files to. If none '
                                                                      'provided will default to the current working '
                                                                      'directory output')
parser.add_argument('--creds_path', default='', type=str, help='Path to file containing the required username and '
                                                                 'password')
parser.add_argument('--upload', action='store_true', help='Upload files to admin portal once download is complete.')

config_file = "download-config.json"


class NasaImageDownloads:

    def __init__(self, output_path: str = None, creds_file: str = None):
        self.nasa_url = "https://oceandata.sci.gsfc.nasa.gov/api/cyan_file_search"
        self.tiles = "1_1 1_2 1_3 1_4 2_1 2_2 2_3 2_4 3_1 3_2 3_3 3_4 3_5 4_1 4_2 4_3 4_4 4_5 5_1 5_2 5_3 5_4 5_5 6_1 6_2 6_3 6_4 6_5 7_1 7_2 7_3 7_4 7_5 8_1 8_2 8_3 8_4"
        self.request_delay = 8  # for preventing 429 response (1s too short, 10s works, maybe shorter works)
        self.request_params = {
            "region": "1",  # valid options: 0 (Alaska); 1 (CONUS)
            "period": None,  # valid options: 1 (Weekly); 2 (Daily)
            "product": "1",  # valid options: 1 (Cyanobacteria Index); 2 (True Color)
            "areaids": self.tiles,
            # valid options: all (retrieve all data); tile (retrieve single tile by ID, ex: 1_1); tile1+tile2 (retrieve multiple tiles by ID, ex: 1_1+2_1+3_1)
            "sdate": None,  # (optional) start date for a search (format YYYY-MM-DD)
            "edate": None,  # (optional) end date for a search (format YYYY-MM-DD)
            "addurl": "1",  # include full url in search result; boolean: 1=true, 0=false
            "results_as_file": "1",  # return results as text file listing; boolean: 1=true, 0=false
            "wgetflag": "1"  # 1 (always 1. It's a mandatory parameter.)
        }
        self.image_path = os.getenv("NASA_IMAGE_PATH", PROJECT_ROOT) if output_path is None else output_path
        self.creds_file = creds_file
        self.max_retries = 2
        self.retries = 0
        self.period = None
        self.previous_daily = None
        self.previous_weekly = None
        self.last_download = None
        self._load_configs()

    def search_files(self, period, sdate, edate):
        """
        Makes request for NASA imagery.
        """

        # TODO: Test "all" for areaids vs. string of requested tiles.

        request_params = dict(self.request_params)
        request_params["period"] = period
        request_params["sdate"] = sdate
        request_params["edate"] = edate
        response = requests.get(self.nasa_url, params=request_params)
        # logger.info(f"Period: {period}, Start Date: {sdate}, End Date: {edate}")
        return response.content

    def _load_configs(self):
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as file:
                    config_json = json.load(file)
                    if config_json['previous_daily'] != None:
                        self.previous_daily = config_json['previous_daily']
                    if config_json['previous_weekly'] != None:
                        self.previous_weekly = config_json['previous_weekly']
                    if config_json['last_download'] != None:
                        self.last_download = config_json['last_download']
            except Exception:
                logger.info("Unable to load config file")

    def _update_config(self):
        if os.path.exists(config_file):
            try:
                with open(config_file, 'w') as file:
                    config_json = {
                        "previous_daily": datetime.today().strftime("%Y-%m-%d") if self.period == 2 else self.previous_daily,
                        "previous_weekly": datetime.today().strftime("%Y-%m-%d") if self.period == 1 else self.previous_weekly,
                        "last_download": datetime.today().strftime("%Y-%m-%d")
                    }
                    file.write(json.dumps(config_json))
            except Exception:
                logger.info("Unable to save config file")

    def _load_creds(self):

        logging.warning("CREDS FILE: {}".format(self.creds_file))

        if self.creds_file != '' and self.creds_file != None:
            try:
                with open(self.creds_file) as file:
                    lines = file.readlines()
                    username = lines[0].replace('\n', '')
                    password = lines[1].replace('\n', '')
            except Exception:
                logger.info("Unable to open credentials file")
                self.creds_file = None
                return self._load_creds()
        else:
            username = os.getenv("NASA_USER")
            password = os.getenv("NASA_PASS")
        if username is None or password is None:
            logger.info("Credentials are required to download data from NASA OceanData.")
            sys.exit()
        return username, password

    def download_files(self, files_list):
        """
        Downloads image files from list of files produced by search_files().
        From cyano: wget --tries=5 --waitretry=10 -S -O --verbose --debug --user=" + this.nasaUserName + " --password=" + this.nasaPassword + " --auth-no-challenge=on --directory-prefix=" + targetFolder
        """
        logger.info("Number of images available: {}".format(len(files_list)))
        for image_url in files_list:
            logger.info("Downloading file: {}".format(image_url))

            username, password = self._load_creds()

            self.retries = 0
            result = self.make_wget_request(image_url)

            time.sleep(self.request_delay)

    def make_wget_request(self, image_url):
        """
        Executes wget with subprocess library.
        """
        username, password = self._load_creds()
        result =  subprocess.run(
            [
                "wget",
                "--server-response",
                "--user", username,
                "--password", password,
                "--auth-no-challenge", "on",
                "--directory-prefix", self.image_path,
                image_url
            ], 
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        valid_response = self.check_response_headers(result)

        if not valid_response and self.retries < self.max_retries:
            self.retries += 1
            logging.info("Retrying request for {}.\nAttempt #{}".format(image_url, self.retries))
            self.make_wget_request(image_url)
        else:
            return result


    def check_response_headers(self, result):
        """
        Checks response headers from subprocess wget stdout.
        """
        status = None
        for line in result.stdout.split("\n"):
            if line.strip().startswith("HTTP/"):
                status = line.split()[1]
                break

        logging.info("Status code: {}".format(status))

        if status == 200:
            return True
        else:
            return False

    def main(self, period, start_date=None, end_date=None):
        """
        Runs file search and file download based on period,
        start date, and end date.
        """
        if start_date == '':
            if period == 2:
                if self.previous_daily is not None:
                    start_date = (datetime.strptime(self.previous_daily, "%Y-%m-%d")).strftime("%Y-%m-%d")
                else:
                    start_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                if self.previous_weekly is not None:
                    start_date = (datetime.strptime(self.previous_weekly, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    start_date = (datetime.today() - timedelta(days=10)).strftime("%Y-%m-%d")
        if end_date == '':
            end_date = datetime.today().strftime("%Y-%m-%d")
        self.period = period
        logger.info(f"Download details, Type: {'Weekly' if period == 1 else 'Daily'}, Start Date: {start_date}, End Date: {end_date}")

        files_response = self.search_files(period, start_date, end_date)
        files_list = files_response.decode("utf-8").split("\n")
        if len(files_list) == 1:
            logger.info("No new files to download.")
            sys.exit()
        self.download_files(files_list)
        self._update_config()
        return files_list


if __name__ == "__main__":
    # Runs NASA image request with provided parameters.
    # Parameters: weekly/daily, start date, end date (format YYYY-MM-DD).
    logger.info("Executing download procedure for NASA OceanData CyAN imagery")
    args = parser.parse_args()

    period = 2 if args.daily else 1

    output_path = args.output_path if os.path.exists(args.output_path) else os.path.join(PROJECT_ROOT, "output")
    creds_path = args.creds_path if os.path.exists(args.creds_path) else ''

    nasa = NasaImageDownloads(output_path=output_path, creds_file=creds_path)
    nasa.main(period, args.start_date, args.end_date)
    if args.upload:
        admin = AdminLogin()
        admin.upload(directory_path=output_path)
    sys.exit()
