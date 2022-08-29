import os
import json
import sys

import requests
import logging
import urllib3
import tqdm

urllib3.disable_warnings()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cyan-downloader")


PROJECT_ROOT=os.path.abspath(os.path.dirname(__file__))

default_image_dir = os.path.join(PROJECT_ROOT, "test_images")


class AdminLogin:
    def __init__(self):
        self.session_id = None
        self.image_dir = os.getenv("NASA_IMAGE_PATH", default_image_dir)
        # self.base_url = "https://ceamdev.ceeopdev.net/admintool/"
        # self.base_url = "http://host.docker.internal:8085"
        self.base_url = os.getenv("ADMIN_TOOL_URL")
        self.login_url = self.base_url + "login/"
        self.upload_url = self.base_url + "upload/"
        self.check_upload_url = self.base_url + "check_data_to_be_uploaded/?file_name="
        self.csrftoken = None
        self.cookies = None
        self.admin_login()

    def admin_login(self):
        print("CyAN Admin Login")

        username = os.getenv("CYAN_ADMIN_USER")
        password = os.getenv("CYAN_ADMIN_PASS")

        token_request = requests.get(self.base_url, verify=False)
        self.csrftoken = token_request.cookies["csrftoken"]
        self.cookies = token_request.cookies

        request_body = {
            'csrfmiddlewaretoken': self.csrftoken,
            'username': username,
            'password': password,
            'next': '/'
        }

        headers = {
            "Origin": self.base_url,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.post(self.login_url, request_body, cookies=self.cookies, verify=False)
        if len(response.history) == 0:
            logger.info("Invalid login credientals")
            sys.exit()
        self.session_id = response.history[0].cookies['sessionid']
        self.csrftoken = response.history[0].cookies['csrftoken']
        self.cookies = response.history[0].cookies
        del username
        del password

    def upload(self, directory_path=None):
        directory_path = self.image_dir
        i = 0
        for image_file in tqdm.tqdm(os.listdir(directory_path), desc="Upload image files to admintool"):
            if image_file.endswith(".tif"):
                self.upload_image(file_path=os.path.join(directory_path, image_file))
                i += 1
        logging.info(f"Completed upload of {i} files from directory: {directory_path}")

    def upload_image(self, file_path):
        file_name = file_path.split("\\")[-1]
        check_file_url = self.check_upload_url + file_name
        headers = {'Accept': 'application/json, text/javascript, */*; q=0.01'}
        response = requests.get(check_file_url, headers=headers, cookies=self.cookies, verify=False)
        response_json = json.loads(response.content)
        if response_json["success"] is True:
            logger.info(f"Uploading image: {file_name}")
            request_body = {
                'csrfmiddlewaretoken': self.csrftoken,
                'sessionid': self.session_id
            }
            image_file = {'file': open(file_path, 'rb')}
            upload_response = requests.post(self.upload_url, request_body, files=image_file, headers=headers, cookies=self.cookies, verify=False)
            logging.info("File upload complete")
            logging.info("Upload response: {}".format(upload_response))
            logging.info("Upload response content: {}".format(upload_response.content))
        else:
            logger.info(f"Image file: {file_name} has already been uploaded")


if __name__ == "__main__":

    # image_path = "D:\\data\\cyan\\L2022154.L3m_DAY_CYAN_CI_cyano_CYAN_CONUS_300m\\L2022154.L3m_DAY_CYAN_CI_cyano_CYAN_CONUS_300m_9_3.tif"
    # image_dir = "D:\\data\\cyan\\L2022154.L3m_DAY_CYAN_CI_cyano_CYAN_CONUS_300m\\"
    # image_dir = "D:\\data\\cyan\\processing\\images"
    # image_dir = "C:/Users/inick/test_images"
    admin = AdminLogin()
    # admin.upload_image(file_path=image_path)
    # admin.upload(directory_path=image_dir)
    admin.upload()
