import logging
import boto3
from botocore.exceptions import ClientError
import os

BASE_URL = "http://s3-east-01.aws.epa.gov"
BUCKET_NAME = "cyan-waterbody-reports"

"""
CyAN Waterbody Report S3 bucket structure
[state, alpine]
[year]
[month]
"""


def upload_report(file_path, directory_path, object_name):
    """Upload a file to an S3 bucket
    :param file_path: path to file to upload
    :param directory_path: directory(ies) in the bucket to upload the file to)
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    # AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    base_url = os.getenv("S3_BASE_URL", BASE_URL)
    bucket_name = os.getenv("S3_BUCKET_NAME", BUCKET_NAME)

    s3_client = boto3.client(
        's3',
        # aws_access_key_id=AWS_ACCESS_KEY,
        # aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        response = s3_client.upload_file(
            file_path, bucket_name, f"{directory_path}/{object_name}",
            ExtraArgs={'ACL': 'public-read'}
        )
    except ClientError as e:
        logging.error(e)
        return False, None

    file_url = base_url + "/" + bucket_name + "/" + directory_path + "/" + object_name
    return True, file_url
