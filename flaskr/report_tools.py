import logging
import boto3
from botocore.exceptions import ClientError
import os
from pathlib import Path

BASE_URL = os.getenv("S3_BASE_URL", "http://s3-east-01.aws.epa.gov")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "cyan-waterbody-reports")

"""
CyAN Waterbody Report S3 bucket structure
[state, alpine]
[year]
[month]
"""


def connect_to_bucket():
    s3_client = boto3.client(
        service_name='s3',
        region_name=os.getenv("AWS_DEFAULT_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        endpoint_url=os.getenv("S3_BASE_URL")
    )
    return s3_client


def upload_report(file_path, directory_path, object_name):
    """Upload a file to an S3 bucket
    :param file_path: path to file to upload
    :param directory_path: directory(ies) in the bucket to upload the file to)
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    s3_client = connect_to_bucket()

    try:
        response = s3_client.upload_file(
            file_path, BUCKET_NAME, f"{directory_path}/{object_name}",
            ExtraArgs={'ACL': 'public-read'}
        )
    except ClientError as e:
        logging.error(e)
        return False, None

    file_url = BASE_URL + "/" + BUCKET_NAME + "/" + directory_path + "/" + object_name
    return True, file_url


def get_reports_list():
    """
    Returns list of available reports from the s3 bucket.
    """
    s3_client = connect_to_bucket()
    bucket_objects = s3_client.list_objects(Bucket=BUCKET_NAME)
    if not "Contents" in bucket_objects:
        print("No contents in bucket: {}".format(BUCKET_NAME))
        return
    return bucket_objects.get("Contents")


def get_monthly_report_by_date(state: str, year: int, month: int):
    """
    Returns a specific report from the s3 bucket based on its ID.

    state report name example: cyano-report_TX_2023-2.pdf 
        * bucket name: CyAN-waterbody-report-{states[0]}_{year}-{day}.pdf
    alpine lake report name example: cyano-report_AlpineLakes_{report_date.year}-{report_date.month}.pdf
        * bucket name: CyAN-waterbody-report-alpine_{year}-{day}.pdf
    """
    directory_path = None

    if state != "alpine" and not len(state) == 2 or not state.isalpha():
        raise Exception("'state' must be 2 characters or 'alpine'.")
    if not len(str(year)) == 4:
        raise Exception("'year' must be 4 digits.")
    if len(str(month)) > 2 or len(str(month)) < 1:
        raise Exception("'month' must be 1 or 2 digits.")

    if state == "state":
        filename = f"CyAN-waterbody-report-{state}_{year}-{month}.pdf"
        key_name = f"{state}/{year}/{month}/{filename}"
    elif state == "alpine":
        filename = f"CyAN-waterbody-report-alpine_{year}-{month}.pdf"
        key_name = f"alpine/{year}/{month}/{filename}"

    # Example bucket Key: "Key": "state/some_state/2034/03/cyanwb_report_f4664adf-14fc-49c6-abde-527eb75af465.pdf"

    s3_client = connect_to_bucket()

    try:
        s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=key_name)
        return filename, s3_object
    except Exception as e:
        logging.error("Could not find object: {}, exception: {}".format(key_name, e))
        return None, None

def create_presigned_url(state: str, year: int, month: int):
    """
    Creates temporary url to s3 bucket object.
    """
    if state != "alpine" and not len(state) == 2 or not state.isalpha():
        raise Exception("'state' must be 2 characters or 'alpine'.")
    if not len(str(year)) == 4:
        raise Exception("'year' must be 4 digits.")
    if len(str(month)) > 2 or len(str(month)) < 1:
        raise Exception("'month' must be 1 or 2 digits.")

    if state == "state":
        key_name = f"{state}/{year}/{month}/CyAN-waterbody-report-{state}_{year}-{month}.pdf"
    elif state == "alpine":
        key_name = f"alpine/{year}/{month}/CyAN-waterbody-report-alpine_{year}-{month}.pdf"

    s3_client = connect_to_bucket()
    url = s3_client.generate_presigned_url('get_object',
                                    Params={"Bucket": BUCKET_NAME, "Key": key_name},
                                    ExpiresIn=15);
    return url


def upload_test():

    OUTPUT_DIR = os.path.join(Path(os.path.abspath(__file__)).parent.parent, "outputs")
    test_file = "cyanwb_report_f4664adf-14fc-49c6-abde-527eb75af465.pdf"

    # file_path=report_path
    file_path = report_file = os.path.join(OUTPUT_DIR, test_file)

    print("Test file_path: {}".format(file_path))

    directory_path = f"state/some_state/2034/03"

    # object_name = f"CyAN-waterbody-report-{states[0]}_{year}-{day}.pdf"
    object_name = test_file

    upload_status, upload_url = upload_report(file_path=file_path, directory_path=directory_path , object_name=object_name)

    print("upload_status: {}\nupload_url: {}".format(upload_status, upload_url))


# State report example:
# upload_status, upload_url = upload_report(file_path=report_path,
#                                   directory_path=f"state/{states[0]}/{report_date.year}/"
#                                                  f"{report_date.month}",
#                                   object_name=f"CyAN-waterbody-report-{states[0]}_{year}-{day}.pdf"
#                                   )

# Alpine lake report example:
# upload_status, upload_url = upload_report(file_path=report_path,
#                                   directory_path=f"alpine/{report_date.year}/"
#                                                  f"{report_date.month}",
#                                   object_name=f"CyAN-waterbody-report-alpine_{year}-{day}.pdf"
#                                   )

# Manual testing:
# import flaskr.report_tools as rt
# rt.upload_report("outputs/cyano-report_TX_2023-3.pdf", "state/TX/2023/3", "CyAN-waterbody-report-TX_2023-3.pdf")
# (True, 'http://localstack:4566/wb-dev-local/state/TX/2023/3/CyAN-waterbody-report-TX_2023-3.pdf')
