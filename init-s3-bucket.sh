#!/bin/sh
echo "EXECUTING THE SCRIPT NOW."
awslocal s3 mb s3://wb-dev-local
awslocal s3api put-bucket-versioning --bucket wb-dev-local --versioning-configuration Status=Enabled
awslocal s3 ls