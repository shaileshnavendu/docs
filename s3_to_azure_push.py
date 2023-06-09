# import modules
import json
import sys

from s3_utils import *
from awsglue.utils import getResolvedOptions
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import time
from datetime import datetime

# initialize script variables
format_length = 150


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'LAYER', 'ARN'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    extract_name = args['LAYER']
    arn = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")
    print(f"{current_datetime()} :: main :: info - extract_name     : {extract_name}")
    print(f"{current_datetime()} :: main :: info - arn              : {arn}")
print("*" * format_length)
try:
    param_data = get_s3_object(bucket, config_file, arn)
    param_contents = json.loads(param_data)
    src_bucket = bucket if arn is None else param_contents["cdm_account_details"]["bucket"]
    azure_blob_connect_string = param_contents[extract_name]["azure_details"]["azure_blob_connect_string"]
    azure_blob_container = param_contents[extract_name]["azure_details"]["azure_blob_container"]
    azure_control_path = param_contents[extract_name]["azure_details"]["azure_control_path"]
    batch_date = param_contents[extract_name]["version"]
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(
        f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")
    print(f"{current_datetime()} :: main :: info - src_bucket                               : {src_bucket}")
    print(
        f"{current_datetime()} :: main :: info - azure_blob_connect_string schema         : {azure_blob_connect_string}")
    print(f"{current_datetime()} :: main :: info - azure_blob_container                     : {azure_blob_container}")
    print(f"{current_datetime()} :: main :: info - azure_control_path                       : {azure_control_path}")
print("*" * format_length)

blob_service_client = BlobServiceClient.from_connection_string(azure_blob_connect_string)
s3_key = add_slash(param_contents["root_path"]) + add_slash(param_contents[extract_name]["outbound_base_dir"]) + \
         add_slash(batch_date) + 'ctl_files/' + param_contents[extract_name]["azure_details"]["push_file_name"]
print(f"s3_key : {s3_key}")
s3_client = boto3.client('s3')
current_file = s3_client.get_object(Bucket=src_bucket, Key=s3_key)
itr = current_file["Body"].iter_chunks()
azure_key = f"{azure_control_path}{s3_key.split('/')[-1]}"
print(f"azure_key - {azure_key}")
azure_container_client = blob_service_client.get_container_client(container=azure_blob_container)
azure_container_client.upload_blob(name=azure_key, data=itr, overwrite=True)
print(f"File successfully published to azure at {azure_key}")
