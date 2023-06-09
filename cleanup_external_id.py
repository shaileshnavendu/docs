import sys
from time import sleep

from awsglue.utils import getResolvedOptions
from cdm_utilities import *

format_length = 150


# function to get current date and time
def current_datetime():
    sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'LAYER', 'TABLE_NAME', 'ARN'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    table_name = args['TABLE_NAME']
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
    print(f"{current_datetime()} :: main :: info - table_name       : {table_name}")
print("*" * format_length)

# parse the config file contents
print(f"\n{current_datetime()} :: main :: info - reading the config file {config_file} in bucket {bucket} ...\n")
try:
    param_data = get_s3_object(bucket, config_file)
    param_contents = json.loads(param_data)
    root_path = param_contents["root_path"]
    tgt_bucket = bucket if arn is None else param_contents["cdm_account_details"]["bucket"]
    outbound_base_path = remove_slash(param_contents[extract_name]["outbound_base_dir"])
    ext_id_tgt_path_wo_prefix = root_path + remove_slash(outbound_base_path) + "_temp/external_id/" + add_slash(
        table_name)
    ext_id_tgt_path = bucket_key_to_s3_path(tgt_bucket, ext_id_tgt_path_wo_prefix)

except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(
        f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")
    print(f"{current_datetime()} :: main :: info - external id path          : {ext_id_tgt_path}")
print("*" * format_length)

print(f"{current_datetime()} :: start cleanup - {ext_id_tgt_path}")
delete_s3_folder(tgt_bucket, ext_id_tgt_path_wo_prefix)
print(f"{current_datetime()} :: start cleanup - {ext_id_tgt_path}")
