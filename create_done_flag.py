import json
import sys
from awsglue.utils import getResolvedOptions
from datetime import datetime
import s3_utils as su


# function to get current date and time
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# function to get all the child wf name for given parent wf
def get_all_child_wfs(json_data, wf):
    _child_wf_li = []
    for k, v in json_data.items():
        if wf in v:
            _child_wf_li.append(k)
    return _child_wf_li


# initialize script variables
format_length = 150

# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'ARN', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    arn = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None
    wf_name = args['WORKFLOW_NAME']
    wf_run_id = args['WORKFLOW_RUN_ID']
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")
    print(f"{current_datetime()} :: main :: info - arn              : {arn}")
    print(f"{current_datetime()} :: main :: info - wf_name          : {wf_name}")
    print(f"{current_datetime()} :: main :: info - wf_run_id        : {wf_run_id}")
print("*" * format_length)

try:
    config_file_object = su.get_s3_object(bucket, config_file, arn)
    config_file_content_json = json.loads(config_file_object)
    done_file_base_dir = config_file_content_json["done_flag_base_dir"]
    done_filepath = config_file_content_json['root_path'] + su.add_slash(done_file_base_dir)
    dependency_filepath = config_file_content_json['wf_dependency_list']
    dependency_file_object = su.get_s3_object(bucket, dependency_filepath, arn)
    dependency_file_content_json = json.loads(dependency_file_object)
    dep_json_data = dependency_file_content_json["wf_dependency_list"]
    batch_date = config_file_content_json["batch_date"]

except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(
        f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")

wf_split = wf_name.split("-")
src_nm = wf_split[3]
print(f"src_nm : {src_nm}")
print(f"src_nm                   :: {src_nm}")

if src_nm in config_file_content_json["src_dataset"]:
    version = config_file_content_json["src_dataset"][src_nm]["version"]
    version = batch_date if version == "" else version
else:
    version = batch_date

child_wf_li = get_all_child_wfs(dep_json_data, wf_name)
print(f"wf_name                  :: {wf_name}")
print(f"child wf list            :: {child_wf_li}")
print(f"parent src version       :: {version}")

for c in child_wf_li:
    body = "Done file for workflow : " + wf_name + " with child " + c + "is created."
    key_filepath = done_filepath + wf_name + "_" + c + "_" + version + ".done"
    response = su.put_s3_object(bucket, key_filepath, body, arn)
    if response:
        print(f"{key_filepath} successfully created")
    else:
        print(f"{key_filepath} could not be created")
