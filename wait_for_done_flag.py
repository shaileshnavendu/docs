import json
import sys
import time
from awsglue.utils import getResolvedOptions
from datetime import datetime
import s3_utils as su


# function to get current date and time
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_list_of_trigger_files(json_data, wf):
    trigger_file_li = []
    for p_wf in json_data[wf]:
        wf_split = p_wf.split("-")
        src_nm = wf_split[3].lower()
        print(f"parent src nm :: {src_nm}")
        if src_nm in config_file_content_json["src_dataset"]:
            version = config_file_content_json["src_dataset"][src_nm]["version"]
            version = batch_date if version == "" else version
        else:
            version = batch_date

        trigger_file_li.append(done_filepath + p_wf + "_" + wf_name + "_" + version + ".done")
    return trigger_file_li


# initialize script variables
format_length = 150
temp_log = ""

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
    print(f"{current_datetime()} :: main :: info - wf_name         : {wf_name}")
    print(f"{current_datetime()} :: main :: info - wf_run_id       : {wf_run_id}")
print("*" * format_length)

try:
    config_file_object = su.get_s3_object(bucket, config_file, arn)
    config_file_content_json = json.loads(config_file_object)
    done_file_base_dir = config_file_content_json["done_flag_base_dir"]
    done_filepath = config_file_content_json['root_path'] + su.add_slash(done_file_base_dir)
    bkp_done_filepath = config_file_content_json['root_path'] + su.remove_slash(done_file_base_dir) + "_archive/"
    dependency_filepath = config_file_content_json['wf_dependency_list']
    dependency_file_object = su.get_s3_object(bucket, dependency_filepath, arn)
    dependency_file_content_json = json.loads(dependency_file_object)
    dep_json_data = dependency_file_content_json["wf_dependency_list"]
    batch_date = config_file_content_json["batch_date"]

except Exception as err:
    print(f"{current_datetime()} :: main : error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main : info - successfully read the config file {config_file} in bucket {bucket}\n")

waiting_file_li = get_list_of_trigger_files(dep_json_data, wf_name)
waiting_file_li_full = su.bucket_keylist_to_s3_pathlist(bucket, waiting_file_li)
print(f"{current_datetime()} :: List of done files waiting for :: \n" + '\n'.join(waiting_file_li_full))

while waiting_file_li_full:
    temp_log += f"\n{current_datetime()} :: Expecting following files : " + ', '.join(waiting_file_li_full)
    su.put_s3_object(bucket, f"{su.add_slash(done_filepath)}{wf_name}.log", temp_log, arn)
    time.sleep(10)
    for f in waiting_file_li_full:
        if su.s3_path_exists(f):
            print(f"{f} - found")
            waiting_file_li_full.remove(f)
else:
    print(f"{current_datetime()} :: Found all files; moving them into {bkp_done_filepath}")
    temp_log += f"\n{current_datetime()} :: ALL FILES AVAILABLE"
    su.put_s3_object(bucket, f"{su.add_slash(done_filepath)}{wf_name}.log", temp_log, arn)

    for f in waiting_file_li:
        try:
            su.copy_s3_object(bucket, f, f.replace(done_filepath, bkp_done_filepath))
            su.delete_s3_object(bucket, f, arn)
        except Exception as e:
            print(f"File {f} could not be copied due to : {e}")
        else:
            print(f"Successfully copied the file {f}")

print(f"job completed successfully")
