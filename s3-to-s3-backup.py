import subprocess
import sys
import traceback
import time
import json
from datetime import datetime
from awsglue.utils import getResolvedOptions
from s3_utils import *


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def run_command(command):
    command_list = command.split(' ')

    try:
        print("Running shell command: \"{}\"".format(command))
        result = subprocess.run(command_list, stdout=subprocess.PIPE);
        # print("Command output:\n---\n{}\n---".format(result.stdout.decode('UTF-8')))
    except Exception as e:
        print("Exception: {}".format(e))
        print(traceback.format_exc())
        raise e
    else:
        print("shell command: \"{}\" executed successfully.".format(command))


if __name__ == "__main__":
    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'LAYER', 'TGT_NAME', 'BACKUP_SUFFIX', 'ARN'])
    try:
        bucket = args['S3_BUCKET']
        config_file = args['CONFIG_FILE']
        layer = args['LAYER'].lower().strip()
        tgt_name = args['TGT_NAME'].lower().strip()
        backup_suffix = args['BACKUP_SUFFIX'].strip()
        arn = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None

    except Exception as err:
        print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
        print(f"{current_datetime()} :: main :: info - bucket            : {bucket}")
        print(f"{current_datetime()} :: main :: info - config_file       : {config_file}")
        print(f"{current_datetime()} :: main :: info - layer             : {layer}")
        print(f"{current_datetime()} :: main :: info - tgt_name          : {tgt_name}")
        print(f"{current_datetime()} :: main :: info - backup_suffix     : {backup_suffix}")
        print(f"{current_datetime()} :: main :: info - arn               : {arn}")
    print("*" * 150)

    # parse the config file contents
    print(f"\n\t{current_datetime()} :: main :: info - reading the config file {config_file} in bucket {bucket} \n")
    param_data = get_s3_object(bucket, config_file)
    param_contents = json.loads(param_data)
    tgt_bucket = bucket if arn is None else param_contents["cdm_account_details"]["bucket"]
    batch_date = param_contents["batch_date"]
    backup_root_path = param_contents["data_archive_root_path"]
    root_path = param_contents["root_path"] + param_contents["data_folder_suffix"]

    backup_version = batch_date + "_" + backup_suffix

    layer_li = list_s3_subfolders_wo_path(tgt_bucket, root_path)
    if layer not in layer_li:
        print(f"{layer} not in {layer_li}")
        # raise Exception(f"{layer} not exist under {root_path} on {tgt_bucket}")

    if tgt_name in ["all", "na"]:
        src_path = add_slash(root_path) + add_slash(layer)
        bkp_path = f'{add_slash(backup_root_path)}' + add_slash(backup_version) + add_slash(layer)
    else:
        src_li = list_s3_subfolders_wo_path(tgt_bucket, add_slash(root_path) + add_slash(layer))
        if tgt_name not in src_li:
            raise Exception(f"{tgt_name} not exist under {add_slash(root_path) + add_slash(layer)} on {tgt_bucket}")
        src_path = add_slash(root_path) + add_slash(layer) + add_slash(tgt_name)
        bkp_path = f'{add_slash(backup_root_path)}' + add_slash(backup_version) + add_slash(layer) + add_slash(tgt_name)

    src_path_full = bucket_key_to_s3_path(tgt_bucket, src_path)
    bkp_path_full = bucket_key_to_s3_path(tgt_bucket, bkp_path)
    print(f'src_path_full :: {src_path_full}')
    print(f'bkp_path_full :: {bkp_path_full}')

    run_command('aws configure set default.s3.max_concurrent_requests 200')
    run_command(f'aws s3 sync {src_path_full} {bkp_path_full} --delete')
