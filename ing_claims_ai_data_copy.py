import json
import subprocess
import sys
import traceback
from datetime import datetime
from awsglue.utils import getResolvedOptions
from s3_utils import *

def current_datetime():
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


def define_params(bucket, config_file, tgt_name):
    # parse the config file contents
    print(f"\t{current_datetime()} :: define_params :: "
          f"info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:
        global source_path, target_base_path, bkp_path, tbl_ls, command

        param_data = get_s3_object(bucket, config_file)
        param_contents = json.loads(param_data)
        root_path = param_contents["root_path"]
        tgt_base_dir = param_contents["ing_base_dir"]
        target_path_wo_prefix = root_path + add_slash(tgt_base_dir) + add_slash(tgt_name)
        target_base_path= bucket_key_to_s3_path(bucket, target_path_wo_prefix)
    except Exception as err:
        print(f"\t{current_datetime()} :: define_params :: error - "
              f"failed to read the config file {config_file} in bucket {bucket}")
        print("\terror details : ", err)
        raise err
    else:
        print(f"\n{current_datetime()} :: define_params :: info - target_base_path  - {target_base_path}")
        bkp_path = 's3://eurekapatient-j1/prod-jbi/backup/shs_claims_AI_data_backup/'
        # tbl_ls = ['px_claim', 'rx_non_market_claim', 'dx_claim', 'sx_claim', 'rx_claim']
        tbl_ls = ['combined_plan', 'diagnosis_codes', 'drug', 'dx_claim', 'new_patient', 'patient', 'patient_activity', 'patient_mpd', 'payment_sub_type', 'pharmacy', 'pharmacy_activity', 'physician', 'physician_activity', 'plan', 'plan_xref', 'procedure_codes', 'procedure_modifier', 'px_claim', 'reject_codes', 'rx_claim', 'rx_non_market_claim', 'surgical_codes', 'sx_claim', 'valid_synoma']
        command = ""
        for tbl in tbl_ls:
            backup_path = bkp_path + add_slash(tbl) + "data_source_id=SHS_CLAIMS_AI/"
            tgt_path = add_slash(target_base_path) + add_slash(tbl) + "data_source_id=SHS_CLAIMS_AI/"
            command = command + "aws s3 cp " + backup_path  + " " + tgt_path + " --recursive,"
        command = command[:-1]
        print(f"command :: {command}")
        return command


if __name__ == "__main__":
    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
    args = getResolvedOptions(sys.argv,['S3_BUCKET', 'CONFIG_FILE', 'TGT_NAME'])
    try:
        bucket = args['S3_BUCKET']
        config_file = args['CONFIG_FILE']
        tgt_name = args['TGT_NAME'].lower()
        # bucket = 'eurekapatient-j1'
        # # 's3://eurekapatient-j1/prod-jbi/backup/shs_claims_AI_data_backup/'
        # config_file = 'prod-jbi/config/params/prod_params.json'
        # tgt_name = 'shs_claims'

    except Exception as err:
        print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
        print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
        print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")
        print(f"{current_datetime()} :: main :: info - tgt_name         : {tgt_name}")

    command = define_params(bucket, config_file, tgt_name)
    run_command('aws configure set default.s3.max_concurrent_requests 200')
    command_ls = command.split(",")
    for cmd in command_ls:
        print(f"cmd ==> {cmd}")
        run_command(cmd)