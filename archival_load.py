# import modules
import sys
import traceback
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from staging_utilities import *


def define_params(bucket, config_file, table_name, src_name):
    # parse the config file contents
    print(
        f"\n{current_datetime()} :: define_params :: info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:
        global src_bucket, tgt_bucket, src_file_path, des_file_path, input_dataset, version

        filename = get_s3_object(bucket, config_file)
        param_contents = json.loads(filename)
        version = param_contents["src_dataset"][src_name]["version"]
        if version.strip() == "":
            raise Exception(f"version can not be empty")

        src_file_path_full = param_contents["src_dataset"][src_name]["inbound_path"]. \
            replace("<version>", version). \
            replace("<market_basket>", market_basket.upper())
        des_file_path_full = param_contents["src_dataset"][src_name]["archive_path"]. \
            replace("<version>", version). \
            replace("<market_basket>", market_basket.upper())
        src_bucket, src_file_path = s3_path_to_bucket_key(src_file_path_full)
        tgt_bucket, des_file_path = s3_path_to_bucket_key(des_file_path_full)

        # Check if ARN has been provided
        # if bucket.lower() != tgt_bucket.lower():
        #     if arn is None:
        #         raise Exception(f"ARN has not been provided for copying data from {bucket} to {tgt_bucket}")

        input_dataset = param_contents["input_dataset"]
    except Exception as err:
        print(
            f"{current_datetime()} :: define_params :: error - failed to read the config file {config_file} in bucket {bucket}")
        print("error details : ", err)
        raise err
    else:
        print(
            f"{current_datetime()} :: define_params :: info - successfully read the config file {config_file} in bucket {bucket}\n")
        print(f"{current_datetime()} :: define_params :: info - src_file_path      : {src_file_path}")
        print(f"{current_datetime()} :: define_params :: info - des_file_path      : {des_file_path}")
        print(f"{current_datetime()} :: define_params :: info - src_bucket         : {src_bucket}")
        print(f"{current_datetime()} :: define_params :: info - tgt_bucket         : {tgt_bucket}")
        print(f"{current_datetime()} :: define_params :: info - input_dataset      : {input_dataset}")
        print(f"{current_datetime()} :: define_params :: info - data_version       : {version}")
        print("*" * 150)


if __name__ == "__main__":
    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
    try:
        args = getResolvedOptions(sys.argv,
                                  ['S3_BUCKET', 'CONFIG_FILE', 'MARKET_BASKET', 'TGT_NAME', 'TABLE_NAME',
                                   'PROCESSING_TYPE', 'JOB_NAME', 'ARN'])
        bucket = args['S3_BUCKET']
        config_file = args['CONFIG_FILE']
        src_name = args['TGT_NAME'].strip().lower()
        market_basket = '' if args['MARKET_BASKET'] in ['""', "NA"] else args['MARKET_BASKET'].strip().lower()
        table_name = args['TABLE_NAME'].strip().lower()
        processing_type = args['PROCESSING_TYPE'].strip().lower()
        job_name = args['JOB_NAME']
        job_run_id = args['JOB_RUN_ID']
        arn = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None
    except Exception as e:
        print(f"{current_datetime()} :: main :: error - could not read glue code arguments\n")
        print("error details : ", e)
        raise e
    else:
        print(f"{current_datetime()} :: main :: info - bucket             : {bucket}")
        print(f"{current_datetime()} :: main :: info - config_file        : {config_file}")
        print(f"{current_datetime()} :: main :: info - market_basket      : {market_basket}")
        print(f"{current_datetime()} :: main :: info - src_name           : {src_name}")
        print(f"{current_datetime()} :: main :: info - table_name         : {table_name}")
        print(f"{current_datetime()} :: main :: info - processing_type    : {processing_type}")
        print(f"{current_datetime()} :: main :: info - job_name           : {job_name}")
        print(f"{current_datetime()} :: main :: info - job_run_id         : {job_run_id}")

    time.sleep(1)
    print(f"\n\n****** START - {table_name} ******")
    try:
        print(f"{current_datetime()} :: main :: step 1 - define params")
        define_params(bucket, config_file, table_name, src_name)

        if table_name == 'all':
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=src_bucket, Prefix=src_file_path)
            li = []
            for page in pages:
                for obj in page['Contents']:
                    print(obj['Key'])
                    if obj['Key'].lower().strip().split("/")[-1] != "test.txt":
                        li.append(obj['Key'])

        else:
            print(f"{current_datetime()} :: main :: step 2 - get input dataset")
            df = get_input_dataset_for_table(bucket, src_name, table_name, market_basket, input_dataset)
            print(f"{current_datetime()} :: main :: step 3 - get file list to process")
            li, sep, file_to, obj_tp = get_file_list_to_process(df, src_bucket, src_file_path, table_name, version,
                                                                processing_type)

            table_dict = {src_name: table_name.split()}
            print(f"table_dict :: {table_dict}")
            ls_file_avlblt_dict = call_if_file_available(bucket, config_file, 'INPUT_DATASET', table_dict)
            file_avlblt_dict = []
            for dict_key in ls_file_avlblt_dict:
                if dict_key['data_source'].lower() == market_basket.lower():
                    file_avlblt_dict.append(dict_key)
            if len(file_avlblt_dict) == 1:
                is_file_available = file_avlblt_dict[0]['is_file_available']
                print(f"{current_datetime()} is_file_available :: {is_file_available}")
            else:
                print(
                    f"{current_datetime()} Multiple dict values returned for table {table_name}, src_name - {src_name}")

            if is_file_available == '1':
                print(f"{current_datetime()} :: main :: step 4 - Total File count :: {len(li)}")
                if len(li) == 0:
                    print(f"{current_datetime()} :: main :: ERROR ::: failing with no files to process")
                    raise Exception("No file to process.")
            else:
                print(f"{current_datetime()} :: Skipping the load process as file is not present")

        print(f"{current_datetime()} :: main :: step 5 - perform archival load")

        for item in li:
            key = item.replace(src_file_path, "")
            tgt_key = des_file_path + key
            print(f"copying {item} file to {tgt_key}")
            cross_account_multipart_copy_s3_object(src_bucket, tgt_bucket, item, tgt_key, arn)
            print(f'deleting {item}')
            delete_s3_object(src_bucket, item, arn)
            print(f"copied...")
    except Exception as e:
        print("ERROR DETAILS - ", e)
        print(traceback.format_exc())
        raise e
    time.sleep(1)
    print(f"\n\n****** END - {table_name} ******")
