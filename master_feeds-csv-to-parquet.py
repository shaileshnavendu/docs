# import modules
import sys
import json
from awsglue.utils import getResolvedOptions

from cdm_utilities import *

from pyspark.context import SparkContext
from pyspark.sql import SQLContext

# Define global variables
format_length = 150

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# setting S3 max connection
hc = sc._jsc.hadoopConfiguration()
hc.setInt("fs.s3.connection.maximum", 100)


# function - to get current date and time
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


if __name__ == "__main__":
    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
    try:
        args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'TGT_NAME'])
        bucket = args['S3_BUCKET']
        config_file = args['CONFIG_FILE']
        dataset = args['TGT_NAME'].lower()
    except Exception as e:
        print(f"{current_datetime()} :: main :: error - could not read glue code arguments\n")
        print("error details : ", e)
        raise e
    else:
        print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
        print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")

    # parse the config file contents
    print(f"\n{current_datetime()} :: main :: info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:
        filename = get_s3_object(bucket, config_file)
        param_contents = json.loads(filename)
        table_list = param_contents["src_dataset"][dataset]["table_list"]
    except Exception as err:
        print(
            f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
        print("error details : ", err)
        raise err
    else:
        print(
            f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")

    for table_name in table_list:
        try:
            src_bucket, src_key = s3_path_to_bucket_key(param_contents["src_dataset"][dataset]["inbound_path"])
            src_file_path = add_slash(src_key) + table_name + ".csv"
            # list all the objects for the given path and file name
            src_file_list = list_s3_objects(src_bucket, src_file_path)
            print(f"{current_datetime()} :: main :: info - The list of file(s) are : {src_file_list}")
            # Append s3:// to the file path
            src_file_list = bucket_keylist_to_s3_pathlist(src_bucket, src_file_list)
            print(f"{current_datetime()} :: main :: info - The file list along with S3 full path : {src_file_list}")

            # des_file_path = param_contents["root_path"] + "ingestion/master_feeds/" + table_name
            des_file_path = param_contents["root_path"] + add_slash(param_contents["master_base_dir"]) + table_name
            des_file_path = bucket_key_to_s3_path(bucket, des_file_path, 's3')

        except Exception as err:
            print(
                f"{current_datetime()} :: main :: error - failed to read the master feed file details in bucket {bucket}")
            print("error details : ", err)
            raise err
        else:
            print(f"{current_datetime()} :: main :: info - src_file_list      : {src_file_list}")
            print(f"{current_datetime()} :: main :: info - des_file_path      : {des_file_path}")
            print("*" * format_length)

        if len(src_file_list) != 0:
            print(
                f"{current_datetime()} :: main :: info - Start - {table_name} with inferSchema set to : {table_list[table_name]}")
            # read csv and write into parquet
            try:
                encoding_format = "utf-8"
                if table_name in ["whitelist_patient_response", "whitelist_statuscodereason", "whitelist_channeltype"]:
                    encoding_format = "ISO-8859-1"
                # df = read_csv(sqlContext, src_file_list)
                df = sqlContext.read.format('csv') \
                    .option("encoding", encoding_format) \
                    .option("header", True) \
                    .option("delimiter", ",") \
                    .option("inferSchema", table_list[table_name]).load(src_file_list).distinct()
                sta = write_parquet(df, des_file_path)
                if sta.lower() == "success":
                    print(f"{current_datetime()} :: main :: info - Job completed successfully")
                else:
                    print(f"{current_datetime()} :: main :: info - Process failed")
            except Exception as err:
                print(f"{current_datetime()} :: main :: error - failed to read and/or write into parquet ")
                print("error details : ", err)
                raise err
            else:
                print(
                    f"{current_datetime()} :: main :: info - successfully read file {src_file_list} and write into {des_file_path}\n")
        else:
            print(f"No files present for the path {src_file_path}")
