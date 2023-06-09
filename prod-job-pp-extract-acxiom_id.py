import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from cdm_utilities import *

format_length = 150
spark = SparkSession.builder.getOrCreate()


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'SNAPSHOT_DATE', 'FILE_NAME'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    snapshot_date = args['SNAPSHOT_DATE']
    file_name = args['FILE_NAME']

    if snapshot_date == "1900-01-01":
        snapshot_date = datetime.now().strftime('%Y-%m-%d')

except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket                   : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file              : {config_file}")
    print(f"{current_datetime()} :: main :: info - snapshot_date            : {snapshot_date}")
    print(f"{current_datetime()} :: main :: info - file_name                : {file_name}")

# read param file
try:
    param_data = get_s3_object(bucket, config_file)
    param_contents = json.loads(param_data)

    ing_path = bucket_key_to_s3_path(bucket, param_contents["root_path"] + add_slash(
        param_contents["ing_base_dir"]) + add_slash("dvcrosswalk") + add_slash("sdoh_outbound"))

    file_path = f'{add_slash(param_contents["root_path"])}sdoh_outbound/'
    tmp_file_path = remove_slash(file_path) + "_tmp/"
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the configuration details\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the config details\n")
    print(f"{current_datetime()} :: main :: info - ingestion path   - {ing_path}\n")
    print(f"{current_datetime()} :: main :: info - output file path - {file_path}\n")
    print(f"{current_datetime()} :: main :: info - output file name - {file_name}\n")

df = spark.read.parquet(ing_path)
df.createOrReplaceTempView("tmp")
out_df = spark.sql(f"select acxiom_id as ACXIOM_ID from tmp where snapshot_date='{snapshot_date}'")
if out_df.count() == 0:
    raise Exception(f"no records to extract from dvcrosswalk/sdoh_outbound for snapshot_date='{snapshot_date}'")

out_df.coalesce(1).write.csv(bucket_key_to_s3_path(bucket, tmp_file_path), mode="overwrite", header=True)
print(f"{current_datetime()} :: {out_df.count()} rows exported")
if not s3_path_exists(bucket_key_to_s3_path(bucket, tmp_file_path)):
    raise Exception(f"{tmp_file_path} is not found on {bucket}")

file_li = list_s3_objects(bucket, tmp_file_path)
if len(file_li) < 1:
    raise Exception(f"no files found at {tmp_file_path} on {bucket}")

if len(file_li) > 1:
    raise Exception(f"more than 1 files found at {tmp_file_path} on {bucket}")

copy_s3_object(bucket, file_li[0], add_slash(file_path) + file_name)
delete_s3_object(bucket, file_li[0])
