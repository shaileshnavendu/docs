import sys
from time import sleep

# initialize spark context and sqlcontext
from pyspark import SparkContext, SQLContext
from awsglue.utils import getResolvedOptions
from cdm_utilities import *
from cdm_utilities import read_redshift_table

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
sc.setLogLevel("Error")
format_length = 150


# function to get current date and time
def current_datetime():
    sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
global sql_query, ext_id_tgt_path
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
    src_schema = param_contents['extracts_lookup_schema']
    redshift_config = param_contents["redshift_to_s3"]
    secret_manager = redshift_config["ingestion_lookup"]['secret_manager_db']
    aws_region = redshift_config['aws_region']
    s3_temp_dir = redshift_config['s3_temp_dir']
    redshift_driver = redshift_config['redshift_driver']
    redshift_db = redshift_config["ingestion_lookup"]['redshift_db']
    redshift_role = redshift_config["ingestion_lookup"]['redshift_role']

except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(
        f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")
    print(f"{current_datetime()} :: main :: info - external id path          : {ext_id_tgt_path_wo_prefix}")
    print(f"{current_datetime()} :: main :: info - source schema        : {src_schema}")
print("*" * format_length)

print(f"\n{current_datetime()} :: main :: info - fetching secret manager credentials ...")
try:
    db_credentials = get_credentials_from_sm(secret_manager, aws_region)
    json_creds = json.loads(db_credentials["SecretString"])
    redshift_host = json_creds['host'].lstrip()
    redshift_port = json_creds['port']
    redshift_user = json_creds['user']
    redshift_password = json_creds['password']
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to fetch secret manager credentials")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully fetched secret manager credentials\n")
    print(f"{current_datetime()} :: main :: info - redshift_host	: '{redshift_host}'")
    print(f"{current_datetime()} :: main :: info - redshift_port	: '{redshift_port}'")
    print(f"{current_datetime()} :: main :: info - redshift_db	    : '{redshift_db}'")

url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}?user={redshift_user}&password={redshift_password}"
sleep(10)
df = read_redshift_table(sqlContext, redshift_driver, url, s3_temp_dir, redshift_role, src_schema, table_name)
print(f"Writing the dataframe content with count :: {df.count()} at location ==> {ext_id_tgt_path}")
write_parquet(df, ext_id_tgt_path, temp_path=None)
print(f"Successfully Completed!!")
