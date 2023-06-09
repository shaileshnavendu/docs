# built-in libraries
import sys
from awsglue.utils import getResolvedOptions
# user defined libraries
from cdm_utilities import *

# initialize script variables
format_length = 150


# function to get current date and time
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d')


# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'ARN', 'LAYER'])
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

# parse the config file contents
print(f"\n{current_datetime()} :: main :: info - reading the config file {config_file} in bucket {bucket} ...\n")
try:

    param_data = get_s3_object(bucket, config_file, arn)
    param_contents = json.loads(param_data)
    tgt_bucket = bucket if arn is None else param_contents["cdm_account_details"]["bucket"]
    batch_date = param_contents[extract_name]["version"]  # changed on 19 Jul 2022 to read custom version for extracts
    load_control_file = param_contents[extract_name]["load_control_file"]
    input_path_wo_prefix = param_contents["root_path"] + f'{param_contents[extract_name]["outbound_base_dir"]}_temp/'
    output_path_wo_prefix = param_contents["root_path"] + f'{param_contents[extract_name]["outbound_base_dir"]}/'
    input_path = bucket_key_to_s3_path(tgt_bucket, input_path_wo_prefix, 's3') + batch_date
    output_path = bucket_key_to_s3_path(tgt_bucket, output_path_wo_prefix, 's3') + batch_date

except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(
        f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")
    print(f"{current_datetime()} :: main :: info - input_path          : {input_path}")
    print(f"{current_datetime()} :: main :: info - output_path          : {output_path}")

print("*" * format_length)

data_dict_summary_path = collect_data_dictionary_summary(input_path, output_path)
if data_dict_summary_path:
    print(f"{current_datetime()} :: main :: info - data_dict_summary_path          : {data_dict_summary_path}")
    dict_summary_output_path = output_path + f'/schema_control/data_dictionary_layout.csv'
    df = pd.concat(map(pd.read_csv, data_dict_summary_path), ignore_index=True)
    df.to_csv(dict_summary_output_path, header=True, index=False, sep="|")
    for path in data_dict_summary_path:
        bckt, key = s3_path_to_bucket_key(path)
        delete_s3_object(bckt, key)
        print(f"{current_datetime()} :: main :: info - deleted temp file             : {path}")
    print(f"{current_datetime()} :: main :: info - dict_summary_output_path          : {dict_summary_output_path}")

else:
    print(f"{current_datetime()} :: main :: error - data_dict_summary_path was not created")

table_level_summary_path = collect_table_level_summary(input_path, output_path)
if table_level_summary_path:
    print(f"{current_datetime()} :: main :: info - table_level_summary_path          : {table_level_summary_path}")
else:
    print(f"{current_datetime()} :: main :: error - table_level_summary_path was not created")

if extract_name == "powerbi_extract":
    table_level_summary_path_v2 = collect_table_level_summary_v2(input_path, output_path)
    if table_level_summary_path_v2:
        print(
            f"{current_datetime()} :: main :: info - table_level_summary_path_v2          : {table_level_summary_path_v2}")
    else:
        print(f"{current_datetime()} :: main :: error - table_level_summary_path_v2 was not created")

file_level_summary_path = collect_file_level_summary(input_path, output_path)
if file_level_summary_path:
    print(f"{current_datetime()} :: main :: info - file_level_summary_path          : {file_level_summary_path}")
else:
    print(f"{current_datetime()} :: main :: error - file_level_summary_path was not created")

load_control_file_path = f'{add_slash(output_path_wo_prefix)}{add_slash(batch_date)}data_load_control/{load_control_file}'
put_s3_object(bucket, load_control_file_path, "")

print("job completed successfully")
