# imports
import sys
from collections import OrderedDict
from datetime import datetime
from awsglue.utils import getResolvedOptions
import qa_utilites as qu
import s3_utils
import traceback
import pandas as pd

format_length = 150


# function - to get current date and time for logging
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M')


def get_elapsed_time(start_time, end_time):
    diff = end_time - start_time
    days, seconds = diff.days, diff.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    elapsed_time = f"{hours}:{minutes}:{seconds}"
    return elapsed_time


def get_data_from_df(qa_summary_df):
    string_data = ','.join(qa_summary_df.columns)
    qa_summary_df = qa_summary_df.astype(str)
    for index, row in qa_summary_df.iterrows():
        line = ''
        for cell in row.values:
            line += cell + ','
        string_data += '\n' + line[:-1]
    return string_data


def get_qa_summary_line_item(validation_result, layer, src, table, validation, start_time, end_time):
    pass_dict = {k: v for k, v in validation_result["test_case_results"].items() if 'pass' in v.lower()}
    fail_dict = {k: v for k, v in validation_result["test_case_results"].items() if 'fail' in v.lower()}
    pending_dict = {k: v for k, v in validation_result["test_case_results"].items() if 'pending' in v.lower()}
    line = OrderedDict()
    line["layer"] = layer
    line["source"] = src
    line["table_name"] = table
    line["validation_type"] = validation
    line["html_file"] = validation_result["html"]
    line["total_test_cases"] = len(validation_result["test_case_results"])
    line["passed"] = len(pass_dict)
    line["failed"] = len(fail_dict)
    line["pending"] = len(pending_dict)
    line["start_time"] = start_time.strftime('%Y-%m-%d %H:%M:%S')
    line["end_time"] = end_time.strftime('%Y-%m-%d %H:%M:%S')
    line["time_elapsed"] = get_elapsed_time(start_time, end_time)
    return line


try:

    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")

    args = getResolvedOptions(sys.argv,
                              ['S3_BUCKET', 'CONFIG_FILE', 'TGT_NAME', 'TABLE_NAME', 'VALIDATION_TYPE', 'LAYER',
                               'SUMMARY_PREFIX', 'TABLE_PREFIX'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    layer_li = args['LAYER'].lower().split(",")
    tgt_name_li = args['TGT_NAME'].lower().split(",")
    table_name_li = args['TABLE_NAME'].lower().split(",")
    validation_type_li = args['VALIDATION_TYPE'].lower().split(",")
    summary_prefix = args['SUMMARY_PREFIX'].lower().strip()
    table_prefix = args['TABLE_PREFIX'].strip()

    print(f"{current_datetime()} :: main :: info - bucket		    : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file	    : {config_file}")
    print(f"{current_datetime()} :: main :: info - layers      	    : {layer_li}")
    print(f"{current_datetime()} :: main :: info - tgt names   	    : {tgt_name_li}")
    print(f"{current_datetime()} :: main :: info - table names 	    : {table_name_li}")
    print(f"{current_datetime()} :: main :: info - validation types : {validation_type_li}")
    print(f"{current_datetime()} :: main :: info - summary prefix   : {summary_prefix}")
    print(f"{current_datetime()} :: main :: info - table prefix     : {table_prefix}")

    summary_list = []

    for layer in layer_li:
        for tgt_name in tgt_name_li:
            for validation_type in validation_type_li:
                for table_name in table_name_li:
                    sub = f"{layer.capitalize()} - {tgt_name.capitalize()} - {table_name.capitalize()} - {validation_type.capitalize()}"

                    param_file_full_path = s3_utils.bucket_key_to_s3_path(bucket, config_file)
                    cmd = f"qu.parse_testcases(param_file_full_path, psubject='{sub}', psanity='Y')"

                    if layer != 'all':
                        cmd = cmd.replace(")", f", player='{layer}')")
                    if tgt_name != 'all':
                        cmd = cmd.replace(")", f", psrc_name='{tgt_name}')")
                    if table_name != 'all':
                        cmd = cmd.replace(")", f", pcdm_table='{table_name}')")
                    if validation_type != 'all':
                        cmd = cmd.replace(")", f", pvalidation_type='{validation_type}')")

                    if table_prefix.upper() != "NA":
                        cmd = cmd.replace(")", f", table_prefix='{table_prefix}')")

                    print(f"{current_datetime()} :: main :: command to run - {cmd}")
                    start_time = datetime.now()
                    resp = eval(cmd)
                    end_time = datetime.now()
                    summary_line = get_qa_summary_line_item(resp, layer, tgt_name, table_name, validation_type,
                                                            start_time, end_time)
                    summary_list.append(summary_line)
                    print("{current_datetime()} :: main :: Final response : ", resp)

    if len(summary_list) > 0:
        print(f"{current_datetime()} :: run_qa :: QA Summary :: Generating QA Summary")
        qa_summary_df = pd.DataFrame(summary_list)
        qa_summary_path = f"s3://eurekapatient-j1/prod-jbi/qa/test-summary/{summary_prefix}-{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        buc, key = s3_utils.s3_path_to_bucket_key(qa_summary_path)
        print(f"{current_datetime()} :: run_qa :: QA Summary Path:: {key}")
        string_data = get_data_from_df(qa_summary_df)
        s3_utils.put_s3_object(bucket, key, string_data)

except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    traceback.print_exc()
    raise err
