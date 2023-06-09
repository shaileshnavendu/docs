"""
This script will try to delete table, update crawler path and refresh them after each new load.
After refreshing crawlers, it will run the qa validation script to generate QA reports for that run.
"""
from datetime import datetime
from time import sleep
from s3_utils import *
import s3_utils
from awsglue.utils import getResolvedOptions
import sys
import json
from io import BytesIO
import traceback

glue_client = boto3.client('glue')
import pandas as pd
import qa_utilites as qu
from collections import OrderedDict


def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_extract_crawler_details(extract_path, prefix):
    b, k = s3_path_to_bucket_key(extract_path)
    prefix = prefix.replace("<env>", env)

    li = list_s3_subfolders_wo_path(b, k)
    di = {}
    for item in li:
        c_nm = f"{prefix}_{item}"
        c_path = bucket_key_to_s3_path(b, add_slash(k) + add_slash(item))
        if "control" in item:
            continue
        if "ctl" in item:
            continue
        if item == "cai":
            c_nm = f"{prefix}_{item}_dim"
            c_path = bucket_key_to_s3_path(b, add_slash(k) + add_slash(item) + add_slash("dim"))
        di[c_nm] = c_path
    return di


def get_crawler_name():
    """
    This function returns crawler name by considering market basket and source name availability
    :return: list of crawler names
    """
    market_basket = []
    if 'market_basket' in layer_df.columns:
        market_basket = layer_df.market_basket.unique()
    c_names = []
    if len(market_basket) > 0:
        for mb in market_basket:
            if src:
                c_name = f"{env}_{src}_{mb.lower()}_{abbr_layer}"
            else:
                c_name = f"{env}_{abbr_layer}"
            c_names.append(c_name)
    else:
        if src:
            c_name = f"{env}_{src}_{abbr_layer}"
        else:
            c_name = f"{env}_{abbr_layer}"
        c_names.append(c_name)
    return c_names


def get_crawler_name_and_path(bucket):
    """
    This function takes in bucket name and prepares a dict of crawler name and s3 path
    :param bucket: s3 bucket name
    :return:
    """
    if layer in ob_layer_li:
        c_path = f"{root_path}{s3_layer}/{version}/"
        c_full_path = bucket_key_to_s3_path(bucket, c_path)
        if not s3_path_exists(c_full_path):
            raise Exception(f"{c_full_path} - not found")
        c_paths = get_extract_crawler_details(c_full_path, ob_layer_li[layer])
        return c_paths

    market_basket = []
    if 'market_basket' in layer_df.columns:
        market_basket = layer_df.market_basket.unique()
    c_paths = {}
    if len(market_basket) > 0:
        for mb in market_basket:
            if src:
                c_name = f"{env}_{src}_{mb.lower()}_{abbr_layer}"
                if version:
                    c_path = f"{root_path}{s3_layer}/{src}/{mb.lower()}/{version}/"
                else:
                    c_path = f"{root_path}{s3_layer}/{src}/{mb.lower()}/"
                c_paths[c_name] = bucket_key_to_s3_path(bucket, c_path)
            else:
                c_name = f"{env}_{abbr_layer}"
                if version:
                    c_path = f"{root_path}{s3_layer}/{version}/"
                else:
                    c_path = f"{root_path}{s3_layer}/"
                c_paths[c_name] = bucket_key_to_s3_path(bucket, c_path)
    else:
        if src:
            c_name = f"{env}_{src}_{abbr_layer}"
            if version:
                c_path = f"{root_path}{s3_layer}/{src}/{version}/"
            else:
                c_path = f"{root_path}{s3_layer}/{src}/"
            c_paths[c_name] = bucket_key_to_s3_path(bucket, c_path)
        else:
            c_name = f"{env}_{abbr_layer}"
            if version:
                c_path = f"{root_path}{s3_layer}/{version}/"
            else:
                c_path = f"{root_path}{s3_layer}/"
            c_paths[c_name] = bucket_key_to_s3_path(bucket, c_path)
    return c_paths


def verify_glue_tables(master_tbl_list):
    """
    This function queries glue to get table list and checks existence in
    master table list.
    :param master_tbl_list: list of tables
    :return: list of tables that are present in both Glue and Master Table List
    """
    if not master_tbl_list:
        return []

    next_token = ""
    list_of_tbls = []
    while True:
        response = glue_client.get_tables(
            DatabaseName=crawler_db,
            NextToken=next_token
        )
        for table in response.get('TableList'):
            tbl_nm = table.get('Name')
            if tbl_nm in master_tbl_list:
                print(tbl_nm)
                list_of_tbls.append(tbl_nm)
        next_token = response.get('NextToken')

        if next_token is None:
            break
    print(f"{current_datetime()} :: verify_glue_tables :: step 3 - verified table list :: {list_of_tbls}")
    return list_of_tbls


def prepare_delete_table_list(src):
    """
    This function prepares a list of tables to delete based on
    availability of market basket and source.
    :param src: Dataset Source
    :return:
    """
    if src in ob_layer_li:
        return []
    tbl_list = []
    unique_cdm_tables_li = layer_df.cdm_table.unique()
    deletion_required = qc_df['deletion_required'].iloc[0]
    print(
        f"\t{current_datetime()} :: prepare_delete_table_list :: "
        f"info -  deletion_required is {deletion_required} and unique_cdm_tables_list => {unique_cdm_tables_li} ...\n")
    market_basket = []
    if 'market_basket' in layer_df.columns:
        market_basket = layer_df.market_basket.unique()
    print(
        f"\t{current_datetime()} :: prepare_delete_table_list :: "
        f"info -  market_basket :: {market_basket} ...\n")
    if deletion_required == 'Y':
        if len(market_basket) > 0:
            for mb in market_basket:
                if src:
                    tbl_list = [f"{env}_{src}_{mb.lower()}_{abbr_layer}_{x}" for x in unique_cdm_tables_li]
                else:
                    tbl_list = [f"{env}_{abbr_layer}_{x}" for x in unique_cdm_tables_li]
        else:
            if src:
                tbl_list = [f"{env}_{src}_{abbr_layer}_{x}" for x in unique_cdm_tables_li]
            else:
                tbl_list = [f"{env}_{abbr_layer}_{x}" for x in unique_cdm_tables_li]
    print(
        f"\t{current_datetime()} :: prepare_delete_table_list :: "
        f"info -  prepared table list :: {tbl_list} ...\n")
    return tbl_list


def delete_tables(glue_tbl_list):
    """
    This function deletes tables from glue, throws error if glue returns
    any other status code apart from 200.
    :param glue_tbl_list: list of tables to delete
    :return:
    """
    if len(glue_tbl_list) == 0:
        print(
            f"\t{current_datetime()} :: delete_tables :: info -  No tables to delete ...\n")
    else:
        for tbl in glue_tbl_list:
            print(
                f"\t{current_datetime()} :: delete_tables :: info - deleting table {tbl} \n")
            response = glue_client.delete_table(
                DatabaseName=crawler_db,
                Name=tbl
            )
            print(response)
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception(
                    f"{current_datetime()} :: delete_tables :: ERROR :: Failed to delete {tbl} table. Exiting the flow...")


def update_crawler_path(c_name, path):
    """
    This function takes in crawler name and s3 path to update the same in Glue.
    :param c_name: Crawler Name
    :param path: S3 Path
    :return:
    """
    print(f"{current_datetime()} :: update_crawler_path :: update_crawler_path for : {c_name, path}")

    response = glue_client.update_crawler(Name=c_name,
                                          Targets={'S3Targets': [{'Path': path}]})
    print(response)


def update_table_prefix(c_name, prefix):
    """
    This function takes in crawler name and s3 path to update the same in Glue.
    :param prefix: Table Prefix
    :param c_name: Crawler Name
    :return:
    """
    print(f"{current_datetime()} :: update_table_prefix :: update table prefix for : {c_name, prefix}")

    response = glue_client.update_crawler(Name=c_name,
                                          TablePrefix=prefix)
    print(response)


def update_crawler(crawler_name, crawler_path):
    """
    Checks path existence and calls update_crawler_path func
    :param crawler_name: Crawler Name
    :param crawler_path: Crawler Path
    :return:
    """
    if s3_path_exists(crawler_path):
        update_crawler_path(crawler_name.replace('<version>_', ''), crawler_path)
        print(F"{crawler_name} UPDATED")
    else:
        raise Exception(f"S3 path ::: {crawler_path} doesn't exists.")

    if layer in ob_layer_li:
        update_table_prefix(crawler_name.replace('<version>_', ''), crawler_name.replace('<version>', version) + "_")


def upload_dummy_file(path):
    """
    Uploads dummy file to s3 path to make sure
    crawler takes each folder as separate table
    :param path: s3 path
    :return:
    """
    dummy_file_nm = "dummy.csv"
    print(f"{add_slash(path) + dummy_file_nm} - checking existence.")
    if s3_path_exists(add_slash(path) + dummy_file_nm):
        print(f"{add_slash(path) + dummy_file_nm} - exists; continuing...")
    else:
        print(f"{add_slash(path) + dummy_file_nm} - does not exist; creating one.")
        data = "dummy_1,dummy_2"
        path = add_slash(path) + dummy_file_nm
        buc, key = s3_path_to_bucket_key(path)
        put_s3_object(buc, key, data)
        print(f"{add_slash(path) + dummy_file_nm} - created.")


def delete_dummy_file(path):
    """
    Uploads dummy file to s3 path to make sure
    crawler takes each folder as separate table
    :param path: s3 path
    :return:
    """
    dummy_file_nm = "dummy.csv"
    print(f"{add_slash(path) + dummy_file_nm} - checking existence.")
    if s3_path_exists(add_slash(path) + dummy_file_nm):
        print(f"{add_slash(path) + dummy_file_nm} - exists; deleting...")
        buc, key = s3_path_to_bucket_key(add_slash(path) + dummy_file_nm)
        delete_s3_object(buc, key)
    else:
        print(f"{add_slash(path) + dummy_file_nm} - does not exist; ")


def wait_for_crawler_refresh(c_name_di):
    c_name_li = list(c_name_di)
    flag = True
    while flag:
        sleep(60)
        print("*" * 150)
        for c_name_raw in c_name_li:
            c_name = c_name_raw.replace('<version>_', '')
            response = glue_client.get_crawler(
                Name=c_name
            )
            print(
                f"{current_datetime()} :: wait_for_crawler_refresh :: crawler status for {c_name} is - "
                f"{response['Crawler']['State']}")
            if response['Crawler']['State'] == 'READY':
                delete_dummy_file(c_name_di[c_name_raw])
                c_name_li.remove(c_name_raw)

        if not c_name_li:
            flag = False


def refresh_crawlers(crawler_name, crawler_path):
    """
    Rrefreshes Crawler and waits for READY status
    :param crawler_name: Crawler Name
    :param crawler_path: S3 Path
    :return:
    """
    print(f"upload dummy file")
    upload_dummy_file(crawler_path)
    print(f"refresh crawler")
    glue_client.start_crawler(Name=crawler_name)
    sleep(1)
    # sleep(30)
    # while True:
    #     response = glue_client.get_crawler(Name=crawler_name)
    #     print(f"{crawler_name} status :: {response['Crawler']['State']}")
    #     status = response['Crawler']['State']
    #     if status.upper() == 'READY':
    #         print("Status Ready :: Exiting the loop")
    #         break
    #     sleep(20)
    # delete_dummy_file(crawler_path)


def get_elapsed_time(start_time, end_time):
    diff = end_time - start_time
    days, seconds = diff.days, diff.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    elapsed_time = f"{hours}:{minutes}:{seconds}"
    return elapsed_time


def get_qa_summary_line_item(validation_result, start_time, end_time):
    pass_dict = {k: v for k, v in validation_result["test_case_results"].items() if 'pass' in v.lower()}
    fail_dict = {k: v for k, v in validation_result["test_case_results"].items() if 'fail' in v.lower()}
    pending_dict = {k: v for k, v in validation_result["test_case_results"].items() if 'pending' in v.lower()}
    line = OrderedDict()
    line["layer"] = layer
    line["source"] = src
    line["html_file"] = validation_result["html"]
    line["total_test_cases"] = len(validation_result["test_case_results"])
    line["passed"] = len(pass_dict)
    line["failed"] = len(fail_dict)
    line["pending"] = len(pending_dict)
    line["start_time"] = start_time.strftime('%Y-%m-%d %H:%M:%S')
    line["end_time"] = end_time.strftime('%Y-%m-%d %H:%M:%S')
    line["time_elapsed"] = get_elapsed_time(start_time, end_time)
    return line


def get_data_from_df(qa_summary_df):
    string_data = ','.join(qa_summary_df.columns)
    qa_summary_df = qa_summary_df.astype(str)
    for index, row in qa_summary_df.iterrows():
        line = ''
        for cell in row.values:
            line += cell + ','
        string_data += '\n' + line[:-1]
    return string_data


def run_qa(tgt_name, table_name, bucket, config_file, qa_summary_path):
    """
    Prepares command to run QA validation using qa utilities. Also, catches failed QA checks.
    :param tgt_name: Dataset Source
    :param table_name: Table Name
    :param bucket: S3 bucket
    :param config_file: Config Param JSON Path
    :return: List of validation types that did not execute properly.
    """
    param_file_full_path = s3_utils.bucket_key_to_s3_path(bucket, config_file)
    print("param_file_full_path", param_file_full_path)
    print("qa_summary_full_path", qa_summary_path)
    failed_validation_li = []
    summary_li = []
    for validation_type in qc_df_validation_list:
        sub = f"{env.capitalize()} | {qa_layer.capitalize()} - {tgt_name.capitalize()} - {table_name.capitalize()} - {validation_type.capitalize()}"
        cmd = f"qu.parse_testcases(param_file_full_path, psubject='{sub}', psanity='Y',player='{qa_layer}'," \
              f"psrc_name='{tgt_name}',preportname='{batch_date}_{env}')"
        if layer in ob_layer_li:
            prefix = ob_layer_li[layer].replace("<env>", env).replace('<version>', version)
            cmd = cmd.replace(")", f", table_prefix='{prefix}')")
        if validation_type != 'all':
            cmd = cmd.replace(")", f", pvalidation_type='{validation_type}')")
        print(f"{current_datetime()} :: run_qa :: command to run - {cmd}")
        try:
            start_time = datetime.now()
            resp = eval(cmd)
            end_time = datetime.now()
            print(f"{current_datetime()} :: run_qa :: Final response : ", resp)
            print(f"{current_datetime()} :: run_qa :: Generating QA Summary Line")
            summary_line = get_qa_summary_line_item(resp, start_time, end_time)
            print(f"{current_datetime()} :: run_qa :: QA Summary Line generated :: {summary_line}")
            summary_li.append(summary_line)
        except Exception as e:
            print(traceback.format_exc())
            print(e)
            print(
                f"{current_datetime()} :: WARNING :: run_qa :: Failed to execute validation check for {validation_type}. Continuing with the rest...")
            failed_validation_li.append(validation_type)

        if 'Fail' in resp["test_case_results"].values() or 'fail' in resp["test_case_results"].values() or 'Failed' in \
                resp["test_case_results"].values() or 'failed' in resp["test_case_results"].values():
            print(
                f"{current_datetime()} :: WARNING :: run_qa :: {validation_type} check contains failed test cases. Continuing with the rest...")
            failed_validation_li.append(resp)

    if len(summary_li) > 0:
        print(f"{current_datetime()} :: run_qa :: QA Summary :: Generating QA Summary")
        qa_summary_df = pd.DataFrame(summary_li)
        buc, key = s3_path_to_bucket_key(qa_summary_path)
        print(f"{current_datetime()} :: run_qa :: QA Summary Path:: {key}")
        string_data = get_data_from_df(qa_summary_df)
        put_s3_object(bucket, key, string_data, arn)
        # qa_summary_df.to_csv(qa_summary_path, header=True, index=False)
        print(f"{current_datetime()} :: run_qa :: QA Summary :: Generated in path {qa_summary_path}")
    else:
        print(f"{current_datetime()} :: run_qa :: QA Summary :: Nothing to generate")

    return failed_validation_li


def define_params(bucket, config_file, layer, src, arn, load_order):
    """
    This function initializes variables required throughout the lifecycle of
    the code.
    """
    print(
        f"\t{current_datetime()} :: define_params :: "
        f"info - reading the config file {config_file} in bucket {bucket} ...\n")
    global batch_date, param_contents, layer_df, qc_df, qc_df_validation_list, abbr_layer, qa_layer, root_path, \
        qa_config_file, env, s3_layer, version, tgt_bucket, crawler_db, add_layer_li, ob_layer_li
    param_data = get_s3_object(bucket, config_file, arn)
    param_contents = json.loads(param_data)
    print(param_contents.keys())
    batch_date = param_contents["batch_date"]
    ob_layer_li = param_contents["outbound_layers"]
    tables_cfg = param_contents['tables_config']
    root_path = param_contents['root_path']
    qa_config_file = param_contents['qc_integration']['qc_config_file']
    crawler_db = param_contents['crawler_details']['crawler_db']
    tgt_bucket = bucket if arn is None else param_contents["cdm_account_details"]["bucket"]
    buc, key = s3_path_to_bucket_key(tables_cfg)
    content = get_s3_object(buc, key)
    layer_df = pd.read_excel(BytesIO(content), layer, dtype=str).fillna("NA")
    if src:
        print(f"Filtering layer_df for {src}")
        layer_df = layer_df[layer_df.cdm_name.str.strip().str.lower() == src.lower()]
    layer_df = layer_df[layer_df.required.str.strip().str.upper() == 'Y']
    qc_df = pd.read_excel(BytesIO(content), sheet_name="qc_automation_config").fillna('')
    qc_df = qc_df[qc_df.layer.str.strip().str.lower() == layer]
    abbr_layer = qc_df['abbr_layer'].iloc[0]
    s3_layer = qc_df['s3_layer'].iloc[0]
    add_layer_li = []
    if load_order == 1:  # hard check to make sure additional_refresh_layers is initialized only once
        print(f"\t{current_datetime()} :: load_order :: {load_order} ")
        qa_layer = qc_df['qa_layer'].iloc[0]
        print(f"\t{current_datetime()} :: qa_layer :: {qa_layer} ")
        if qc_df['validation_type'].iloc[0] != '':
            qc_df_validation_list = qc_df['validation_type'].iloc[0].split(',')
        else:
            qc_df_validation_list = []

        if qc_df['additional_refresh_layers'].iloc[0] != '':
            add_layer_li = [x.strip() for x in qc_df['additional_refresh_layers'].iloc[0].split(',')]
            print(
                f"\t{current_datetime()} :: define_params :: "
                f"info -  add_layer_li :: {add_layer_li}")
    env = qc_df['env'].iloc[0].lower()
    version_required = qc_df['version_required'].iloc[0]
    version = ''
    if version_required.lower() == 'y' and src in param_contents["src_dataset"]:
        version = param_contents["src_dataset"][src]["version"]
    elif version_required.lower() == 'y' and layer in param_contents:
        version = param_contents[layer]["version"]
    elif version_required.lower() == 'y' and src not in param_contents["src_dataset"] and layer not in param_contents:
        version = batch_date
    print(
        f"\t{current_datetime()} :: define_params :: "
        f"info -  abbr_layer is {abbr_layer} :::: other params ::::  {env, batch_date, qc_df_validation_list}, crawler_db is {crawler_db} version_required is {version_required}, qa_layer:: {qa_layer}..\n")


def start_job(bucket, config_file, layer, src, arn, load_order):
    define_params(bucket, config_file, layer, src, arn, load_order)
    print(f"{current_datetime()} :: start_job :: step 2 - prepare_delete_table_list")
    delete_tbl_list = prepare_delete_table_list(src)
    print(f"{current_datetime()} :: start_job :: step 3 - verify_glue_tables")
    glue_tbl_list = verify_glue_tables(delete_tbl_list)
    print(f"{current_datetime()} :: start_job :: step 4 - delete_tables from glue_tbl_list ::  :: {glue_tbl_list}")
    delete_tables(glue_tbl_list)
    # crawler_name_li = get_crawler_name()
    print(f"{current_datetime()} :: start_job :: step 5 - get_crawler_name_and_path")
    crawler_path_dict = get_crawler_name_and_path(tgt_bucket)
    print(f"{current_datetime()} :: start_job :: crawler_path_dict :: {crawler_path_dict}")
    update_required = qc_df['update_required'].iloc[0]
    print(f"{current_datetime()} :: start_job :: update_required :: {update_required}")
    for c_name in crawler_path_dict:
        if update_required.lower() == 'y':
            print(f"{current_datetime()} :: start_job :: step 5 - update_crawler :: {c_name}")
            update_crawler(c_name, crawler_path_dict[c_name])
    for c_name in crawler_path_dict:
        print(f"{current_datetime()} :: start_job :: step 6 - refresh_crawlers :: {c_name}")
        refresh_crawlers(c_name.replace('<version>_', ''), crawler_path_dict[c_name])

    print(f"{current_datetime()} :: start_job :: waiting for refresh completion")
    wait_for_crawler_refresh(crawler_path_dict)


print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")

args = getResolvedOptions(sys.argv, ['ARN', 'S3_BUCKET', 'CONFIG_FILE', 'TGT_NAME', 'LAYER'])
arn = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None
bucket = args['S3_BUCKET']
config_file = args['CONFIG_FILE']
layer = args['LAYER'].lower()
src = '' if args['TGT_NAME'].lower() == 'na' else args['TGT_NAME'].lower()

print(f"{current_datetime()} :: main :: info - arn		        : {arn}")
print(f"{current_datetime()} :: main :: info - bucket		    : {bucket}")
print(f"{current_datetime()} :: main :: info - config_file	    : {config_file}")
print(f"{current_datetime()} :: main :: info - layers      	    : {layer}")
print(f"{current_datetime()} :: main :: info - tgt names   	    : {src}")
print(f"{current_datetime()} :: main :: step 1 - define params")
load_oder = 1
start_job(bucket, config_file, layer, src, arn, load_oder)

for layer in add_layer_li:
    load_oder = 2
    print(f"running in load order 2 for layer:: {layer}")
    if layer != '':
        start_job(bucket, config_file, layer, src, None, load_oder)

print(f"{current_datetime()} :: main :: step 7 - run_qa")
qc_summary_base_path = param_contents['qc_integration']['qc_summary_path']
summary_output_path = f'{qc_summary_base_path}{batch_date}/{s3_layer}_{src}_qa_report.csv'
failed_validation_li = run_qa(src, 'ALL', bucket, config_file, summary_output_path)
if len(failed_validation_li) > 0:
    raise Exception(f"QA validation failed for {failed_validation_li}. Failing execution")
print("....job completed successfully.....")
