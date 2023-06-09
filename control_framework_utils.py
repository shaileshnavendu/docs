import json
import time
from datetime import datetime
from io import BytesIO

import pandas as pd

from s3_utils import *

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 2000000)
pd.set_option('display.max_colwidth', 10000)

# initialize glue client
glue_client = boto3.client('glue')


# boto3.setup_default_session(profile_name='dev')
# glue_client = boto3.client("glue", region_name='us-east-1')


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_table_list(file_name, layer, src_name=None, market=''):
    buc, key = s3_path_to_bucket_key(file_name)
    content = get_s3_object(buc, key)
    df = pd.read_excel(BytesIO(content), layer, dtype=str).fillna("NA")
    df = df[df.required.str.strip().str.lower() == 'y']
    if src_name is not None:
        df = df[df.cdm_name.str.strip().str.lower() == src_name.strip().lower()]
    if market != '' and market is not None:
        if 'market_basket' in df.columns:
            df = df[df.market_basket.str.strip().str.lower() == market.strip().lower()]
        else:
            raise Exception(f"get_table_list - 'market_basket' column not found in {file_name}, sheet - {layer} ")
    return df


def get_table_list_by_load_order(file_name, layer, src_name=None, market=''):
    df = get_table_list(file_name, layer, src_name, market)
    order_by = 'load_order'
    if 'rule_order' in df.columns:
        df["rule_order"].replace('NA', 0, inplace=True)
        df["rule_order"] = df["rule_order"].astype(int)
        df_new = df.drop_duplicates(subset=['rule_order', 'cdm_table'])
        if df.shape[0] != df_new.shape[0]:
            raise Exception(f'Duplicate entry of rule_order and cdm_table present in the file {file_name}')
        order_by = 'rule_order'
    di = df.groupby(by=order_by, sort=False).apply(lambda x: dict(zip(x.cdm_table, x.dpu))).to_dict()
    sorted_di = {}
    for key in sorted(di):
        sorted_di[key] = di[key]
    return sorted_di, order_by


def get_previous_execution_status(path):
    if s3_path_exists(path):
        b, k = s3_path_to_bucket_key(path)
        data = get_s3_object(b, k)
        json_data = json.loads(data)
    else:
        json_data = {}
    return json_data


def get_current_execution_details(job_nm, jr_id):
    response = glue_client.get_job_run(
        JobName=job_nm,
        RunId=jr_id
    )
    json_data = {"job_name": job_nm,
                 "start_datetime": str(response['JobRun']['StartedOn']),
                 "end_datetime": str(response['JobRun']['CompletedOn']),
                 "job_status": response['JobRun']['JobRunState'],
                 "error_message": response['JobRun']['ErrorMessage'] if 'ErrorMessage' in response['JobRun'] else None,
                 "execution_time": response['JobRun']['ExecutionTime']
                 }
    return json_data


def get_previous_execution_details(path):
    if s3_path_exists(path):
        b, k = s3_path_to_bucket_key(path)
        data = get_s3_object(b, k)
        json_data = json.loads(data)
    else:
        json_data = {}
    return json_data


def run_job(job_name, args, dpu, nt):
    if int(dpu) != 0:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=args,
            WorkerType='Standard' if nt.strip().upper() == "STANDARD" else nt.strip().upper(),
            NumberOfWorkers=int(dpu))
    else:
        response = glue_client.start_job_run(JobName=job_name, Arguments=args)
    job_run_id = response['JobRunId']
    return job_run_id


def get_load_type_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "load_type" in df.columns:
        di = dict(zip(df.cdm_table, df.load_type))
    else:
        di = {}
    return di


def get_market_basket_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "market_basket" in df.columns:
        di = dict(zip(df.cdm_table, df.market_basket))
    else:
        di = {}
    return di


def get_id_list_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "id_list" in df.columns:
        di = dict(zip(df.cdm_table, df.id_list))
    else:
        di = {}
    return di


def get_spark_properties_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "spark_properties" in df.columns:
        df['spark_properties'] = df['spark_properties'].replace(['NA'], ['{}'])
        di = dict(zip(df.cdm_table, df.spark_properties))
    else:
        di = {}
    return di


def get_decryption_flag_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "decryption_required" in df.columns:
        di = dict(zip(df.cdm_table, df.decryption_required))
    else:
        di = {}
    return di


def get_proc_type_by_table(file_name, layer, src_name=None, market=None):
    df = get_table_list(file_name, layer, src_name, market)
    if "processing_type" in df.columns:
        di = dict(zip(df.cdm_table, df.processing_type))
    else:
        di = {}
    return di


def get_pre_proc_fn_by_table(file_name, layer, src_name=None, market=None):
    df = get_table_list(file_name, layer, src_name, market)
    if "pre_processing_fn" in df.columns:
        di = dict(zip(df.cdm_table, df.pre_processing_fn))
    else:
        di = {}
    return di


def get_post_proc_fn_by_table(file_name, layer, src_name=None, market=None):
    df = get_table_list(file_name, layer, src_name, market)
    if "post_processing_fn" in df.columns:
        di = dict(zip(df.cdm_table, df.post_processing_fn))
    else:
        di = {}
    return di


def get_header_flg_by_table(file_name, layer, src_name=None, market=None):
    df = get_table_list(file_name, layer, src_name, market)
    if "headers_flg" in df.columns:
        di = dict(zip(df.cdm_table, df.headers_flg))
    else:
        di = {}
    return di


def get_validation_fn_by_table(file_name, layer, src_name=None, market=None):
    df = get_table_list(file_name, layer, src_name, market)
    if "validation_fn" in df.columns:
        di = dict(zip(df.cdm_table, df.validation_fn))
    else:
        di = {}
    return di


def get_table_alias_by_table(file_name, layer, src_name=None, market=None):
    df = get_table_list(file_name, layer, src_name, market)
    if "table_alias" in df.columns:
        di = dict(zip(df.cdm_table, df.table_alias))
    else:
        di = {}
    return di


def get_decrypt_file_count_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "file_count" in df.columns:
        di = dict(zip(df.cdm_table, df.file_count))
    else:
        di = {}
    return di


def get_num_partition_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "output_num_partition" in df.columns:
        di = dict(zip(df.cdm_table, df.output_num_partition))
    else:
        di = {}
    return di


def get_node_type_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "node_type" in df.columns:
        di = dict(zip(df.cdm_table, df.node_type))
    else:
        di = {}
    return di


def get_stg_load_args_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "stg_load_args" in df.columns:
        di = dict(zip(df.cdm_table, df.stg_load_args))
    else:
        di = {}
    return di


def get_current_exec_tables(tbls_by_load_typ, prev_status, order_by):
    # get table list for current execution
    curr_exec_tables = {}
    for lo in tbls_by_load_typ:
        curr_exec_tables[lo] = {}
        for table, dpu in tbls_by_load_typ[lo].items():
            # iterate through each table
            if order_by == 'rule_order':
                table_key = f"{table}#{order_by}={lo}"
            else:
                table_key = table
            if table_key in prev_status \
                    and prev_status[table_key]['status'] == "SUCCEEDED":
                print(
                    f"{current_datetime()} :: get_current_exec_tables :: {table_key} - completed with status SUCCEEDED in previous execution. Skipping in the current run.")
                continue
            elif table_key in prev_status \
                    and prev_status[table_key]['status'] == "RUNNING":
                print(
                    f"{current_datetime()} :: get_current_exec_tables :: {table_key} - completed with status RUNNING in previous execution. Skipping in the current run.")
                continue

            curr_exec_tables[lo][table_key] = dpu
    return curr_exec_tables


def get_schema_overwrite_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "schema_overwrite" in df.columns:
        di = dict(zip(df.cdm_table, df.schema_overwrite))
    else:
        di = {}
    return di


def get_add_date_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "add_date" in df.columns:
        di = dict(zip(df.cdm_table, df.add_date))
    else:
        di = {}
    return di


def get_schema_exception_list_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "schema_exception_list" in df.columns:
        di = dict(zip(df.cdm_table, df.schema_exception_list))
    else:
        di = {}
    return di


def get_part_keys_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "partition_keys" in df.columns:
        di = dict(zip(df.cdm_table, df.partition_keys))
    else:
        di = {}
    return di


def get_touchfile_name_by_table(file_name, layer, src_name=None):
    df = get_table_list(file_name, layer, src_name)
    if "touchfile_name" in df.columns:
        di = dict(zip(df.cdm_table, df.touchfile_name))
    else:
        di = {}
    return di


def set_default_status(curr_exec_tables):
    di = {}
    for lo in curr_exec_tables:
        for table in curr_exec_tables[lo]:
            di[table] = {"jr_id": None, "status": "NOT STARTED"}  # TODO
    return di


def create_batches_by_load_order(di, max_dpu_available, nt_by_tbl):
    max_dpu = max_dpu_available - 100
    new_di = {}
    i = 0
    for lo in di:
        i += 1
        tot_dpu = max_dpu
        new_di[lo] = {}
        for tbl, dpu in di[lo].items():
            dpu_counter = dpu
            if tbl in nt_by_tbl and nt_by_tbl[tbl].strip().upper() in ['G.2X', 'NA']:
                dpu_counter = int(dpu_counter) * 2
            tot_dpu -= int(dpu_counter)
            if tot_dpu < 0:
                i += 1
                tot_dpu = max_dpu - int(dpu_counter)

            if f"batch_{i}" not in new_di[lo]:
                new_di[lo][f"batch_{i}"] = {}
            new_di[lo][f"batch_{i}"][tbl] = dpu
    return new_di


def check_if_aws_failure(job_error_msg):
    if "threshold for executors failed after launch reached" in job_error_msg.lower():
        return True
    elif "subnet does not have enough free addresses" in job_error_msg.lower():
        return True
    elif "failed to execute with exception number of ip addresses on subnet" in job_error_msg.lower():
        return True
    elif "number of ip addresses on subnet is 0" in job_error_msg.lower():
        return True
    elif "headobject operation: not found" in job_error_msg.lower():
        return True
    elif "exceeded maximum concurrent compute" in job_error_msg.lower():
        return True
    else:
        return False


def wait_for_jobs_completion(job_nm, jr_dict, done_file_full, arn_to_write):
    # wait for job completion
    tbl_li = list(jr_dict)  # fetch table list
    counter = len(tbl_li)  # create a counter with number of tables
    job_sta_dict = {}  # initializing dictionary to capture job status
    fail_list = []  # capturing failed job list
    aws_failures_dict = {}  # aws failures retry dict
    # Keep iterating till all the job executions completed
    while counter > 0:
        print(f"{current_datetime()} :: wait_for_jobs_completion : waiting for job completion for - {tbl_li}")
        time.sleep(10)
        # iterating through each source to get status of job
        for tbl in tbl_li:
            if jr_dict[tbl] is None:
                fail_list.append(tbl)
                tbl_li.remove(tbl)
                counter = counter - 1
                job_sta_dict[tbl] = {"jr_id": None, "status": "COULD NOT TRIGGER"}
                continue

            response = glue_client.get_job_run(
                JobName=job_nm,
                RunId=jr_dict[tbl]
            )
            jr_id = response['JobRun']['Id']
            job_sta = response['JobRun']['JobRunState']
            job_sta_dict[tbl] = {"jr_id": jr_id, "status": job_sta}

            # if job run not completed, keep waiting & check for next table
            if job_sta in ['STARTING', 'RUNNING', 'STOPPING']:
                continue
            # If job run completed, remove completed job from iteration, update the counter and failed job list.
            else:
                intermediate_status_dict = get_previous_execution_status(done_file_full)
                intermediate_status_dict[tbl] = {"jr_id": jr_id, "status": job_sta}
                bucket, done_file = s3_path_to_bucket_key(done_file_full)
                put_s3_object(bucket, done_file, bytes(json.dumps(intermediate_status_dict, indent=4).encode("utf-8")),
                              arn_to_write)
                print(
                    f"{current_datetime()} :: wait_for_jobs_completion : Job {job_nm} for {tbl} with job run id {jr_dict[tbl]} completed with status : {job_sta}")
                tbl_li.remove(tbl)
                counter = counter - 1
                if job_sta != 'SUCCEEDED':
                    fail_list.append(tbl)
                    if job_sta == 'FAILED' and 'ErrorMessage' in response[
                        'JobRun']:  # script below is to handle aws failures and send to retry dict
                        job_error_msg = response['JobRun']['ErrorMessage']
                        dpu = 0
                        if 'NumberOfWorkers' in response['JobRun'] and response['JobRun']['NumberOfWorkers'] > 0:
                            dpu = response['JobRun']['NumberOfWorkers']
                        isAwsFailure = check_if_aws_failure(job_error_msg)
                        if isAwsFailure:
                            aws_failures_dict[tbl] = dpu
                            fail_list.remove(tbl)
    return job_sta_dict, fail_list, aws_failures_dict
