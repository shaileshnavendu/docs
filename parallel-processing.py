import copy
import sys
import traceback

from awsglue.utils import getResolvedOptions

from control_framework_utils import *

# Define global variables
format_length = 150


def write_in_progress_json(jr_id, order_by, lo, table, done_file, jr_status, jr_dict={}):
    if order_by != 'load_order':
        jr_dict[f"{table}#{order_by}={lo}"] = {"jr_id": jr_id, "status": jr_status.upper(), order_by: lo}
    else:
        jr_dict[table] = {"jr_id": jr_id, "status": jr_status.upper(), order_by: lo}
    put_s3_object(bucket, done_file, bytes(json.dumps(jr_dict, indent=4).encode("utf-8")), arn_to_write)


if __name__ == "__main__":
    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
    try:
        args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
        workflow_name = args['WORKFLOW_NAME']
        workflow_run_id = args['WORKFLOW_RUN_ID']

        workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)[
            "RunProperties"]
        bucket = workflow_params["S3_BUCKET"]
        config_file = workflow_params["CONFIG_FILE"]
    except:
        args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE'])
        bucket = args['S3_BUCKET']
        config_file = args['CONFIG_FILE']

    try:
        args = getResolvedOptions(sys.argv,
                                  ['S3_BUCKET', 'CONFIG_FILE', 'TGT_NAME', 'LAYER', 'JOB_NAME', 'ARN', 'MARKET_BASKET'])
        # bucket = args['S3_BUCKET']
        # config_file = args['CONFIG_FILE']
        tgt_name = args['TGT_NAME'].lower().strip()
        layer = args['LAYER'].lower().strip()
        job_name = args['JOB_NAME']
        arn = args['ARN']
        market_basket = '' if args['MARKET_BASKET'] in ['NA', '""'] else args['MARKET_BASKET'].lower().strip()
        # arn_to_write = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None
        arn_to_write = None  # changed this as we are suppose to keep all the layers in single env
    except Exception as e:
        print(f"{current_datetime()} :: main :: error - could not read glue code arguments\n")
        print("error details : ", e)
        print(traceback.format_exc())
        raise e
    else:
        print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
        print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")
        print(f"{current_datetime()} :: main :: info - tgt_name         : {tgt_name}")
        print(f"{current_datetime()} :: main :: info - layer            : {layer}")
        print(f"{current_datetime()} :: main :: info - job_name         : {job_name}")
        print(f"{current_datetime()} :: main :: info - arn              : {arn}")
        print(f"{current_datetime()} :: main :: info - market_basket    : {market_basket}")
    #
    # config_file = "dev/config/dev_params_new.json"
    # bucket = "eurekapatient-j1-dev"
    # tgt_name = "hub_expansion"
    # layer = "de-identification"
    # job_name = "devtest-job-ep-hub_expansion-deid-load"
    # market_basket = ''
    # arn_to_write = None
    # arn = 'NA'
    # parse the config file contents
    print(f"\n{current_datetime()} :: main :: info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:
        filename = get_s3_object(bucket, config_file)
        param_contents = json.loads(filename)
        table_cfg = param_contents["tables_config"]
        batch_date = param_contents["batch_date"]
        max_available_dpu = param_contents["max_dpu_by_layer"][layer]
        if tgt_name in param_contents["src_dataset"]:
            version = param_contents["src_dataset"][tgt_name]["version"]
            version = batch_date if version == "" else version
        elif layer in param_contents:
            version = param_contents[layer]["version"]
        else:
            version = batch_date
        root_path = param_contents["root_path"]
        done_file_base_dir = param_contents["done_flag_base_dir"]
        job_run_base_dir = param_contents["job_run_details_base_dir"]
        if layer in ["staging", "archival", "source_validation"]:
            done_file = f"{add_slash(root_path)}{add_slash(done_file_base_dir)}{tgt_name}_{market_basket}_{layer}_{version}.json"
            jr_file = f"{add_slash(root_path)}{add_slash(job_run_base_dir)}JobRun_{tgt_name}_{market_basket}_{layer}_{version}.json"
        else:
            done_file = f"{add_slash(root_path)}{add_slash(done_file_base_dir)}{tgt_name}_{layer}_{version}.json"
            jr_file = f"{add_slash(root_path)}{add_slash(job_run_base_dir)}JobRun_{tgt_name}_{layer}_{version}.json"

        done_file_full = bucket_key_to_s3_path(bucket, done_file)
        jr_file_full = bucket_key_to_s3_path(bucket, jr_file)

    except Exception as err:
        print(
            f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
        print("error details : ", err)
        print(traceback.format_exc())
        raise err
    else:
        print(
            f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")
        print(f"{current_datetime()} :: main :: info - table_cfg         : {table_cfg}")
        print(f"{current_datetime()} :: main :: info - done_file_full    : {done_file_full}")
        print(f"{current_datetime()} :: main :: info - job run details   : {jr_file_full}")
        print("*" * format_length)

    # get table list by load order
    table_li_by_load_type, order_by = get_table_list_by_load_order(table_cfg, layer, tgt_name, market_basket)
    print(f"{current_datetime()} :: main :: table list by load order is - ")
    print(json.dumps(table_li_by_load_type, indent=4))

    _all_job_li_0 = []
    for _lo, _job_di in table_li_by_load_type.items():
        _all_job_li_0 += list(_job_di)

    # get previous execution status
    prev_status = get_previous_execution_status(done_file_full)
    print(f"{current_datetime()} :: main :: previous execution status is - ")
    print(json.dumps(prev_status, indent=4))

    # get table list for current execution
    curr_exec_tables = get_current_exec_tables(table_li_by_load_type, prev_status, order_by)
    print(f"{current_datetime()} :: main :: table list for current execution is - ")
    print(json.dumps(curr_exec_tables, indent=4))

    _all_job_li = []
    for _lo, _job_di in curr_exec_tables.items():
        _all_job_li += list(_job_di)
    # print(_all_job_li)
    if _all_job_li_0 and (not _all_job_li):
        print(f"{current_datetime()} :: main :: all tables completed with status SUCCEEDED for version {version}")
        print(traceback.format_exc())
        raise Exception(f"there are no tables to trigger in current execution")

    # get load type by table
    load_type_by_table = get_load_type_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: load type by table is - ")
    print(json.dumps(load_type_by_table, indent=4))

    # get processing type for staging by table
    proc_type_by_table = get_proc_type_by_table(table_cfg, layer, tgt_name, market_basket)
    print(f"{current_datetime()} :: main :: processing type by table is - ")
    print(json.dumps(proc_type_by_table, indent=4))

    # get pre processing function for staging by table
    pre_proc_fn_by_table = get_pre_proc_fn_by_table(table_cfg, layer, tgt_name, market_basket)
    print(f"{current_datetime()} :: main :: pre processing function by table is - ")
    print(json.dumps(pre_proc_fn_by_table, indent=4))

    # get post processing function for staging by table
    post_proc_fn_by_table = get_post_proc_fn_by_table(table_cfg, layer, tgt_name, market_basket)
    print(f"{current_datetime()} :: main :: post processing function by table is - ")
    print(json.dumps(post_proc_fn_by_table, indent=4))

    # get headers flag by table
    header_flg_by_table = get_header_flg_by_table(table_cfg, layer, tgt_name, market_basket)
    print(f"{current_datetime()} :: main :: header flag by table is - ")
    print(json.dumps(header_flg_by_table, indent=4))

    # get validation functions by table
    validation_fn_by_table = get_validation_fn_by_table(table_cfg, layer, tgt_name, market_basket)
    print(f"{current_datetime()} :: main :: validation function by table is - ")
    print(json.dumps(validation_fn_by_table, indent=4))

    # get table alias by table
    table_alias_flg_by_table = get_table_alias_by_table(table_cfg, layer, tgt_name, market_basket)
    print(f"{current_datetime()} :: main :: table alias by table is - ")
    print(json.dumps(table_alias_flg_by_table, indent=4))

    # get id list by table
    id_li_by_table = get_id_list_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: id list by table is - ")
    print(json.dumps(id_li_by_table, indent=4))

    # get spark properties by table
    spark_properties_by_table = get_spark_properties_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: spark properties by table is - ")
    print(json.dumps(spark_properties_by_table, indent=4))

    # get decryption_required flag by table
    decryption_flag_by_table = get_decryption_flag_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: decryption_required flag by table is - ")
    print(json.dumps(decryption_flag_by_table, indent=4))

    # get decryption file_count flag by table
    decrypt_file_count_by_table = get_decrypt_file_count_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: decryption file count by table is - ")
    print(json.dumps(decrypt_file_count_by_table, indent=4))

    # get staging load arguments by table
    stg_load_args_by_table = get_stg_load_args_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: staging load arguments by table is - ")
    print(json.dumps(stg_load_args_by_table, indent=4))

    # get node type by table
    node_type_by_table = get_node_type_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: node type by table is - ")
    print(json.dumps(node_type_by_table, indent=4))

    # create batches by load order
    batch_by_load_order = create_batches_by_load_order(curr_exec_tables, max_available_dpu, node_type_by_table)
    print(f"{current_datetime()} :: main :: batches by load order - ")
    print(json.dumps(batch_by_load_order, indent=4))

    # get number of partitions by table
    num_partition_by_table = get_num_partition_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: num_partition_by_table by table is - ")
    print(json.dumps(num_partition_by_table, indent=4))

    # get schema_overwrite by table
    schema_overwrite_by_table = get_schema_overwrite_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: schema_overwrite by table is - ")
    print(json.dumps(schema_overwrite_by_table, indent=4))

    # get touchfile_name by table
    touchfile_name_by_table = get_touchfile_name_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: touchfile_name by table is - ")
    print(json.dumps(touchfile_name_by_table, indent=4))

    # get add_date by table
    add_date_by_table = get_add_date_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: add_date by table is - ")
    print(json.dumps(add_date_by_table, indent=4))

    # get schema_exception_list by table
    schema_exception_list_by_table = get_schema_exception_list_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: schema_exception_list by table is - ")
    print(json.dumps(schema_exception_list_by_table, indent=4))

    # get partition keys by table
    part_keys_by_table = get_part_keys_by_table(table_cfg, layer, tgt_name)
    print(f"{current_datetime()} :: main :: schema_exception_list by table is - ")
    print(json.dumps(part_keys_by_table, indent=4))

    prev_lo = "0"
    load_sta = {"0": "SUCCEEDED"}
    jr_dict = {}
    job_sta_dict = set_default_status(curr_exec_tables)
    fail_li = []
    print(json.dumps(job_sta_dict, indent=4))
    node_type = "G.2x"
    for lo in batch_by_load_order:
        # iterating through each load order
        if load_sta[prev_lo] != "SUCCEEDED":
            print(f"load status for load order {prev_lo} is - {load_sta[prev_lo]}; could not start {lo}")
            fail_li += [t for t in curr_exec_tables[lo]]
        else:
            jr_dict_lo = {}
            jr_sta_lo = {}
            fail_li_lo = []
            aws_failure_by_lo_dict = {}
            batch_counter = len(batch_by_load_order[lo])
            while batch_counter > 0:
                for batch in batch_by_load_order[lo]:
                    # iterating through each batch
                    jr_dict_batch = {}
                    count = len(batch_by_load_order[lo][batch])
                    while count > 0:
                        aws_fail_by_batch_dict = {}
                        for table, dpu in batch_by_load_order[lo][batch].items():
                            # iterate through each table
                            if table in prev_status \
                                    and prev_status[table] == "SUCCEEDED":
                                print(f"{current_datetime()} :: main :: "
                                      f"{table} - completed with status SUCCEEDED in previous execution for version - {version}")
                                continue
                            print(
                                f"{current_datetime()} :: main :: trigger job {job_name} for table {table} with {dpu} worker nodes")
                            table_nm = table.split('#')[0]
                            if table_nm in node_type_by_table:
                                node_type = node_type_by_table[table_nm]
                                if node_type.strip().upper() == "NA":
                                    node_type = "G.2x"
                            job_args = {
                                '--CONFIG_FILE': config_file,
                                '--S3_BUCKET': bucket,
                                '--TGT_NAME': tgt_name,
                                '--TABLE_NAME': table_nm,
                                '--LAYER': layer,
                                '--ARN': arn,
                                '--MARKET_BASKET': market_basket
                            }
                            if table_nm in load_type_by_table:
                                job_args['--LOAD_TYPE'] = load_type_by_table[table_nm]
                            if table_nm in id_li_by_table:
                                job_args['--ID_LIST'] = id_li_by_table[table_nm]
                            if table_nm in spark_properties_by_table:
                                job_args['--SPARK_PROPERTIES'] = spark_properties_by_table[table_nm]
                            if table_nm in decryption_flag_by_table:
                                job_args['--DECRYPTION_FLAG'] = decryption_flag_by_table[table_nm]
                            if table_nm in decrypt_file_count_by_table:
                                job_args['--FILE_COUNT'] = decrypt_file_count_by_table[table_nm]
                            if table_nm in stg_load_args_by_table:
                                job_args['--LOAD_ARG'] = stg_load_args_by_table[table_nm]
                            if table_nm in num_partition_by_table:
                                job_args['--NUM_PARTITIONS'] = num_partition_by_table[table_nm]
                            if order_by == 'rule_order':
                                job_args['--RULE_ORDER'] = str(lo)
                            if table_nm in schema_overwrite_by_table:
                                job_args['--SCHEMA_OVERWRITE'] = schema_overwrite_by_table[table_nm]
                            if table_nm in proc_type_by_table:
                                job_args['--PROCESSING_TYPE'] = proc_type_by_table[table_nm]
                            if table_nm in pre_proc_fn_by_table:
                                job_args['--PREPROCESSING_FUNCTION'] = pre_proc_fn_by_table[table_nm]
                            if table_nm in post_proc_fn_by_table:
                                job_args['--POSTPROCESSING_FUNCTION'] = post_proc_fn_by_table[table_nm]
                            if table_nm in header_flg_by_table:
                                job_args['--HEADERS_FLAG'] = header_flg_by_table[table_nm]
                            if table_nm in validation_fn_by_table:
                                job_args['--VALIDATION_FUNCTION'] = validation_fn_by_table[table_nm]
                            if table_nm in table_alias_flg_by_table:
                                job_args['--TABLE_ALIAS'] = table_alias_flg_by_table[table_nm]
                            if table_nm in touchfile_name_by_table:
                                job_args['--TOUCHFILE_NAME'] = touchfile_name_by_table[table_nm]
                            if table_nm in add_date_by_table:
                                job_args['--ADD_DATE'] = add_date_by_table[table_nm]
                            if table_nm in schema_exception_list_by_table:
                                job_args['--SCHEMA_EXCEPTION_LIST'] = schema_exception_list_by_table[table_nm]
                            if table_nm in part_keys_by_table:
                                job_args['--PARTITION_KEYS'] = part_keys_by_table[table_nm]
                            try:
                                jr_id = run_job(job_name, job_args, dpu, node_type)
                                intermediate_status_dict = get_previous_execution_status(done_file_full)
                                write_in_progress_json(jr_id, order_by, lo, table_nm, done_file, "RUNNING",
                                                       intermediate_status_dict)
                            except Exception as e:
                                print(f"error in run_job - {e}")
                                jr_id = None
                                if e.__class__.__name__ in ['ConcurrentRunsExceededException',
                                                            'ResourceNumberLimitExceededException',
                                                            'OperationTimeoutException']:
                                    print(
                                        f"{current_datetime()} :: main :: WARNING :: aws error {e.__class__.__name__} for job_args:- \n\n{json.dumps(job_args)}")
                                    aws_fail_by_batch_dict[table] = dpu
                            jr_dict_batch[table] = jr_id
                            time.sleep(10)

                        count = len(aws_fail_by_batch_dict)
                        if count > 0:
                            print(
                                f"{current_datetime()} :: main :: AWS failure(s) for batch {batch}  is {count} in load order {lo}. Trying again for  :- \n\n{json.dumps(aws_fail_by_batch_dict)}")
                            batch_by_load_order[lo][batch] = copy.deepcopy(aws_fail_by_batch_dict)
                            time.sleep(60)
                    print(
                        f"{current_datetime()} :: main :: Job list with respective job run id :- \n\n{json.dumps(jr_dict_batch)}")

                    # wait for jobs completion and capture job status
                    jr_sta_batch, fail_li_batch, aws_failures_by_batch_dict = wait_for_jobs_completion(job_name,
                                                                                                       jr_dict_batch,
                                                                                                       done_file_full,
                                                                                                       arn_to_write)
                    if len(aws_failures_by_batch_dict) > 0:
                        aws_failure_by_lo_dict[batch] = aws_failures_by_batch_dict
                    elif len(aws_failures_by_batch_dict) == 0:
                        aws_failure_by_lo_dict = {}
                    jr_dict_lo = {**jr_dict_lo, **jr_dict_batch}
                    jr_sta_lo = {**jr_sta_lo, **jr_sta_batch}
                    fail_li_lo = fail_li_lo + fail_li_batch

                batch_counter = len(aws_failure_by_lo_dict)
                if batch_counter > 0:
                    print(
                        f"{current_datetime()} :: main :: AWS failure(s) across all batches is {batch_counter} in load order {lo}. Trying again for  :- \n\n{json.dumps(aws_failure_by_lo_dict)}")
                    batch_by_load_order[lo] = copy.deepcopy(aws_failure_by_lo_dict)

            if not fail_li_lo:
                load_sta[lo] = "SUCCEEDED"
            else:
                load_sta[lo] = "FAILED"
            prev_lo = lo

        jr_dict = {**jr_dict, **jr_dict_lo}
        job_sta_dict = {**job_sta_dict, **jr_sta_lo}
        fail_li = fail_li_lo + fail_li

    # append skipped jobs' status into job status dictionary
    for t, p_dict in prev_status.items():
        if t not in job_sta_dict:
            job_sta_dict[t] = p_dict
    print(f"{current_datetime()} :: main :: Table list with respective status :- \n\n{json.dumps(job_sta_dict)}")

    # write job status dictionary into done file path
    put_s3_object(bucket, done_file, bytes(json.dumps(job_sta_dict, indent=4).encode("utf-8")), arn_to_write)
    unique_sta = list(set([el['status'] for el in job_sta_dict.values()]))

    # get previous execution details
    run_dtls = get_previous_execution_details(jr_file_full)
    # print(f"{current_datetime()} :: main :: previous execution details - ")
    # print(json.dumps(_run_dtls, indent=4))

    # capture current job run details
    for tbl, run_id in jr_dict.items():
        if tbl not in run_dtls:
            run_dtls[tbl] = {}
        if run_id is None:
            run_dtls[tbl][run_id] = None
        else:
            run_dtls[tbl][run_id] = get_current_execution_details(job_name, run_id)

    # print(f"{current_datetime()} :: main :: complete execution details - ")
    # print(json.dumps(run_dtls, indent=4))

    # write job run details dictionary into done file path
    put_s3_object(bucket, jr_file, bytes(json.dumps(run_dtls, indent=4).encode("utf-8")), arn_to_write)

    # if there are failed jobs, raise an exception
    fail_li = list(set(fail_li))
    if len(fail_li) > 0:
        print(traceback.format_exc())
        raise Exception(f"{len(fail_li)} jobs failed; failed job list is {fail_li}")
    elif len(unique_sta) > 1 or (len(unique_sta) == 1 and unique_sta[0] != "SUCCEEDED"):
        failed_dict = dict([(_j, _d) for _j, _d in job_sta_dict.items() if _d['status'] != 'SUCCEEDED'])
        print(traceback.format_exc())
        raise Exception(f"{len(failed_dict)} jobs failed; failed job list is {failed_dict}")
    else:
        print("job completed successfully")
