# import modules
import sys
import traceback
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from staging_utilities import *


def shs_claims_count_validation(file_li):
    table_alias_new = table_alias.replace("<version>", version[-4:]).replace("<mb>", market_basket.upper())
    cnt_val_config = validation_config["row_count"]
    cnt_val_file = cnt_val_config["filename"].replace("<version>", version).replace("<mb>", market_basket.upper())
    cnt_val_file_sep = cnt_val_config["delimiter"]
    cnt_val_file_head = cnt_val_config["headers"]
    cnt_val_file_skp_row = cnt_val_config["skip_rows"]
    cnt_val_file_df = read_pandas_df(bucket_key_to_s3_path(src_bucket, src_file_path) + cnt_val_file, cnt_val_file_head,
                                     cnt_val_file_sep, cnt_val_file_skp_row)
    if processing_type == "pattern":
        cnt_val_file_df = cnt_val_file_df[cnt_val_file_df["file_name"].str.contains(table_alias_new)]
    elif processing_type == "fixed":
        cnt_val_file_df = cnt_val_file_df[cnt_val_file_df["file_name"].str.strip() == table_alias_new]
    else:
        raise Exception(f"incorrect processing type - {processing_type}")

    print(f"records from control file considered for row count are - \n{cnt_val_file_df}")
    cnt_from_file = cnt_val_file_df.row_count.sum()
    print(f"count from control file for {table_name} - {cnt_from_file}")
    parquet_df = read_csv(sqlContext, bucket_keylist_to_s3_pathlist(src_bucket, file_li), delimiter=sep)
    df_count = parquet_df.count()
    print(f"count from dataframe for {table_name} - {df_count}")
    if cnt_from_file == df_count:
        print(f"shs_claims_count_validation - results - PASS")
        return True
    else:
        print(f"shs_claims_count_validation - results - FAIL")
        return False


def shs_claims_file_validation(file_li):
    file_val_config = validation_config["file_size"]
    file_val_file = file_val_config["filename"].replace("<version>", version).replace("<mb>", market_basket.upper())
    file_val_file_sep = file_val_config["delimiter"]
    file_val_file_head = file_val_config["headers"]
    file_val_file_skp_row = file_val_config["skip_rows"]
    file_val_file_df = read_pandas_df(bucket_key_to_s3_path(src_bucket, src_file_path) + file_val_file,
                                      file_val_file_head,
                                      file_val_file_sep, file_val_file_skp_row)
    fail_li = []
    for file in file_li:
        file_wo_path = file.split("/")[-1]
        file_size_in_ctl_df = file_val_file_df[file_val_file_df["file_name"] == file_wo_path]["file_size"]
        file_cnt_in_ctl_df = file_val_file_df[file_val_file_df["file_name"] == file_wo_path]["row_count"]
        if file_size_in_ctl_df.empty:
            fail_li.append(file)
            print(f"could not find file size from control file for {file_wo_path}")
        else:
            file_size_in_ctl = file_size_in_ctl_df.values[0]
            print(f"file size from control file for {file_wo_path} - {file_size_in_ctl}")

        if file_cnt_in_ctl_df.empty:
            fail_li.append(file)
            print(f"could not find row count from control file for {file_wo_path}")
        else:
            file_cnt_in_ctl = file_cnt_in_ctl_df.values[0]
            print(f"row count from control file for {file_wo_path} - {file_cnt_in_ctl}")
        response = s3_client.head_object(Bucket=src_bucket, Key=file)
        file_size_on_s3 = response['ContentLength']
        print(f"file size on s3 {file_wo_path} - {file_size_on_s3}")
        parquet_df = read_csv(sqlContext, bucket_key_to_s3_path(src_bucket, file), delimiter=sep)
        df_count = parquet_df.count()
        if headers_flg:
            df_count += 1
        print(f"count from dataframe for {table_name} - {df_count}")

        if file_size_in_ctl == file_size_on_s3 and file_cnt_in_ctl == df_count:
            pass
        else:
            fail_li.append(file)

    if not fail_li:
        print(f"shs_claims_file_validation - results - PASS")
        return True
    else:
        print(f"shs_claims_file_validation - results - FAIL")
        return False


def shs_pah_claims_file_validation(file_li):
    file_val_config = validation_config["file_size"]
    file_val_file = file_val_config["filename"].replace("<version>", version).replace("<mb>", market_basket.upper())
    file_val_file_sep = file_val_config["delimiter"]
    file_val_file_head = file_val_config["headers"]
    file_val_file_skp_row = file_val_config["skip_rows"]
    file_val_file_df = read_pandas_df(bucket_key_to_s3_path(src_bucket, src_file_path) + file_val_file,
                                      file_val_file_head,
                                      file_val_file_sep, file_val_file_skp_row)
    # print(file_val_file_df)
    fail_li = []
    for file in file_li:
        file_wo_path = file.split("/")[-1]
        file_size_in_ctl_df = file_val_file_df[file_val_file_df["file_name"] == file_wo_path]["file_size"]
        if file_size_in_ctl_df.empty:
            fail_li.append(file)
            print(f"could not find file size from control file for {file_wo_path}")
        else:
            file_size_in_ctl = file_size_in_ctl_df.values[0]
            print(f"file size from control file for {file_wo_path} - {file_size_in_ctl}")
        response = s3_client.head_object(Bucket=src_bucket, Key=file)
        file_size_on_s3 = response['ContentLength']
        print(f"file size on s3 {file_wo_path} - {file_size_on_s3}")

        if file_size_in_ctl == file_size_on_s3:
            pass
        else:
            fail_li.append(file)

    if not fail_li:
        print(f"shs_pah_claims_file_validation - results - PASS")
        return True
    else:
        print(f"shs_pah_claims_file_validation - results - FAIL")
        return False


def shs_pah_claims_count_validation(file_li):
    cnt_val_config = validation_config["row_count"]
    cnt_val_file = cnt_val_config["filename"].replace("<version>", version)
    cnt_val_file_sep = cnt_val_config["delimiter"]
    cnt_val_file_head = cnt_val_config["headers"]
    cnt_val_file_skp_row = cnt_val_config["skip_rows"]
    cnt_val_file_df = read_pandas_df(bucket_key_to_s3_path(src_bucket, src_file_path) + cnt_val_file, cnt_val_file_head,
                                     cnt_val_file_sep, cnt_val_file_skp_row)
    fail_li = []
    for file in file_li:
        file_wo_path = file.split("/")[-1]
        file_cnt_in_ctl_df = cnt_val_file_df[cnt_val_file_df["file_name"] == file_wo_path]["row_count"]

        if file_cnt_in_ctl_df.empty:
            fail_li.append(file)
            print(f"could not find row count from control file for {file_wo_path}")
        else:
            file_cnt_in_ctl = file_cnt_in_ctl_df.values[0]
            print(f"row count from control file for {file_wo_path} - {file_cnt_in_ctl}")
        parquet_df = read_csv(sqlContext, bucket_key_to_s3_path(src_bucket, file), delimiter=sep)
        df_count = parquet_df.count()
        if headers_flg:
            df_count += 1
        print(f"count from dataframe for {table_name} - {df_count}")

        if file_cnt_in_ctl == df_count:
            pass
        else:
            fail_li.append(file)

    if not fail_li:
        print(f"shs_pah_claims_count_validation - results - PASS")
        return True
    else:
        print(f"shs_pah_claims_count_validation - results - FAIL")
        return False


def iqvia_count_validation(file_li):
    cnt_val_config = validation_config["row_count"]
    cnt_val_file = cnt_val_config["filename"].replace("<version>", version)
    cnt_val_file_sep = cnt_val_config["delimiter"]
    cnt_val_file_head = cnt_val_config["headers"]
    cnt_val_file_skp_row = cnt_val_config["skip_rows"]
    cnt_val_file_df = read_pandas_df(bucket_key_to_s3_path(src_bucket, src_file_path) + cnt_val_file, cnt_val_file_head,
                                     cnt_val_file_sep, cnt_val_file_skp_row)
    fail_li = []
    for file in file_li:
        file_wo_path = rreplace(file.split("/")[-1], '.gz', '.TXT')
        file_cnt_in_ctl_df = cnt_val_file_df[cnt_val_file_df["file_name"] == file_wo_path]["row_count"]

        if file_cnt_in_ctl_df.empty:
            fail_li.append(file)
            file_cnt_in_ctl = None
            print(f"could not find row count from control file for {file_wo_path}")
        else:
            file_cnt_in_ctl = file_cnt_in_ctl_df.values[0]
            print(f"row count from control file for {file_wo_path} - {file_cnt_in_ctl}")

        parquet_df = read_csv(sqlContext, bucket_key_to_s3_path(src_bucket, file), delimiter=sep,
                              headers_flg=headers_flg)
        df_count = parquet_df.count()
        print(f"count from dataframe for {table_name} - {df_count}")

        if file_cnt_in_ctl == df_count:
            pass
        else:
            fail_li.append(file)

    if not fail_li:
        print(f"iqvia_count_validation - results - PASS")
        return True
    else:
        print(f"iqvia_count_validation - results - FAIL")
        return False


def deactivekeys_validation(file_li):
    df = read_csv(sqlContext, bucket_keylist_to_s3_pathlist(src_bucket, file_li), delimiter=sep,
                  headers_flg=headers_flg)
    df_count = df.count()
    print(f"count from dataframe for {table_name} - {df_count}")

    if load_type == "historical":
        if df_count == 0:
            print(f"deactivekeys_validation - results - PASS")
            return True
        else:
            print(f"deactivekeys_validation - results - FAIL")
            return False
    elif load_type == "incremental":
        if df_count != 0:
            print(f"deactivekeys_validation - results - PASS")
            return True
        else:
            print(f"deactivekeys_validation - results - FAIL")
            return False
    else:
        raise Exception(f"invalid load type - {load_type}")


def schema_validation(file_li):
    if "src" in schema_exception_list:
        src_except_li = [_.strip().lower() for _ in schema_exception_list["src"].split(",")]
    else:
        src_except_li = []
    if "tgt" in schema_exception_list:
        tgt_except_li = [_.strip().lower() for _ in schema_exception_list["tgt"].split(",")]
    else:
        tgt_except_li = []
    ing_map_df = read_mapping(ing_mapping_file, table_name, src_name)
    ing_map_df = ing_map_df[ing_map_df.cdm_column.str.strip() != '']
    ing_map_df = ing_map_df[~((ing_map_df.src_table.str.strip() == '')
                              & (ing_map_df.src_column.str.strip() == '')
                              & (ing_map_df.sql_tx.str.strip() == ''))
                            & (ing_map_df.cai_tx.str.strip() == '')]
    mapped_columns = set(sorted(c.strip().lower() for c in ing_map_df.cdm_column))
    print(f"columns present in mapping file - {mapped_columns}")
    source_df = staging_load(src_bucket, file_li, sep, file_to, obj_tp, headers_flg)
    source_columns = set(sorted(c.strip().lower() for c in source_df.columns))
    print(f"columns present in source file - {source_columns}")
    missing_cols = {
        "columns present in mapping but not in source feed": list(mapped_columns - source_columns - set(tgt_except_li)),
        "columns present in source but not in mapping file": list(source_columns - mapped_columns - set(src_except_li))}
    print(json.dumps(missing_cols, indent=4))
    if list(missing_cols.values())[0] == [] and list(missing_cols.values())[1] == []:
        print(f"schema_validation - results - PASS")
        return True
    else:
        print(f"schema_validation - results - FAIL")
        return False


def define_params(bucket, config_file, table_name, src_name):
    # parse the config file contents
    print(f"\n{current_datetime()} :: define_params :: info - reading the config file {config_file} "
          f"in bucket {bucket} ...\n")
    try:
        global src_bucket, src_file_path, des_file_path, input_dataset, version, tmp_file_path, validation_config, ing_mapping_file

        filename = get_s3_object(bucket, config_file)
        param_contents = json.loads(filename)
        version = param_contents["src_dataset"][src_name]["version"]
        if version.strip() == "":
            raise Exception(f"version can not be empty")

        # src_file_path = param_contents["src_dataset"][src_name]["archive_path"]. \
        #     replace("<version>", version). \
        #     replace("<market_basket>", market_basket.upper())

        src_file_path_full = param_contents["src_dataset"][src_name]["inbound_path"]. \
            replace("<version>", version). \
            replace("<market_basket>", market_basket.upper())

        src_bucket, src_file_path = s3_path_to_bucket_key(src_file_path_full)

        des_file_path = param_contents["root_path"] + param_contents["stg_base_dir"] + "_validation/" + add_slash(
            src_name) + add_slash(
            market_basket) + add_slash(table_name)

        tmp_file_path = param_contents["root_path"] + param_contents["stg_base_dir"] + "_validation-temp/" + add_slash(
            src_name) + add_slash(
            market_basket) + add_slash(table_name)

        input_dataset = param_contents["input_dataset"]
        ing_mapping_file = param_contents["ing_mapping"]

        if "source_file_validation_pattern" in param_contents["src_dataset"][src_name]:
            validation_config = param_contents["src_dataset"][src_name]["source_file_validation_pattern"]
        else:
            validation_config = {}

    except Exception as err:
        print(f"{current_datetime()} :: define_params :: error - failed to read the config file {config_file} "
              f"in bucket {bucket}")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: define_params :: info - successfully read the config file {config_file} "
              f"in bucket {bucket}\n")
        print(f"{current_datetime()} :: define_params :: info - src_file_path      : {src_file_path}")
        print(f"{current_datetime()} :: define_params :: info - des_file_path      : {des_file_path}")
        print(f"{current_datetime()} :: define_params :: info - tmp_file_path      : {tmp_file_path}")
        print(f"{current_datetime()} :: define_params :: info - input_dataset      : {input_dataset}")
        print(f"{current_datetime()} :: define_params :: info - data_version       : {version}")
        print("*" * 150)


if __name__ == "__main__":
    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
    try:
        args = getResolvedOptions(sys.argv,
                                  ['S3_BUCKET', 'CONFIG_FILE', 'MARKET_BASKET', 'TGT_NAME', 'TABLE_NAME',
                                   'PROCESSING_TYPE', 'PREPROCESSING_FUNCTION', 'LOAD_TYPE', 'VALIDATION_FUNCTION',
                                   'HEADERS_FLAG', 'TABLE_ALIAS', 'SCHEMA_EXCEPTION_LIST', 'JOB_NAME'])
        bucket = args['S3_BUCKET']
        config_file = args['CONFIG_FILE']
        src_name = args['TGT_NAME'].strip().lower()
        market_basket = '' if args['MARKET_BASKET'] in ['""', "NA"] else args['MARKET_BASKET'].strip().lower()
        table_name = args['TABLE_NAME'].strip().lower()
        table_alias = args['TABLE_ALIAS']
        processing_type = args['PROCESSING_TYPE'].strip().lower()
        pre_processing_fn = args['PREPROCESSING_FUNCTION'].strip().lower()
        headers_flg = args['HEADERS_FLAG'].strip().lower()
        headers_flg = False if headers_flg == "false" else True
        load_type = args['LOAD_TYPE'].strip().lower()
        validation_fn_li = args['VALIDATION_FUNCTION'].strip().lower().split(",")
        validation_fn_li = [fn.strip() for fn in validation_fn_li if fn.strip() not in ['""', 'na']]
        pre_processing_fn = '' if pre_processing_fn in ['""', "na"] else pre_processing_fn
        schema_exception_list = args['SCHEMA_EXCEPTION_LIST']
        schema_exception_list = '{}' if schema_exception_list.strip().lower() in ['""', "na"] else schema_exception_list
        schema_exception_list = json.loads(schema_exception_list)
        job_name = args['JOB_NAME']
        job_run_id = args['JOB_RUN_ID']
    except Exception as e:
        print(f"{current_datetime()} :: main :: error - could not read glue code arguments\n")
        print("error details : ", e)
        raise e
    else:
        print(f"{current_datetime()} :: main :: info - bucket                  : {bucket}")
        print(f"{current_datetime()} :: main :: info - config_file             : {config_file}")
        print(f"{current_datetime()} :: main :: info - market_basket           : {market_basket}")
        print(f"{current_datetime()} :: main :: info - src_name                : {src_name}")
        print(f"{current_datetime()} :: main :: info - table_name              : {table_name}")
        print(f"{current_datetime()} :: main :: info - table_alias             : {table_alias}")
        print(f"{current_datetime()} :: main :: info - processing_type         : {processing_type}")
        print(f"{current_datetime()} :: main :: info - pre_processing_fn       : {pre_processing_fn}")
        print(f"{current_datetime()} :: main :: info - load_type               : {load_type}")
        print(f"{current_datetime()} :: main :: info - validation_fn_li        : {validation_fn_li}")
        print(f"{current_datetime()} :: main :: info - headers_flg             : {headers_flg}")
        print(f"{current_datetime()} :: main :: info - schema_exception_list   : {schema_exception_list}")
        print(f"{current_datetime()} :: main :: info - job_name                : {job_name}")
        print(f"{current_datetime()} :: main :: info - job_run_id              : {job_run_id}")

    if load_type not in (f"historical", "incremental"):
        raise Exception(f"invalid load type - {load_type}; only supported values are 'historical', 'incremental'")

    if validation_fn_li:
        time.sleep(1)
        print(f"\n\n****** START - {table_name} ******")
        try:
            print(f"{current_datetime()} :: main :: step 1 - define params")
            define_params(bucket, config_file, table_name, src_name)
            print(f"{current_datetime()} :: main :: cleanup staging & temp path")
            delete_s3_folder(bucket, des_file_path)
            delete_s3_folder(bucket, tmp_file_path)
            print(f"{current_datetime()} :: main :: step 2 - get input dataset")
            df = get_input_dataset_for_table(bucket, src_name, table_name, market_basket, input_dataset)
            print(f"{current_datetime()} :: main :: step 3 - get file list to process")
            li, sep, file_to, obj_tp, qt = get_file_list_to_process(df, src_bucket, src_file_path, table_name, version,
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
                print(f"{current_datetime()} Multiple dict values returned for table {table_name}, "
                      f"src_name - {src_name}")

            if is_file_available == '1':
                print(f"{current_datetime()} :: main :: step 4 - Total File count :: {len(li)}")
                if len(li) == 0:
                    print(f"{current_datetime()} :: main :: ERROR ::: failing with no files to process")
                    raise Exception("No file to process.")
                print(f"{current_datetime()} :: main :: step 5 - perform staging load")

                if pre_processing_fn != '':
                    print(f"{current_datetime()} :: pre-processing with function {pre_processing_fn}")
                    fn_call = f"{pre_processing_fn}(bucket='{src_bucket}', files_li={li}, path='{tmp_file_path}')"
                    print(fn_call)
                    final_file_li = eval(fn_call)
                else:
                    print(f"{current_datetime()} :: pre-processing not applicable")
                    final_file_li = li

                result_dict = {}
                for fn in validation_fn_li:
                    val_fn_call = f"{fn}(final_file_li)"
                    print(val_fn_call)
                    res = eval(val_fn_call)
                    result_dict[fn] = res

                if len(set(result_dict.values())) == 1 and list(set(result_dict.values()))[0] == True:
                    print(f"source feed validated successfully")
                else:
                    raise Exception(
                        f"there are differences in source feeds and control files, see log files for more details")

                print(f"{current_datetime()} :: main :: step 6 - clean-up temp path {tmp_file_path}")
                delete_s3_folder(src_bucket, tmp_file_path)
            else:
                print(f"{current_datetime()} :: Skipping the load process as file is not present")
        except Exception as e:
            print("ERROR DETAILS - ", e)
            print(traceback.format_exc())
            raise e
        time.sleep(1)
        print(f"\n\n****** END - {table_name} ******")
