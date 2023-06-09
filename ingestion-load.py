import sys

import pandas as pd
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit, concat_ws, sha2, row_number, col, to_date, md5, to_timestamp, coalesce
from pyspark.sql.window import Window
import traceback
from cdm_utilities import *
from incremental_load_functions import *

# initialize script variables
format_length = 150


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def initialize_spark_context(spark_props):
    sc = SparkContext(conf=SparkConf()
                      .set("spark.driver.maxResultSize", "0")
                      .set('spark.sql.legacy.timeParserPolicy', 'LEGACY')
                      # .set('spark.sql.autoBroadcastJoinThreshold', broadcast_threshold_value)
                      .set('spark.sql.parquet.fs.optimized.committer.optimization-enabled', True)
                      # .set('spark.sql.shuffle.partitions', shuffle_partition_value)
                      ).getOrCreate()
    sqlContext = SQLContext(sc)
    sc.setLogLevel("Error")

    di = ast.literal_eval(spark_props)
    print(di)

    for k, v in di.items():
        print(f"{k} = {v}")
        sqlContext.sql(f'set {k}={v}')

    # Setting S3 max connection to 100
    hc = sc._jsc.hadoopConfiguration()
    hc.setInt("fs.s3.connection.maximum", 100)
    hc.setInt("fs.s3.maxRetries", 20)
    hc.set("fs.s3.multiobjectdelete.enable", "false")
    return sc, sqlContext


def validate_rule_order(df):
    rule_orders = df.rule_order.unique().tolist()
    print("all rule_orders    :: ", rule_orders)
    print("current rule_order :: ", rule_order)
    if rule_order not in rule_orders:
        raise Exception(f'Rule order {rule_order} not present for target {tgt_name} and table {table_name}')


def define_params(bucket, config_file, table_name, tgt_name):
    # parse the config file contents
    print(f"\t{current_datetime()} :: define_params :: "
          f"info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:
        global source_path, target_path, tmp_path, full_mapping_df, batch_date, input_dataset, param_contents, \
            hash_reqd, load_type, load_fn_name, load_fn_args, custom_flag, sql_script_path, master_path, version

        param_data = get_s3_object(bucket, config_file)
        param_contents = json.loads(param_data)

        sql_script_path = param_contents['sql_script_path']
        input_dataset = param_contents["input_dataset"]
        mapping_file = param_contents["ing_mapping"]
        batch_date = param_contents["batch_date"]
        version = param_contents["src_dataset"][tgt_name]["version"]

        print(f"\t{current_datetime()} :: define_params :: reading mapping from {mapping_file}")
        orig_mapping_df = read_mapping(mapping_file, table_name, tgt_name)
        validate_rule_order(orig_mapping_df)
        orig_mapping_df = orig_mapping_df[orig_mapping_df.rule_order == rule_order]
        full_mapping_df = orig_mapping_df[orig_mapping_df.cai_tx.str.strip() == '']
        if full_mapping_df.empty:
            raise Exception(f"No mapping available for cdm_name - {tgt_name}, cdm_table - {table_name}")

        # get custom flag
        custom_flag = get_custom_flag(full_mapping_df)

        # get incremental load arguments
        inc_args = get_inc_load_args(full_mapping_df)
        if inc_args is None:
            raise Exception(
                f"incremental load arguments are not defined for cdm_name - {tgt_name}, cdm_table - {table_name}")
        load_fn_name = list(inc_args)[0]
        load_fn_args = inc_args[load_fn_name]
        hash_reqd = load_fn_args["hash_key_reqd"]

        root_path = param_contents["root_path"]
        src_base_dir = param_contents["stg_base_dir"] if custom_flag == "N" else param_contents["ing_base_dir"]
        tgt_base_dir = param_contents["ing_base_dir"]
        tmp_base_dir = tgt_base_dir + "_temp"
        master_base_dir = param_contents["master_base_dir"]

        source_path_wo_prefix = root_path + add_slash(src_base_dir)
        source_path = bucket_key_to_s3_path(bucket, source_path_wo_prefix)
        master_path_wo_prefix = root_path + add_slash(master_base_dir)
        master_path = bucket_key_to_s3_path(bucket, master_path_wo_prefix)
        target_path_wo_prefix = root_path + add_slash(tgt_base_dir) + add_slash(tgt_name) + add_slash(table_name)
        target_path = bucket_key_to_s3_path(bucket, target_path_wo_prefix)
        tmp_path_wo_prefix = root_path + add_slash(tmp_base_dir) + add_slash(tgt_name) + add_slash(table_name)
        tmp_path = bucket_key_to_s3_path(bucket, tmp_path_wo_prefix)

    except Exception as err:
        print(f"\t{current_datetime()} :: define_params :: error - "
              f"failed to read the config file {config_file} in bucket {bucket}")
        print("\terror details : ", err)
        raise err
    else:
        print(f"\t{current_datetime()} :: define_params :: info - "
              f"successfully read the config file {config_file} in bucket {bucket}\n")
        print(f"\n{current_datetime()} :: define_params :: info - sql script path                - {sql_script_path}")
        print(f"\n{current_datetime()} :: define_params :: info - input_dataset                  - {input_dataset}")
        print(f"\t{current_datetime()} :: define_params :: info - source_path                    - {source_path}")
        print(f"\t{current_datetime()} :: define_params :: info - target_path                    - {target_path}")
        print(f"\t{current_datetime()} :: define_params :: info - temp path                      - {tmp_path}")
        print(f"\t{current_datetime()} :: define_params :: info - incremental load arguments     - "
              f"{json.dumps(inc_args, indent=4)}")
        print(f"\t{current_datetime()} :: define_params :: info - incremental load function name - {load_fn_name}")
        print(f"\t{current_datetime()} :: define_params :: info - incremental load function args - {load_fn_args}")
        print(f"\t{current_datetime()} :: define_params :: info - Hash key generation required ? - {hash_reqd}")


def create_union_structure(df_li):
    if s3_path_exists(remove_slash(tmp_path) + "_tmp/"):
        print(f"\t{current_datetime()} :: create_union_structure :: cleaning up {remove_slash(tmp_path) + '_tmp/'}")
        delete_s3_folder_spark(sqlContext, remove_slash(tmp_path) + "_tmp/")
    df_len = len(df_li)
    print(f"\t{current_datetime()} :: create_union_structure :: combining {df_len} dataframes")
    # get unique column list from all transformed dataframes
    unique_col_li = get_all_columns_from_df_list(df_li)
    # create combined structure
    for i, df in enumerate(df_li):
        print(f"\t{current_datetime()} :: create_union_structure :: start :: df - {i + 1}")
        for c in unique_col_li:
            if c not in df.columns:
                df = df.withColumn(c, lit(None).cast(unique_col_li[c]))
            else:
                df = df.withColumn(c, col(c).cast(unique_col_li[c]))
        if i == 0:
            sel_li = df.columns
        print(f"\t{current_datetime()} :: create_union_structure :: {i} - count - {df.count()}")
        df.select(sel_li).write.parquet(path=remove_slash(tmp_path) + "_tmp/", mode="Append", compression="snappy")
        print(f"\t{current_datetime()} :: create_union_structure :: end :: df - {i + 1}")


def get_input_dataset(df):
    # filter input dataset file for given source and source tables, convert into dictionary format
    src_name = get_source_list(df)
    src_tbl_li = get_source_table_list(df)
    print(src_name)
    print(src_tbl_li)
    buc, filekey = s3_path_to_bucket_key(input_dataset)
    dataset_data = get_s3_object(buc, filekey)
    dataset_df = pd.read_csv(BytesIO(dataset_data)).fillna('')
    dataset_df = dataset_df[((dataset_df.src.str.strip().str.lower().isin(src_name)) &
                             (dataset_df.dataset.str.strip().str.lower().isin(src_tbl_li)))]
    return dataset_df


def read_src_data(df):
    # to read source data into dataframe
    sql_file_nm = get_sql_file_name(df)  # get sql file name
    src_sql = get_src_sql(df, bucket, sql_script_path)  # get source sql
    src_df = {}  # initialize source dataframe dictionary
    global deactive_path  # deactive keys file path

    if custom_flag == "N":  # if not custom table, read data from staging else from ingestion
        # src = get_src_name(df)  # get source name
        inp_ds_df = get_input_dataset(df)
        deact_tbl_nm = load_fn_args["deactive_file"].lower() if load_fn_name.lower() == "deact_key_load" else None
        print(inp_ds_df)
        for i in inp_ds_df.index:
            file_exists_flg = str(inp_ds_df["is_file_available"][i]).strip()
            mkt = inp_ds_df["data_source"][i]
            src = inp_ds_df["src"][i]
            src_tbl = inp_ds_df["dataset"][i]
            table_w_mkt = f"{src_tbl}_{mkt}"
            path = source_path + add_slash(src) + add_slash(mkt.lower()) + add_slash(src_tbl.lower())
            print(path)
            if not s3_path_exists(path):
                if file_exists_flg == '1':
                    raise Exception(f"{path} not found")
                else:
                    print(f"{table_name} marked unavailable in input dataset, skipping the current iteration...")
                    continue
            deactive_path = rreplace(path, table_name.lower(), deact_tbl_nm) if deact_tbl_nm is not None else None
            src_df[table_w_mkt] = read_parquet(sqlContext, path)
            if src_df[table_w_mkt] is None:
                raise Exception(f"read_src_data : could not load {table_w_mkt} into dataframe")

    else:
        if sql_file_nm is not None:
            src = get_src_name(df)  # get source name
            src_tbl_li = [_ for _ in src_sql.split(" ") if '<ingestion_schema>.' in _ or '<master_schema>.' in _]
            src_tbl_li = set([_.replace('<ingestion_schema>.', "").replace('<master_schema>.', "") for _ in src_tbl_li])
            src_tbl_li = list(src_tbl_li)
            src_tbl_di = {src: src_tbl_li}
        else:
            src_tbl_di = get_grouped_tables_by_sources(df)
        print(f"src_tbl_di : {src_tbl_di}")
        for src, src_tbl_li in src_tbl_di.items():
            for src_tbl in src_tbl_li:
                path = source_path + add_slash(src) + add_slash(src_tbl.lower())
                master_file_path = master_path + add_slash(src_tbl.lower())
                if s3_path_exists(path):
                    src_df[src_tbl] = read_active_records_from_parquet(sqlContext, path)
                elif s3_path_exists(master_file_path):
                    src_df[src_tbl] = read_parquet(sqlContext, master_file_path)
                elif not s3_path_exists(path):
                    buc, key = s3_path_to_bucket_key(path)
                    isFileMarkedRequired = check_src_file_validity(buc, param_contents, "INGESTION_CUSTOM",
                                                                   src_tbl.lower(), src)
                    if isFileMarkedRequired:
                        raise Exception(
                            f"Table {src_tbl} and source {src} marked required in input dataset but file not present in the s3 path {key}")
                    else:
                        print(
                            f"load_all_tables -  {src_tbl} marked unavailable in input dataset, skipping the current iteration...")
                        continue

    print(f"\t{current_datetime()} :: read_src_data :: read dataframes are - {list(src_df)}")
    return src_df


def transform_data():
    # get rules order and rule set list
    print(f"\t{current_datetime()} :: transform_data :: get rules list")
    ro_rs_li = get_ruleorder_ruleset(full_mapping_df)
    # iterate through each rule in rules order and rule set list
    trf_df_li = []
    for ro, rs_li in ro_rs_li.items():
        print(f"\t{current_datetime()} :: transform_data :: start :: rule order {ro}")
        for rs in rs_li:
            print(f"\t{current_datetime()} :: transform_data :: start :: rule set {rs}")
            # get current mapping
            curr_mapping = full_mapping_df[(full_mapping_df.rule_order == ro) & (full_mapping_df.rule_set == rs)]
            cfg_mapping_df = curr_mapping[
                ~((curr_mapping.src_table != '') | (curr_mapping.src_column != '') | (curr_mapping.sql_tx != ''))]
            inp_ds_df = get_input_dataset(curr_mapping)  # reading input dataset info here
            main_tbl = get_main_table(curr_mapping)
            print(f"\t{current_datetime()} :: transform_data :: driving table is - {main_tbl}")
            main_ind = -1

            print(f"\t{current_datetime()} :: transform_data :: read source data into dataframes")
            dfs = read_src_data(curr_mapping)  # reading source data
            # Iterate through each row in input src_name; each row signifies a market_basket (data_source).
            # For each market_basket, generate sql from src_mapping_df and append to same table in ing_tmp_path.
            # Reading each data in respective dataframe and combining with union is time consuming process.
            if custom_flag == "N":
                mkt_li = pd.unique(inp_ds_df["data_source"])
                for mkt in mkt_li:
                    mkt_inp_ds_df = inp_ds_df[inp_ds_df.data_source == mkt]
                    print(
                        f"\t{current_datetime()} :: transform_data :: input dataset filtered for market is - {mkt_inp_ds_df}")
                    print(f"\t{current_datetime()} :: transform_data :: start :: data source {mkt}")
                    for i in mkt_inp_ds_df.index:
                        skip_flag = False
                        # file_exists_flg = str(inp_ds_df["is_file_available"][i]).strip()
                        # mkt = inp_ds_df["data_source"][i]
                        src_table = mkt_inp_ds_df["dataset"][i]
                        if src_table.lower() == main_tbl.lower():
                            main_ind = i
                        table_w_mkt = f"{src_table}_{mkt}"
                        if table_w_mkt not in dfs:
                            skip_flag = True
                            continue
                        df = dfs[table_w_mkt]
                        df.createOrReplaceTempView(src_table)
                        print(f"\t{current_datetime()} :: transform_data :: {table_w_mkt} - count - {df.count()}")
                    if skip_flag:
                        continue
                    print(f"\t{current_datetime()} :: transform_data :: start :: "
                          f"data source {mkt_inp_ds_df['data_source_id'][i]}")
                    print(f"\t{current_datetime()} :: transform_data :: get source sql")
                    src_sql = get_src_sql(curr_mapping, bucket, sql_script_path)
                    print(f"\t{current_datetime()} :: transform_data :: source sql is - {src_sql}")
                    print(
                        f"\t{current_datetime()} :: transform_data :: executing sql and capturing output in dataframe")
                    trf_df = sqlContext.sql(src_sql)
                    print(f"\t{current_datetime()} :: transform_data :: transformed data - count - {trf_df.count()}")
                    print(f"\t{current_datetime()} :: transform_data :: capturing config info")
                    cfg_dict = {}
                    main_tbl_inp_ds_df = mkt_inp_ds_df[
                        mkt_inp_ds_df.dataset.str.lower().str.strip() == main_tbl.lower()]
                    for j in cfg_mapping_df.index:
                        cfg_dict[cfg_mapping_df["cdm_column"][j]] = main_tbl_inp_ds_df[cfg_mapping_df["cdm_column"][j]][
                            main_ind]
                    print(f"\t{current_datetime()} :: transform_data :: appending config info - {cfg_dict}")
                    for key, val in cfg_dict.items():
                        trf_df = trf_df \
                            .withColumn(key, lit(val))
                    distinct_row_mapping = curr_mapping[curr_mapping.src_unique_row_arg.str.strip() != '']
                    if not distinct_row_mapping.empty:
                        print(f"\t{current_datetime()} :: transform_data :: Found duplicate delete script. Starting...")
                        drop_dup_query_dict = json.loads(distinct_row_mapping['src_unique_row_arg'].iloc[0])
                        trf_df = get_distinct_rows(sqlContext, trf_df, drop_dup_query_dict, table_name)
                        print(f"\t{current_datetime()} :: transform_data :: Duplicate delete script ended.")

                    trf_df_li.append(trf_df)
                    print(f"\t{current_datetime()} :: transform_data :: end :: "
                          f"data source {mkt_inp_ds_df['data_source_id'][i]}")
                    print(f"\t{current_datetime()} :: transform_data :: end :: data source {mkt}")

            else:
                for vw_nm, df in dfs.items():
                    df.createOrReplaceTempView(vw_nm)
                    print(f"\t{current_datetime()} :: transform_data :: {vw_nm} - count - {df.count()}")
                print(f"\t{current_datetime()} :: transform_data - custom table :: get source sql")
                src_sql = get_src_sql(curr_mapping, bucket, sql_script_path).replace('<ingestion_schema>.', '').replace(
                    '<master_schema>.', '')
                print(f"\t{current_datetime()} :: transform_data - custom table :: source sql is - {src_sql}")
                print(f"\t{current_datetime()} :: transform_data - custom table :: "
                      f"executing sql and capturing output in dataframe")
                trf_df = sqlContext.sql(src_sql)
                distinct_row_mapping = curr_mapping[curr_mapping.src_unique_row_arg.str.strip() != '']
                if not distinct_row_mapping.empty:
                    print(f"\t{current_datetime()} :: transform_data :: Found duplicate delete script. Starting...")
                    drop_dup_query_dict = json.loads(distinct_row_mapping['src_unique_row_arg'].iloc[0])
                    trf_df = get_distinct_rows(sqlContext, trf_df, drop_dup_query_dict, table_name)
                    print(f"\t{current_datetime()} :: transform_data :: Duplicate delete script ended.")

                trf_df_li.append(trf_df)
            print(f"\t{current_datetime()} :: transform_data :: end :: rule set {rs}")
        print(f"\t{current_datetime()} :: transform_data :: end :: rule order {ro}")
    return trf_df_li


def load_tgt_data():
    trf_df_li = transform_data()
    create_union_structure(trf_df_li)
    # read from ingestion temp path and load into source dataframe
    print(f"\t{current_datetime()} :: load_tgt_data :: read unionised data")
    if hash_reqd.upper() == "Y":
        src_df = read_parquet(sqlContext, remove_slash(tmp_path) + "_tmp/").distinct()
    else:
        src_df = read_parquet(sqlContext, remove_slash(tmp_path) + "_tmp/")
    print(f"\t{current_datetime()} :: load_tgt_data :: {remove_slash(tmp_path) + '_tmp/'} - count - {src_df.count()}")
    if src_df is None:
        raise Exception(f"could not load {remove_slash(tmp_path) + '_tmp/'}")

    distinct_row_mapping = full_mapping_df[full_mapping_df.tgt_unique_row_arg.str.strip() != '']
    if not distinct_row_mapping.empty:
        print(f"\t{current_datetime()} :: load_tgt_data :: Found duplicate delete script. Starting...")
        drop_dup_query_dict = json.loads(distinct_row_mapping['tgt_unique_row_arg'].iloc[0])
        src_df = get_distinct_rows(sqlContext, src_df, drop_dup_query_dict, table_name)
        print(f"\t{current_datetime()} :: load_tgt_data :: Duplicate delete script ended.")

    tgt_type = get_target_type(full_mapping_df)
    if "dim" in tgt_type:
        # identify the list of columns to generate hash
        scd_col_list = sorted(full_mapping_df[(full_mapping_df.scd.str.lower().str.strip() == 'y') &
                                              (full_mapping_df.cdm_column.str.lower().str.strip() != '')][
                                  "cdm_column"].str.lower().str.strip().tolist())
        scd_col_list = sorted(src_df.columns) if not scd_col_list else scd_col_list
    else:
        scd_col_list = sorted(src_df.columns)

    print(f"\t{current_datetime()} :: load_tgt_data :: append audit columns and write unique data in temp location")
    # append audit columns to dataframe
    src_df = src_df \
        .withColumn("add_date", to_date(lit(batch_date), "yyyyMMdd")) \
        .withColumn("delete_date", to_date(lit("99991231"), "yyyyMMdd")) \
        .withColumn("last_modified_date", to_timestamp(lit(current_datetime()))) \
        .withColumn("is_active", lit(1)) \
        .withColumn("is_incremental", lit(1)) \
        .withColumn("data_version", lit(version))

    if hash_reqd.upper() == "Y":
        src_df = src_df.withColumn("hashkey",
                                   md5(concat_ws("-", *[coalesce(col(c).cast(StringType()), lit('-')) for c in
                                                        sorted(scd_col_list)])))

    print(f"\t{current_datetime()} :: load_tgt_data :: {tmp_path} - count - {src_df.count()}")
    write_parquet(src_df, tmp_path)
    delete_s3_folder_spark(sqlContext, remove_slash(tmp_path) + "_tmp/")
    print(f"\t{current_datetime()} :: load_tgt_data :: ingestion transformation completed")

    # apply incremental load functions
    print(f"{current_datetime()} :: load_tgt_data :: apply incremental load functions")
    delete_date = get_prev_date(batch_date)
    if load_fn_name.lower() == "deact_key_load":
        fn_call = f"{load_fn_name.lower()}(sqlContext, sc, src_path='{tmp_path}', tgt_path='{target_path}', deact_path='{deactive_path}', inc_args={load_fn_args}, del_date='{delete_date}', schema_overwrite='{schema_overwrite}')"
    elif load_fn_name.lower() == 'except_key_column_checksum_load':
        join_cnd = get_scd_join_condition(full_mapping_df)
        fn_call = f"{load_fn_name.lower()}(sqlContext, sc, src_path='{tmp_path}', tgt_path='{target_path}', inc_args='{join_cnd}', del_date='{delete_date}', schema_overwrite='{schema_overwrite}')"
    else:
        fn_call = f"{load_fn_name.lower()}(sqlContext, sc, src_path='{tmp_path}', tgt_path='{target_path}', inc_args={load_fn_args}, del_date='{delete_date}', schema_overwrite='{schema_overwrite}')"
    print(fn_call)
    response = eval(fn_call)
    load_sta = response['status']
    message = response['text']
    if load_sta.upper() != 'SUCCESS':
        raise Exception(message)
    print(f"\t{current_datetime()} :: load_tgt_data :: move data into target path")
    move_s3_folder_spark_with_partition(sqlContext, message, target_path, part_keys)
    # move_s3_folder_spark(sqlContext, message, target_path)
    print(f"\t{current_datetime()} :: load_tgt_data :: job execution completed")


# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
args = getResolvedOptions(sys.argv,
                          ['S3_BUCKET', 'CONFIG_FILE', 'TABLE_NAME', 'SPARK_PROPERTIES', 'TGT_NAME', 'JOB_NAME',
                           'SCHEMA_OVERWRITE', 'RULE_ORDER', 'PARTITION_KEYS'])
try:
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    table_name = args['TABLE_NAME'].lower()
    tgt_name = args['TGT_NAME'].lower()
    part_keys = args['PARTITION_KEYS'].strip().lower()
    # part_keys = (', '.join('"' + item.strip() + '"' for item in part_keys.split(",")))
    rule_order = int(args['RULE_ORDER'])
    spark_properties = args['SPARK_PROPERTIES']
    job_name = args['JOB_NAME']
    job_run_id = args['JOB_RUN_ID']
    schema_overwrite = 'Y' if args['SCHEMA_OVERWRITE'].strip().upper() == 'Y' else 'N'

except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")
    print(f"{current_datetime()} :: main :: info - table_name       : {table_name}")
    print(f"{current_datetime()} :: main :: info - tgt_name         : {tgt_name}")
    print(f"{current_datetime()} :: main :: info - spark_properties : {spark_properties}")
    print(f"{current_datetime()} :: main :: info - job_name         : {job_name}")
    print(f"{current_datetime()} :: main :: info - job_run_id       : {job_run_id}")
    print(f"{current_datetime()} :: main :: info - schema_overwrite : {schema_overwrite}")
    print(f"{current_datetime()} :: main :: info - partition keys   : {part_keys}")

    time.sleep(1)
    print("*" * format_length)

time.sleep(1)
print(f"\n\n****** START - {table_name} ******")
# if spark_properties != '':
#     spark_properties = json.loads(spark_properties)
#     shuffle_partition_value = int(spark_properties["shuffle_partition"]) if spark_properties[
#                                                                                 "shuffle_partition"] != '' else 300
#     broadcast_threshold_value = int(spark_properties["broadcast_threshold"]) if spark_properties[
#                                                                                     "broadcast_threshold"] != '' else 2097152000
# else:
#     broadcast_threshold_value = 2097152000
#     shuffle_partition_value = 300

# print(f"broadcast_threshold_value :: {broadcast_threshold_value}")
# print(f"shuffle_partition_value :: {shuffle_partition_value}")
try:
    print(f"{current_datetime()} :: main :: step 0 - Initialize Spark Context with the defined properties")
    sc, sqlContext = initialize_spark_context(spark_properties)
    print(f"Spark set properties :: {sc.getConf().getAll()}")
    print(f"{current_datetime()} :: main :: step 1 - define params")
    define_params(bucket, config_file, table_name, tgt_name)
    print(f"{current_datetime()} :: main :: step 2 - read source data, apply transformations and load target data")
    load_tgt_data()
except Exception as e:
    print("ERROR DETAILS - ", e)
    print(traceback.format_exc())
    raise e
time.sleep(1)
print(f"\n\n****** END - {table_name} ******")
