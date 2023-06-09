# built-in libraries
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit, col
from awsglue.utils import getResolvedOptions
import sys
import traceback

# user defined libraries
from cdm_utilities import *


# initialize spark context and sqlcontext
sc = SparkContext(conf=SparkConf()
                  .set('spark.sql.autoBroadcastJoinThreshold', 524288000)
                  .set('spark.sql.parquet.fs.optimized.committer.optimization-enabled', True)
                  .set('spark.sql.shuffle.partitions', 300)
                  ).getOrCreate()
sqlContext = SQLContext(sc)
sc.setLogLevel("Error")

# Setting S3 max connection to 100
hc = sc._jsc.hadoopConfiguration()
hc.setInt("fs.s3.connection.maximum", 100)
hc.set("fs.s3.multiobjectdelete.enable", "true")

# initialize script variables
format_length = 150


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def define_params(bucket, config_file, table_name, arn):
    # parse the config file contents
    print(
        f"\n\t{current_datetime()} :: define_params :: info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:

        global source_path, target_path, tmp_path, master_path, icdm_post_fns, \
            custom_flag, tgt_bucket, param_contents, full_mapping_df, audit_columns, batch_date

        param_data = get_s3_object(bucket, config_file, arn)
        param_contents = json.loads(param_data)
        tgt_bucket = bucket if arn is None else param_contents["cdm_account_details"]["bucket"]
        audit_columns = param_contents['audit_columns']
        mapping_file = param_contents["icdm_mapping"]
        batch_date = param_contents["batch_date"]

        root_path = param_contents["root_path"]
        src_base_dir = param_contents["masked_filtered_base_dir"]
        tgt_base_dir = param_contents["icdm_base_dir"]
        tmp_base_dir = tgt_base_dir + "_temp"
        master_base_dir = param_contents["master_filtered_base_dir"]

        source_path_wo_prefix = root_path + add_slash(src_base_dir)
        source_path = bucket_key_to_s3_path(tgt_bucket, source_path_wo_prefix)
        target_path_wo_prefix = root_path + add_slash(tgt_base_dir)
        target_path = bucket_key_to_s3_path(tgt_bucket, target_path_wo_prefix)
        tmp_path_wo_prefix = root_path + add_slash(tmp_base_dir)
        tmp_path = bucket_key_to_s3_path(tgt_bucket, tmp_path_wo_prefix)
        master_path_wo_prefix = root_path + add_slash(master_base_dir)
        master_path = bucket_key_to_s3_path(tgt_bucket, master_path_wo_prefix)

        icdm_post_fns = param_contents["icdm_post_fns"]

        full_mapping_df = read_mapping(mapping_file, table_name, arn=arn)
        if full_mapping_df.empty:
            print(traceback.format_exc())
            raise Exception(f"mapping not available for cdm_table={table_name}")

        custom_flag = get_custom_flag(full_mapping_df)
        if custom_flag == 'Y':
            source_path = bucket_key_to_s3_path(tgt_bucket, target_path_wo_prefix)

    except Exception as err:
        print(
            f"\t{current_datetime()} :: define_params :: error - failed to read the config file {config_file} in bucket {bucket}")
        print("\terror details : ", err)
        raise err
    else:
        print(
            f"\t{current_datetime()} :: define_params :: info - successfully read the config file {config_file} in bucket {bucket}\n")
        print(f"\t{current_datetime()} :: define_params :: info - source_path                    - {source_path}")
        print(f"\t{current_datetime()} :: define_params :: info - target_path                    - {target_path}")
        print(f"\t{current_datetime()} :: define_params :: info - temp_path                      - {tmp_path}")
        print(f"\t{current_datetime()} :: define_params :: info - master_path                    - {master_path}")
        print("*" * format_length)


def create_union_structure(df_li, path):
    if s3_path_exists(add_slash(path)):
        print(f"\t{current_datetime()} :: create_union_structure :: cleaning up {path}")
        delete_s3_folder_spark(sqlContext, path)
    df_len = len(df_li)
    print(f"\t{current_datetime()} :: create_union_structure :: combining {df_len} dataframes")
    # get unique column list from all transformed dataframes
    unique_col_li = get_all_columns_from_df_list(df_li)
    # create combined structure
    new_df_li = []
    for i, df in enumerate(df_li):
        print(f"\t{current_datetime()} :: create_union_structure :: start :: df - {i + 1}")
        for c in unique_col_li:
            if c not in df.columns:
                df = df.withColumn(c, lit(None).cast(unique_col_li[c]))
            else:
                df = df.withColumn(c, col(c).cast(unique_col_li[c]))
        if i == 0:
            sel_li = df.columns
        df.select(sel_li).write.parquet(path=path, mode="Append", compression="snappy")
        print(f"\t{current_datetime()} :: create_union_structure :: end :: df - {i + 1}")


def dedupe(bucket, config_file, table_name, arn, fn_args):
    full_tmp_path = tmp_path + add_slash(table_name)
    full_src_path = target_path + add_slash(table_name)
    full_tgt_path = target_path + add_slash("picdm_" + table_name)

    print(f"\t{current_datetime()} :: dedupe :: info - final source_path       - {full_src_path}")
    print(f"\t{current_datetime()} :: dedupe :: info - final target_path       - {full_tgt_path}")
    print(f"\t{current_datetime()} :: dedupe :: info - final temp_path         - {full_tmp_path}")

    if s3_path_exists(full_tmp_path):
        print(f"\t{current_datetime()} :: dedupe :: start - clean-up temp path - {full_tmp_path}")
        delete_s3_folder_spark(sqlContext, full_tmp_path)
        print(f"\t{current_datetime()} :: dedupe :: end - clean-up temp path")

    for arg_ind, arg in enumerate(fn_args):
        print(f"**************************** start : iteration {arg_ind} (till temp load) ****************************")

        key = arg["key"]
        table_list = arg["table_list"]
        print(f"\t{current_datetime()} :: dedupe :: start : loading tables from {table_list} into dataframe")
        src_dfs = {}
        for t in table_list:
            src_dfs[t] = read_active_records_from_parquet(sqlContext, target_path + add_slash(t))
            src_dfs[t].createOrReplaceTempView(t)
        print(f"\t{current_datetime()} :: dedupe :: end : loading tables from {table_list} into dataframe")
        sqls = arg["sqls"]
        print(f"\t{current_datetime()} :: dedupe :: build sql")
        fin_sql_li = []
        for t, sql in sqls.items():
            fin_sql_li.append(f"{t} as ({sql})")

        fin_sql = "with " + ', \n'.join(fin_sql_li) + f" \nselect * from {t} "
        print(f"\t{current_datetime()} :: dedupe :: sql to execute is - {fin_sql}")
        df = sqlContext.sql(fin_sql)
        print(f"\t{current_datetime()} :: dedupe :: write {t} into {full_tmp_path}")
        df.write.parquet(path=full_tmp_path, mode="Append", compression="snappy")
        print(f"***************************** end : iteration {arg_ind} (till temp load) *****************************")

    print(f"reading from temp path")
    df = read_parquet(sqlContext, full_tmp_path).repartition(*key).persist()
    df.createOrReplaceTempView("temp")

    cols_to_upd = set(df.columns) - set(key)
    cols_no_upd = set(src_dfs[table_name].columns) - cols_to_upd

    select_list = []
    for c in cols_to_upd:
        select_list.append(f"temp.{c}")
    for c in cols_no_upd:
        select_list.append(f"{table_name}.{c}")
    join_cnd = ' and '.join([f"{table_name}.{item} = temp.{item}" for item in key])
    final_sql = f"select {','.join(select_list)} from {table_name} left join temp on {join_cnd}"
    print(f"final_sql : {final_sql}")
    final_df = sqlContext.sql(final_sql)

    inactive_df = read_parquet(sqlContext, full_src_path)
    inactive_df = inactive_df.filter(inactive_df.is_active == "0")

    if s3_path_exists(full_tgt_path):
        print(f"\t{current_datetime()} :: dedupe :: start : combine active and inactive records into temp path")
        create_union_structure([final_df, inactive_df], remove_slash(full_tgt_path) + '_tmp')
        print(f"\t{current_datetime()} :: dedupe :: end : combine active and inactive records into temp path")

        time.sleep(5)
        print(f"\t{current_datetime()} :: dedupe :: start : move data from temp path to final path")
        move_s3_folder_spark(sqlContext, remove_slash(full_tgt_path) + '_tmp', full_tgt_path)
        print(f"\t{current_datetime()} :: dedupe :: end : move data from temp path to final path")
    else:
        print(f"\t{current_datetime()} :: dedupe :: start : combine active and inactive records into target path")
        create_union_structure([final_df, inactive_df], full_tgt_path)
        print(f"\t{current_datetime()} :: dedupe :: end : combine active and inactive records into target path")
    delete_s3_folder_spark(sqlContext, full_tmp_path)


def snapshot(bucket, config_file, table_name, arn, fn_args):
    snapshot_src_path = target_path + table_name
    snapshot_tgt_path = target_path + table_name + "_snapshot/"
    snapshot_tmp_path = tmp_path + table_name + "_snapshot/"
    if s3_path_exists(snapshot_tgt_path):
        hist_flg = False
    else:
        hist_flg = True

    if s3_path_exists(snapshot_tmp_path):
        print(f"\t{current_datetime()} :: snapshot :: cleanup - {snapshot_tmp_path}")
        delete_s3_folder_spark(sqlContext, snapshot_tmp_path)

    # load source into dataframe
    src_df = read_parquet(sqlContext, snapshot_src_path)
    src_df.createOrReplaceTempView(table_name)

    args = fn_args["load_sqls"]
    output = fn_args["fn_output"]
    print(f"\t{current_datetime()} :: snapshot :: applying historical configs - {args}")
    dfs = {}
    for i, t in enumerate(args):
        sql = args[t]
        if "upd" in t:
            upd_cols_li = sql.split(",")
            for item in upd_cols_li:
                col_nm = item.split("=")[0].strip().lower()
                col_val = item.split("=")[1].strip()
                dfs[list(args)[i - 1]] = dfs[list(args)[i - 1]].withColumn(col_nm, lit(col_val))
        else:
            print(f"\t{current_datetime()} :: snapshot :: sql to run is - {sql}")
            dfs[t] = sqlContext.sql(sql)
            dfs[t].createOrReplaceTempView(t)
            if t in output:
                if hist_flg:
                    print(f"\t{current_datetime()} :: snapshot :: write into {snapshot_tgt_path}")
                    dfs[t].write.parquet(path=snapshot_tgt_path, mode="append", compression="snappy")
                else:
                    print(f"\t{current_datetime()} :: snapshot :: write into {snapshot_tmp_path}")
                    dfs[t].write.parquet(path=snapshot_tmp_path, mode="append", compression="snappy")

    if not hist_flg:
        print(f"\n{current_datetime()} :: snapshot :: move data into target path")
        move_s3_folder_spark_with_partition(sqlContext, snapshot_tmp_path, snapshot_tgt_path, "data_source_id")
    print(f"\n{current_datetime()} :: snapshot :: job execution completed")


if __name__ == "__main__":
    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
    try:
        args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'TABLE_NAME', 'JOB_NAME', 'ARN'])
        bucket = args['S3_BUCKET']
        config_file = args['CONFIG_FILE']
        arn = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None
        table_name = args['TABLE_NAME']
        job_name = args['JOB_NAME']
        job_run_id = args['JOB_RUN_ID']
    except Exception as err:
        print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
        print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
        print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")
        print(f"{current_datetime()} :: main :: info - table_name       : {table_name}")
        print(f"{current_datetime()} :: main :: info - arn              : {arn}")
        print(f"{current_datetime()} :: main :: info - job_name         : {job_name}")
        print(f"{current_datetime()} :: main :: info - job_run_id       : {job_run_id}")
    print("*" * format_length)

    print(f"\n\n****** START - {table_name} ******")
    try:
        print(f"{current_datetime()} :: main :: step 1 - define params")
        define_params(bucket, config_file, table_name, arn)
        print(f"{current_datetime()} :: main :: step 2 - apply post icdm functions")
        # iterate through post icdm functions
        for key, val in icdm_post_fns.items():
            fn_mapping_df = full_mapping_df[full_mapping_df[key] != '']
            if fn_mapping_df.empty:
                print(f"no functions defined under {key} for table {table_name}")
                continue
            if len(fn_mapping_df.index) > 1:
                raise Exception(f"number of post icdm functions should not exceed 1")
            fn = fn_mapping_df[key].unique()[0]
            fn_arg = ast.literal_eval(fn_mapping_df[val].unique()[0])
            fn_call = f"{fn}(bucket, config_file, table_name, arn, {fn_arg})"
            eval(fn_call)
        print("job completed successfully")
    except Exception as e:
        print("ERROR DETAILS - ", e)
        print(traceback.format_exc())
        raise e
    print(f"\n\n****** END - {table_name} ******")
