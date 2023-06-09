import sys
import traceback
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit
from cdm_utilities import *

# initialize script variables
format_length = 150


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def union_all_table_dataframes(dfs):
    # df_cnt = len(dfs)
    for i, df in enumerate(dfs):
        if i == 0:
            union_df = df
        else:
            union_df = union_df.unionByName(df)
    return union_df.groupBy(*union_df.columns).count().drop(col("count"))


def define_params(bucket, config_file, table_name):
    # parse the config file contents
    print(f"\n\t{current_datetime()} :: define_params :: "
          f"info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:

        global source_path, target_path, tmp_path, mapping_file, audit_columns, sql_script_path, cai_map_df, \
            cai_tab_nm, cai_tab_path, cai_tab_tmp, all_tab_nm, all_tab_path, cai_col_nm, cai_index_id, all_map_df, input_dataset, param_contents, json_script_path

        param_data = get_s3_object(bucket, config_file)
        param_contents = json.loads(param_data)

        mapping_file = param_contents["ing_mapping"]
        input_dataset = param_contents["input_dataset"]
        json_script_path = param_contents['json_script_path']

        root_path = param_contents["root_path"]
        src_base_dir = param_contents["ing_base_dir"]
        tgt_base_dir = param_contents["cai_base_dir"]
        tmp_base_dir = tgt_base_dir + "_temp"

        source_path_wo_prefix = root_path + add_slash(src_base_dir)
        source_path = bucket_key_to_s3_path(bucket, source_path_wo_prefix)
        target_path_wo_prefix = root_path + add_slash(tgt_base_dir)
        target_path = bucket_key_to_s3_path(bucket, target_path_wo_prefix)
        tmp_path_wo_prefix = root_path + add_slash(tmp_base_dir)
        tmp_path = bucket_key_to_s3_path(bucket, tmp_path_wo_prefix)
        sql_script_path = param_contents['sql_script_path']

        cai_aud_cols = ["insert_ts", "update_ts"]
        audit_columns = param_contents['audit_columns'] + cai_aud_cols

        cai_tab_nm = "cai_" + table_name + "_id_map"
        cai_tab_path = target_path + add_slash(cai_tab_nm)
        cai_tab_tmp = target_path + add_slash("tmp_" + cai_tab_nm)
        cai_col_nm = "cai_" + table_name + "_id"
        all_tab_nm = "all_" + table_name
        all_tab_path = target_path + add_slash(all_tab_nm)
        cai_index_id = "cai_" + table_name + "_index_id"

        # read mapping for all_table
        all_map_df = read_mapping(mapping_file, all_tab_nm, "integrated")
        if all_map_df.empty:
            raise Exception(f"No mapping available for cdm_table = {all_tab_nm} and cdm_name = integrated")

        cai_map_df = read_mapping(mapping_file, cai_tab_nm, "integrated")
        if cai_map_df.empty:
            raise Exception(f"No mapping available for cdm_table = {cai_tab_nm} and cdm_name = integrated")

    except Exception as err:
        print(f"\t{current_datetime()} :: define_params :: error - "
              f"failed to read the config file {config_file} in bucket {bucket}")
        print("\terror details : ", err)
        raise err
    else:
        print(f"\t{current_datetime()} :: define_params :: info - "
              f"successfully read the config file {config_file} in bucket {bucket}\n")
        print(f"\t{current_datetime()} :: define_params :: info - source_path          : {source_path}")
        print(f"\t{current_datetime()} :: define_params :: info - target_path          : {target_path}")
        print(f"\t{current_datetime()} :: define_params :: info - temp path            : {tmp_path}")
        print(f"\t{current_datetime()} :: define_params :: info - sql_script_path      : {sql_script_path}")
        print("*" * format_length)


def get_cai_tx_args():
    # get cai transformation arguments
    cai_tx_map = cai_map_df[cai_map_df.cai_tx.str.strip() != '']
    cai_arg_li = pd.unique(cai_tx_map.cai_arg).tolist()
    if len(cai_arg_li) > 1:
        raise Exception(f"get_cai_tx_args - incorrect cai arguments defined")
    else:
        if cai_arg_li[0].split(".")[-1] == 'json':
            json_script_path_full = bucket_key_to_s3_path(bucket, add_slash(json_script_path)) + \
                                    cai_arg_li[0]
            print(f"Full_path :: {json_script_path_full}")
            if s3_path_exists(json_script_path_full):
                print(f":: reading from {json_script_path_full}")
                cai_tx_json = get_s3_object(bucket, add_slash(json_script_path) + cai_arg_li[0])
                return cai_tx_json.decode('utf-8')
            else:
                raise Exception(f"get_cai_tx_args - {json_script_path_full} does not exists")
        else:
            return cai_arg_li[0] if cai_arg_li else None


def load_all_tables():
    # fetch rule order and rule sets
    ro_rs_li = get_ruleorder_ruleset(all_map_df)

    # iterate through each rule order and rule set to populate combined table
    all_dfs = []
    for ro, rs_li in ro_rs_li.items():
        print(f"\n\t{current_datetime()} :: load_all_tables :: start :: rule order {ro}")
        for rs in rs_li:
            print(f"\n\t{current_datetime()} :: load_all_tables :: start :: rule set {rs}")
            # get current mapping
            curr_mapping = all_map_df[(all_map_df.rule_order == ro) & (all_map_df.rule_set == rs)]

            # get source name
            src_li = get_source_list(curr_mapping)
            if len(src_li) > 1:
                raise Exception(f"rule order = {ro}, rule set = {rs} has more than one source")
            if len(src_li) < 1:
                raise Exception(f"atleast one source name should be defined for rule order = {ro}, rule set = {rs}")
            src_name = src_li[0]

            # get source table
            src_tbl_li = get_source_table_list(curr_mapping)
            exec_sql = True
            for src_tbl_name in src_tbl_li:
                # read source data into dataframe
                src_full_path = source_path + add_slash(src_name) + add_slash(src_tbl_name)
                if not s3_path_exists(src_full_path):
                    bucket, key = s3_path_to_bucket_key(src_full_path)
                    isFileMarkedRequired = check_src_file_validity(bucket, param_contents, "CAI_CREATION",
                                                                   src_tbl_name.lower(), src_name)
                    if isFileMarkedRequired:
                        raise Exception(
                            f"Table {src_tbl_name} and source {src_name} marked required in input dataset but file not present in the s3 path {key}")
                    else:
                        print(
                            f"load_all_tables -  {src_tbl_name} marked unavailable in input dataset, skipping the current iteration...")
                        exec_sql = False
                        continue
                else:
                    # src_df = read_active_records_from_parquet(sqlContext, src_full_path)
                    src_df = read_parquet(sqlContext, src_full_path)
                    src_df.createOrReplaceTempView(src_tbl_name)

            if exec_sql:
                # get sql to read from source
                all_tab_sql = build_src_sql(curr_mapping)
                print(
                    f"\n\t{current_datetime()} :: load_all_tables :: sql for rule order = {ro} and rule set = {rs} is \n\n\t{all_tab_sql}\n\n")

                # run sql and capture result in dataframe list
                all_dfs.append(sqlContext.sql(all_tab_sql))

            print(f"\n\t{current_datetime()} :: load_all_tables :: end :: rule set {rs}")
        print(f"\n\t{current_datetime()} :: load_all_tables :: end :: rule order {ro}")

    # union all dataframes and load into target location
    if not all_dfs:
        print(f"\n\t{current_datetime()} :: load_all_tables :: all_dfs - is empty list")
    else:
        fin_df = union_dataframes(all_dfs)
        fin_df = union_all_table_dataframes(all_dfs)
        print(f"\n\t{current_datetime()} :: load_all_tables :: union dataframes completed")
        # append audit columns
        fin_df = fin_df.withColumn("insert_ts", lit(current_datetime()))
        # save dataframe as parquet
        write_parquet(fin_df, all_tab_path)
        time.sleep(1)
        print(f"\n\t{current_datetime()} :: load_all_tables :: {all_tab_nm} loaded successfully")

    return all_dfs


def insert_cai_id_map():
    # read mapping
    print(f"\n\t{current_datetime()} :: insert_cai_id_map :: reading mapping for {cai_tab_nm}")
    cai_tab_map = read_mapping(mapping_file, cai_tab_nm, "integrated")
    if cai_tab_map.empty:
        raise Exception(f"No mapping available for cdm_table = {cai_tab_nm} and cdm_name = integrated")
    # get sql file name from mapping and fetch sql from scripts path
    print(f"\n\t{current_datetime()} :: insert_cai_id_map :: fetching sql to load {cai_tab_nm}")
    sql_file_nm = get_sql_file_name(cai_tab_map)
    if sql_file_nm is None:
        raise Exception(f"sql file name not defined for cdm_table = {cai_tab_nm} and cdm_name = integrated")
    sql_script_file = add_slash(sql_script_path) + sql_file_nm
    cai_ins_sql = get_s3_object(bucket, sql_script_file).decode('utf-8')
    print(
        f"\n\t{current_datetime()} :: insert_cai_id_map :: sql to insert into {cai_tab_nm} is \n\n\t{cai_ins_sql}\n\n")
    # read existing cai table into dataframe; if not exist, create empty dataframe
    print(f"\n\t{current_datetime()} :: insert_cai_id_map :: loading {cai_tab_nm} table into dataframe")
    if s3_path_exists(cai_tab_path):
        tgt_df = read_parquet(sqlContext, cai_tab_path)
    else:
        # get column name and datatype
        tgt_cols = dict(zip(cai_tab_map.cdm_column, cai_tab_map.datatype))
        # derive target schema
        f = []
        for c, d in tgt_cols.items():
            new_d = get_spark_type(d)
            f.append(StructField(c, new_d, True))
        tgt_schema = StructType(list(f))
        # create empty dataframe with target schema
        tgt_df = sqlContext.createDataFrame(sc.emptyRDD(), tgt_schema)
    tgt_df.createOrReplaceTempView(cai_tab_nm)
    tgt_cnt = tgt_df.count()
    print(f"\n\t{current_datetime()} :: insert_cai_id_map :: no of records in {cai_tab_nm} : before load : {tgt_cnt}")

    # read all_<table> into dataframe
    print(f"\n\t{current_datetime()} :: insert_cai_id_map :: loading {all_tab_nm} table into dataframe")
    if not s3_path_exists(all_tab_path):
        raise Exception(f"{all_tab_path} - does not exist")
    src_df = read_parquet(sqlContext, all_tab_path)
    src_df.createOrReplaceTempView(all_tab_nm)

    # execute cai - insert sql and load data into target
    print(f"\n\t{current_datetime()} :: insert_cai_id_map :: execute cai insert sql")
    cai_ins_df = sqlContext.sql(cai_ins_sql)
    cai_ins_cnt = cai_ins_df.count()
    print(f"\n\t{current_datetime()} :: insert_cai_id_map :: no of new records : {cai_ins_cnt}")

    # append audit_columns
    cai_ins_df = cai_ins_df \
        .withColumn("insert_ts", lit(current_datetime())) \
        .withColumn("update_ts", lit(current_datetime()))

    # save cai ids into target
    if cai_ins_cnt == 0:
        print(f"\n\t{current_datetime()} :: insert_cai_id_map :: nothing to insert")
    elif tgt_cnt == 0:
        print(f"\n\t{current_datetime()} :: insert_cai_id_map :: insert into {cai_tab_nm}")
        write_parquet(cai_ins_df, cai_tab_path)
    else:
        print(f"\n\t{current_datetime()} :: insert_cai_id_map :: append to {cai_tab_nm}")
        write_parquet(union_dataframes([tgt_df, cai_ins_df]), cai_tab_tmp)
        print(f"\n\t{current_datetime()} :: insert_cai_id_map :: start - move data from temp to target path")
        time.sleep(1)
        # move_s3_folder_spark(sqlContext, cai_tab_tmp, cai_tab_path)
        move_s3_folder_spark_with_partition(sqlContext, cai_tab_tmp, cai_tab_path, part_keys)
        time.sleep(1)
        print(f"\n\t{current_datetime()} :: insert_cai_id_map :: end - move data from temp to target path")
    time.sleep(1)
    print(f"\n\t{current_datetime()} :: insert_cai_id_map :: {cai_tab_nm} loaded successfully")


def update_cai_id_map(output_li):
    # get cai arguments
    cai_args = get_cai_tx_args()
    if cai_args is None:
        print(f"update_cai_id_map - cai arguments not set for {table_name}")
        return

    cai_args = ast.literal_eval(cai_args)
    print(json.dumps(cai_args, indent=4))

    err_li = [id_typ for id_typ in output_li if id_typ.strip().lower() not in cai_args]
    if err_li:
        raise Exception(f"update_cai_id_map - cai id update arguments not defined for following list of ids {err_li}")

    for op in output_li:
        print(f"\n\t{current_datetime()} :: update_cai_id_map :: start - {op}")

        # read cai id map table
        cai_id_map_df = read_parquet(sqlContext, cai_tab_path)
        cai_id_map_df.createOrReplaceTempView(cai_tab_nm)
        before_count = cai_id_map_df.count()
        print(f"\n\t{current_datetime()} :: update_cai_id_map :: count (before update) - {before_count}")

        curr_cai_args = cai_args[op]
        dfs = {}
        vw_li = []
        cols_to_upd = []
        for k, v in curr_cai_args.items():
            if k == "table_list":
                tables_di = curr_cai_args["table_list"]
                for src, src_tbl_li in tables_di.items():
                    for src_tbl_w_alias in src_tbl_li:
                        src_tbl = src_tbl_w_alias.split(":")[0]
                        src_tbl_alias = src_tbl_w_alias.split(":")[-1]
                        src_tbl_path = source_path + add_slash(src.lower().strip()) + add_slash(src_tbl.lower().strip())
                        # print(src_tbl_path)
                        dfs[src_tbl_alias] = read_active_records_from_parquet(sqlContext, src_tbl_path)
                        if dfs[src_tbl_alias] is None:
                            raise Exception(f"update_cai_id_map - {src_tbl_path} does not exist")
                        print(
                            f"{current_datetime()} :: update_cai_id_map - creating view {src_tbl_alias} for {src}/{src_tbl}")
                        dfs[src_tbl_alias].createOrReplaceTempView(src_tbl_alias)
                continue

            if k == "key":
                pks = [c.strip().lower() for c in curr_cai_args["key"].split(",")]
                # print(pks)
                continue

            run_sql = v["sql"]
            print(f"\t{current_datetime()} :: update_cai_id_map :: {op} - {k} ==> {run_sql}")
            vw_li.append(k)
            dfs[k] = sqlContext.sql(run_sql)
            # added for debug - 2022-09-23
            # write_parquet(dfs[k], remove_slash(cai_tab_path) + add_slash(f"_debug_{op}_{k}/"))

            dfs[k].createOrReplaceTempView(k)
            cols_to_upd += [c for c in dfs[k].columns if c not in cols_to_upd]

        from_clause = cai_tab_nm
        select_list = [f"{cai_tab_nm}.{c}" for c in cai_id_map_df.columns if c in pks or c not in cols_to_upd]
        for vw in vw_li:
            join_cond = ' and '.join([f"{cai_tab_nm}.{c} = {vw}.{c}" for c in pks])
            from_clause += f" left join {vw} on {join_cond}"

        for c in (set(cols_to_upd) - set(pks)):
            nvl_col = []
            for v in vw_li:
                nvl_col.append(f"{v}.{c}")
            select_list.append(f"coalesce(" + ','.join(nvl_col) + f",{cai_tab_nm}.{c}) as {c}")

        fin_sql = f"select " + ', '.join(select_list) + f" from {from_clause} "
        print(f"\t{current_datetime()} :: update_cai_id_map :: {op} - fin_sql ==> {fin_sql}")

        # out_df = sqlContext.sql(fin_sql).distinct()
        out_df = sqlContext.sql(fin_sql)
        out_df = out_df.groupBy(*out_df.columns).count().drop(col("count"))
        out_df = out_df.withColumn("update_ts", lit(current_datetime()))
        after_count = out_df.count()
        print(f"\n\t{current_datetime()} :: update_cai_id_map :: count (after update) - {after_count}")
        if before_count != after_count:
            write_parquet(out_df, remove_slash(cai_tab_path) + add_slash("_error/"))
            raise Exception(f"{op} returns duplicates......Stopping before writing !!")
        write_parquet(out_df, cai_tab_tmp)
        print(f"\n\t{current_datetime()} :: update_cai_id_map :: start - move data from temp to target path")
        time.sleep(1)
        # move_s3_folder_spark(sqlContext, cai_tab_tmp, cai_tab_path)
        move_s3_folder_spark_with_partition(sqlContext, cai_tab_tmp, cai_tab_path, part_keys)
        time.sleep(1)
        print(f"\n\t{current_datetime()} :: update_cai_id_map :: debug - load into {remove_slash(cai_tab_path)}_{op}")
        copy_s3_folder_spark(sqlContext, cai_tab_path, remove_slash(cai_tab_path) + "_" + op)
        print(f"\n\t{current_datetime()} :: update_cai_id_map :: end - move data from temp to target path")
        print(f"\n\t{current_datetime()} :: update_cai_id_map :: end - {op}")

    print(f"\n\t{current_datetime()} :: update_cai_id_map :: completed successfully")


# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
args = getResolvedOptions(sys.argv,
                          ['S3_BUCKET', 'CONFIG_FILE', 'TABLE_NAME', 'ID_LIST', 'PREPROCESSING_FUNCTION',
                           'SPARK_PROPERTIES', 'JOB_NAME'])
try:
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    table_name = args['TABLE_NAME'].lower()
    ids_to_process = [i.strip() for i in args['ID_LIST'].split(",")] if args['ID_LIST'].strip().lower() != 'na' else []
    pre_processing_fn = args['PREPROCESSING_FUNCTION'].strip().lower()
    pre_processing_fn = '' if pre_processing_fn in ['""', "na"] else pre_processing_fn
    pre_processing_fn = [p.strip() for p in pre_processing_fn.split(",")]
    spark_properties = args['SPARK_PROPERTIES']
    part_keys = "id_type"
    job_name = args['JOB_NAME']
    job_run_id = args['JOB_RUN_ID']
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket            : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file       : {config_file}")
    print(f"{current_datetime()} :: main :: info - table_name        : {table_name}")
    print(f"{current_datetime()} :: main :: info - ids_to_process    : {ids_to_process}")
    print(f"{current_datetime()} :: main :: info - pre_processing_fn : {pre_processing_fn}")
    print(f"{current_datetime()} :: main :: info - part_keys         : {part_keys}")
    print(f"{current_datetime()} :: main :: info - job_name          : {job_name}")
    print(f"{current_datetime()} :: main :: info - job_name          : {job_name}")
    print(f"{current_datetime()} :: main :: info - job_run_id        : {job_run_id}")
print("*" * format_length)

print(f"\n\n****** START - {table_name} ******")

sc = SparkContext(conf=SparkConf()
                  .set('spark.sql.parquet.fs.optimized.committer.optimization-enabled', True)
                  # .set('spark.sql.autoBroadcastJoinThreshold', 2097152000)
                  # .set('spark.sql.broadcastTimeout', 600)
                  # .set('spark.sql.shuffle.partitions', 240)
                  ).getOrCreate()
sqlContext = SQLContext(sc)

di = ast.literal_eval(spark_properties)
print(di)

for k, v in di.items():
    print(f"{k} = {v}")
    sqlContext.sql(f'set {k}={v}')

# Setting S3 max connection to 100
hc = sc._jsc.hadoopConfiguration()
hc.setInt("fs.s3.connection.maximum", 1000)
hc.set("fs.s3.multiobjectdelete.enable", "true")

try:
    if not pre_processing_fn:
        raise Exception(f"no functions defined for processing")

    missing_functions = list(set(pre_processing_fn) - {'insert_cai_id_map', 'update_cai_id_map'})
    if len(missing_functions) > 0:
        raise Exception(f"functions {missing_functions} not part of cai load")

    print(f"{current_datetime()} :: main :: step 1 - define params")
    define_params(bucket, config_file, table_name)

    if "insert_cai_id_map" in pre_processing_fn:
        print(f"{current_datetime()} :: main :: step 2 - load all table")
        dfs_li = load_all_tables()

        if dfs_li:
            print(f"{current_datetime()} :: main :: step 3 - insert into cai id map table")
            insert_cai_id_map()
        else:
            raise Exception(f"failed to load all_{table_name}")

    if "update_cai_id_map" in pre_processing_fn:
        # if ids_to_process and dfs_li:
        if ids_to_process:
            print(f"{current_datetime()} :: main :: step 4 - update cai id map table")
            update_cai_id_map(ids_to_process)

except Exception as e:
    print("ERROR DETAILS - ", e)
    print(traceback.format_exc())
    raise e
print(f"\n\n****** END - {table_name} ******")
