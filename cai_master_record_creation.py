import sys
import sys
import time
import traceback

from awsglue.utils import getResolvedOptions
from pandasql import sqldf
from pyspark.sql.functions import split, concat_ws, md5, coalesce

from incremental_load_functions import *

# initialize script variables
format_length = 150


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_cai_load_args(mapping_df):
    cai_arg_li = [s for s in pd.unique(mapping_df.cai_load_arg) if s.strip() != ""]
    return ast.literal_eval(cai_arg_li[0]) if cai_arg_li else None


def get_cross_upd_args(mapping_df):
    cross_upd_args_li = [s for s in pd.unique(mapping_df.cai_arg) if s.strip() != ""]
    return ast.literal_eval(cross_upd_args_li[0]) if cross_upd_args_li else None


# initialize spark context and sqlcontext
def initialize_spark_context():
    # if spark_properties != '':
    #     props = json.loads(spark_properties)
    #     shuffle_part = int(props["shuffle_partition"]) if props["shuffle_partition"] != '' else 300
    #     broadcast_threshold = int(props["broadcast_threshold"]) if props["broadcast_threshold"] != '' else 2097152000
    # else:
    #     broadcast_threshold = 2097152000
    #     shuffle_part = 300
    # print(f"broadcast_threshold_value :: {broadcast_threshold}")
    # print(f"shuffle_partition_value :: {shuffle_part}")
    sc = SparkContext(conf=SparkConf()
                      .set("spark.driver.maxResultSize", "0")
                      .set("spark.sql.parquet.mergeSchema", True)
                      # .set('spark.sql.autoBroadcastJoinThreshold', broadcast_threshold)
                      .set('spark.sql.parquet.fs.optimized.committer.optimization-enabled', True)
                      # .set('spark.sql.shuffle.partitions', shuffle_part)
                      ).getOrCreate()
    sqlContext = SQLContext(sc)
    sc.setLogLevel("Error")

    di = ast.literal_eval(spark_properties)
    print(di)

    for k, v in di.items():
        print(f"{k} = {v}")
        sqlContext.sql(f'set {k}={v}')

    # spark.sql.crossJoin.enabled=true

    # Setting S3 max connection to 100
    hc = sc._jsc.hadoopConfiguration()
    hc.setInt("fs.s3.connection.maximum", 100)
    hc.setInt("fs.s3.maxRetries", 20)
    hc.set("fs.s3.multiobjectdelete.enable", "false")

    return sc, sqlContext


def define_params():
    # parse the config file contents
    print(f"{current_datetime()} :: define_params :: info - reading the config file {config_file} in bucket {bucket}")
    try:
        global batch_date, audit_cols, source_path, target_path, tmp_path, master_path, full_mapping_df, \
            hash_reqd, load_fn_name, load_fn_args, cai_load_args, audit_cols, param_contents, json_script_path

        param_data = get_s3_object(bucket, config_file)
        param_contents = json.loads(param_data)

        mapping_file = param_contents["ing_mapping"]
        batch_date = param_contents["batch_date"]
        audit_cols = param_contents["audit_columns"]
        json_script_path = param_contents['json_script_path']

        root_path = param_contents["root_path"]
        src_base_dir = param_contents["ing_w_cai_base_dir"]
        tgt_base_dir = param_contents["ing_w_cai_base_dir"]
        master_base_dir = param_contents["master_base_dir"]
        tmp_base_dir = tgt_base_dir + "_temp"

        source_path_wo_prefix = root_path + add_slash(src_base_dir)
        source_path = bucket_key_to_s3_path(bucket, source_path_wo_prefix)
        target_path_wo_prefix = root_path + add_slash(tgt_base_dir) + add_slash(tgt_name) + add_slash(table_name)
        target_path = bucket_key_to_s3_path(bucket, target_path_wo_prefix)
        tmp_path_wo_prefix = root_path + add_slash(tmp_base_dir) + add_slash(tgt_name) + add_slash(table_name)
        tmp_path = bucket_key_to_s3_path(bucket, tmp_path_wo_prefix)
        master_path_wo_prefix = root_path + add_slash(master_base_dir)
        master_path = bucket_key_to_s3_path(bucket, master_path_wo_prefix)

        print(f"{current_datetime()} :: define_params :: reading mapping from {mapping_file}")
        full_mapping_df = read_mapping(mapping_file, table_name, tgt_name)
        if full_mapping_df.empty:
            raise Exception(f"No mapping available for cdm_name - {tgt_name}, cdm_table - {table_name}")

        cai_load_args = get_cai_load_args(full_mapping_df)
        if cai_load_args is None:
            load_args = None
            load_fn_name = None
            load_fn_args = None
            hash_reqd = None
        else:
            load_args = cai_load_args["load_fn"]
            load_fn_name = list(load_args)[0]
            load_fn_args = load_args[load_fn_name]
            hash_reqd = load_fn_args["hash_key_reqd"]

    except Exception as err:
        print(f"{current_datetime()} :: define_params :: error - failed to read the config file {config_file} in "
              f"bucket {bucket}")
        print("error details : ", err)
        raise err
    else:
        print(
            f"{current_datetime()} :: define_params :: info - "
            f"successfully read the config file {config_file} in bucket {bucket}")
        print(f"{current_datetime()} :: define_params :: info - source_path                    - {source_path}")
        print(f"{current_datetime()} :: define_params :: info - target_path                    - {target_path}")
        print(f"{current_datetime()} :: define_params :: info - temp path                      - {tmp_path}")
        print(f"{current_datetime()} :: define_params :: info - master_path                    - {master_path}")
        print(f"{current_datetime()} :: define_params :: info - cai load arguments             - \n"
              f"{json.dumps(cai_load_args, indent=4)}")
        print(f"{current_datetime()} :: define_params :: info - cai load function name         - {load_fn_name}")
        print(f"{current_datetime()} :: define_params :: info - cai load function args         - {load_fn_args}")
        print(f"{current_datetime()} :: define_params :: info - Hash key generation required ? - {hash_reqd}")
        print("*" * format_length)


def read_src_data(map_df):
    dfs = {}
    src_nm_tbl_dict = get_grouped_tables_by_sources(map_df)
    print(f"{current_datetime()}:: list of tables to read :: {src_nm_tbl_dict}")
    for src, tbl_li in src_nm_tbl_dict.items():
        for tbl in tbl_li:
            src_tbl_path = source_path + add_slash(src) + add_slash(tbl)
            if s3_path_exists(src_tbl_path):
                dfs[f"{src}_{tbl}"] = read_active_records_from_parquet(sqlContext, src_tbl_path)
            elif s3_path_exists(master_path + add_slash(tbl)):
                dfs[f"{src}_{tbl}"] = read_parquet(sqlContext, master_path + add_slash(tbl))
            else:
                is_file_reqd = check_src_file_validity(bucket, param_contents, "ICDM", tbl.lower(), src)
                if is_file_reqd:
                    raise Exception(f"Table {tbl} and source {src} marked required in input "
                                    f"dataset but file not present in the s3 path {src_tbl_path}")
                else:
                    print(f"load_all_tables -  {tbl} marked unavailable in input dataset, "
                          f"skipping the current iteration...")
                    continue
    return dfs


def build_integrated_sql(map_df):
    query_select = """ 
                   select \
                       case 
                       when coalesce(trim(sql_tx),'')||coalesce(trim(src_column),'')='' then 'null'  \
                       when trim(sql_tx)='' then (case when trim(src_alias)='' then trim(src_table) else trim(src_alias) end) ||'.'||trim(src_column)  \
                       else trim(sql_tx) end || \
                       case when lower(trim(cdm_column)) = '*' then '' else ' as ' || lower(trim(cdm_column)) end as source  \
                       from map_df  \
                       where trim(cdm_column) is not null and trim(cdm_column) <> ''
                   """

    query_from = "select distinct  trim(src_name)||'_'||trim(src_table) || ' as ' || (case when trim(src_alias)='' then trim(src_table) else trim(src_alias) end) as source " \
                 "from map_df " \
                 "where trim(src_table)<>'' and trim(src_table) is not null " \
                 "and trim(src_table) " \
                 "not in (select trim(src_table) from map_df where lower(map_type) like '%join%')"

    query_join = "select ' ' || map_type || ' ' || trim(src_name)||'_'||trim(src_table) || ' as ' || (case when trim(src_alias)='' then trim(src_table) else trim(src_alias) end) || ' ON ' || trim(sql_tx) as source " \
                 "from map_df where lower(map_type) like '%join%'"

    query_where = "select trim(sql_tx) as source from map_df where lower(trim(map_type))='where' "
    audit_tbl = "Select distinct (case when trim(src_alias)='' then trim(src_table) else trim(src_alias) end) as source " \
                "from map_df " \
                "where trim(src_table)<>'' and trim(src_table) is not null " \
                "and trim(src_table) " \
                "not in (select trim(src_table) from map_df where lower(map_type) like '%join%')"

    audit_tbl_ref = " ".join(sqldf(audit_tbl)["source"].tolist())
    from_list = sqldf(query_from)["source"].tolist()
    from_clause = " ".join(from_list)
    join_list = sqldf(query_join)["source"].tolist()
    join_clause = " ".join(join_list)
    where_list = sqldf(query_where)["source"].tolist()
    where_clause = '' if where_list == [] else " WHERE " + where_list[0]
    select_list = sqldf(query_select)["source"].tolist()
    # add audit columns in query
    select_list = select_list + [f"{audit_tbl_ref}.{c}" for c in audit_cols if c.strip().lower() != 'hashkey']
    select_clause = " , ".join(select_list)
    src_query = "SELECT " + select_clause + " FROM " + from_clause + join_clause + where_clause
    return src_query


def create_union_structure(df_li):
    if s3_path_exists(remove_slash(tmp_path) + "_tmp/"):
        print(f"{current_datetime()} :: create_union_structure :: cleaning up {remove_slash(tmp_path) + '_tmp/'}")
        delete_s3_folder_spark(sqlContext, remove_slash(tmp_path) + "_tmp/")

    if s3_path_exists(remove_slash(tmp_path) + "_tmp2/"):
        print(f"{current_datetime()} :: create_union_structure :: cleaning up {remove_slash(tmp_path) + '_tmp2/'}")
        delete_s3_folder_spark(sqlContext, remove_slash(tmp_path) + "_tmp2/")

    df_len = len(df_li)
    print(f"{current_datetime()} :: create_union_structure :: combining {df_len} dataframes")
    # get unique column list from all transformed dataframes
    unique_col_li = get_all_columns_from_df_list(df_li)
    # create combined structure
    for i, df in enumerate(df_li):
        print(f"{current_datetime()} :: create_union_structure :: start :: df - {i + 1}")
        for c in unique_col_li:
            if c not in df.columns:
                df = df.withColumn(c, lit(None).cast(unique_col_li[c]))
            else:
                df = df.withColumn(c, col(c).cast(unique_col_li[c]))
        if i == 0:
            sel_li = df.columns
        print(f"{current_datetime()} :: create_union_structure :: {i} - count - {df.count()}")
        df.select(sel_li).write.parquet(path=remove_slash(tmp_path) + "_tmp2/", mode="Append", compression="snappy")
        print(f"{current_datetime()} :: create_union_structure :: end :: df - {i + 1}")

    _df = read_parquet(sqlContext, remove_slash(tmp_path) + "_tmp2/")
    distinct_row_mapping = full_mapping_df[full_mapping_df.tgt_unique_row_arg.str.strip() != '']
    if not distinct_row_mapping.empty:
        json_script_path_full = bucket_key_to_s3_path(bucket, json_script_path) + \
                                distinct_row_mapping['tgt_unique_row_arg'].iloc[0]
        if s3_path_exists(json_script_path_full):
            print(f"{current_datetime()} :: combine_base_data :: reading from {json_script_path_full}")
            drop_dup_query_dict = json.loads(get_s3_object(bucket, add_slash(json_script_path) +
                                                           distinct_row_mapping['tgt_unique_row_arg'].iloc[0]))
        else:
            drop_dup_query_dict = json.loads(distinct_row_mapping['tgt_unique_row_arg'].iloc[0])
        print(f"\t{current_datetime()} :: create_union_structure :: Found duplicate delete script. Starting...")
        print(drop_dup_query_dict)
        _df = get_distinct_rows(sqlContext, _df, drop_dup_query_dict, table_name)
        print(f"\t{current_datetime()} :: create_union_structure :: Duplicate delete script ended.")

    # audit columns read while building query
    # _df = _df \
    #     .withColumn("add_date", to_date(lit(batch_date), "yyyyMMdd")) \
    #     .withColumn("delete_date", to_date(lit("99991231"), "yyyyMMdd")) \
    #     .withColumn("is_active", lit(1)) \
    #     .withColumn("is_incremental", lit(1)) \
    #     .withColumn("last_modified_date", to_timestamp(lit(current_datetime()), "yyyy-MM-dd HH:mm:ss"))

    write_parquet(_df, remove_slash(tmp_path) + "_tmp/")

    _1, _2 = s3_path_to_bucket_key(remove_slash(tmp_path) + "_tmp2/")
    # delete_s3_folder(_1, _2)

    return remove_slash(tmp_path) + "_tmp/"


def combine_base_data():
    # get rules order and rule set list
    print(f"{current_datetime()} :: combine_base_data :: get rules list")
    ro_rs_li = get_ruleorder_ruleset(full_mapping_df)
    # iterate through each rule in rules order and rule set list
    trf_df_li = []
    for ro, rs_li in ro_rs_li.items():
        print(f"{current_datetime()} :: combine_base_data :: start :: rule order {ro}")
        for rs in rs_li:
            print(f"{current_datetime()} :: combine_base_data :: start :: rule set {rs}")
            # get current mapping
            curr_mapping = full_mapping_df[(full_mapping_df.rule_order == ro) & (full_mapping_df.rule_set == rs)]
            src_dfs = read_src_data(curr_mapping)
            if len(src_dfs) != 0:
                for src_tbl, src_df in src_dfs.items():
                    src_df.createOrReplaceTempView(src_tbl)

                trf_sql = build_integrated_sql(curr_mapping)
                print(trf_sql)
                trf_df = sqlContext.sql(trf_sql)
                distinct_row_mapping = curr_mapping[curr_mapping.src_unique_row_arg.str.strip() != '']
                if not distinct_row_mapping.empty:
                    print(f"{current_datetime()} :: combine_base_data :: Found duplicate delete script. Starting..")
                    json_script_path_full = bucket_key_to_s3_path(bucket, json_script_path) + \
                                            distinct_row_mapping['src_unique_row_arg'].iloc[0]
                    if s3_path_exists(json_script_path_full):
                        print(f"{current_datetime()} :: combine_base_data :: reading from {json_script_path_full}")
                        drop_dup_query_dict = json.loads(get_s3_object(bucket, add_slash(json_script_path) +
                                                                       distinct_row_mapping['src_unique_row_arg'].iloc[
                                                                           0]))
                    else:
                        drop_dup_query_dict = json.loads(distinct_row_mapping['src_unique_row_arg'].iloc[0])
                    trf_df = get_distinct_rows(sqlContext, trf_df, drop_dup_query_dict, table_name)
                    print(f"{current_datetime()} :: combine_base_data :: Duplicate delete script ended.")
                trf_df = trf_df.distinct()
                trf_df_li.append(trf_df)
            print(f"{current_datetime()} :: combine_base_data :: end :: rule set {rs}")
        print(f"{current_datetime()} :: combine_base_data :: end :: rule order {ro}")
    print(f"{current_datetime()} :: combine_base_data :: load unioned data in temp path")
    combined_based_data_path = create_union_structure(trf_df_li)
    return combined_based_data_path


def create_master_data(combined_base_data_path, cai_key):
    tmp_mas_data_path = remove_slash(tmp_path) + "_tmp_mas/"
    if s3_path_exists(tmp_mas_data_path):
        _1, _2 = s3_path_to_bucket_key(tmp_mas_data_path)
        delete_s3_folder(_1, _2)

    master_data_args = get_cross_upd_args(full_mapping_df)[cai_key]
    master_data_args = {cai_key: master_data_args}
    ds_di_to_process = {}
    for key1, val1 in master_data_args.items():
        ds_di_to_process[key1] = []
        for key2, val2 in val1.items():
            if isinstance(val2, dict) and "data_source_id" in val2:
                ds_di_to_process[key1] += val2['data_source_id'].replace(' ', '').split(',')
    print(f"{current_datetime()} :: data source id list to create master record - {ds_di_to_process}")
    print(f"{current_datetime()} :: get primary keys for the table")
    pk_li = read_primary_keys(full_mapping_df)
    pk_wo_ds = [p for p in pk_li if p != 'data_source_id']
    join_cond = ' and '.join([f"a.{k} = b.{k}" for k in pk_wo_ds])
    print(f"{current_datetime()} ::start -  creating master data")
    for master_ds_id, child_ds_id_li in ds_di_to_process.items():
        for child_ds_id in child_ds_id_li:
            print(f"{current_datetime()} :: start - iteration - {master_ds_id}/{child_ds_id}")
            time.sleep(5)
            df = read_parquet(sqlContext, [combined_base_data_path, tmp_mas_data_path])
            sel_order = df.columns
            df.createOrReplaceTempView("combined_base_data")
            sql = f"select * from combined_base_data a where lower(data_source_id)='{child_ds_id.lower()}' " \
                  f"and not exists (select 1 from combined_base_data b " \
                  f"where lower(b.data_source_id)='{master_ds_id.lower()}' " \
                  f"and {join_cond})"
            print(sql)
            out_df = sqlContext.sql(sql)
            out_df = out_df.withColumn("data_source_id", lit(master_ds_id))
            print(f"out_df - count - {out_df.count()}")
            out_df.select(*sel_order).write.parquet(tmp_mas_data_path, "append")
            print(f"{current_datetime()} :: end - iteration - {master_ds_id}/{child_ds_id}")

    _df = read_parquet(sqlContext, [combined_base_data_path, tmp_mas_data_path]).distinct()
    _df.write.parquet(tmp_path, "overwrite")

    cu_path = cross_update(tmp_path, cai_key)
    _df = read_parquet(sqlContext, cu_path)
    _df.write.parquet(combined_base_data_path, "append")

    time.sleep(5)
    _1, _2 = s3_path_to_bucket_key(tmp_path)
    # delete_s3_folder(_1, _2)

    _1, _2 = s3_path_to_bucket_key(cu_path)
    # delete_s3_folder(_1, _2)

    if s3_path_exists(tmp_mas_data_path):
        _1, _2 = s3_path_to_bucket_key(tmp_mas_data_path)
        # delete_s3_folder(_1, _2)

    print(f"{current_datetime()} :: end -  creating master data")
    return combined_base_data_path


def cross_update(master_data_path, cai_key):
    print(f"{current_datetime()} :: start -  cross update")
    crs_upd_data_path = remove_slash(tmp_path) + "_crsupd/"
    _1, _2 = s3_path_to_bucket_key(crs_upd_data_path)
    delete_s3_folder(_1, _2)

    cross_upd_args = get_cross_upd_args(full_mapping_df)[cai_key]
    cross_upd_args = {cai_key: cross_upd_args}
    sql_di_to_process = {}
    for key1, val1 in cross_upd_args.items():
        sql_di_to_process[key1] = []
        for key2, val2 in val1.items():
            if isinstance(val2, dict) and "sql" in val2:
                sql_di_to_process[key1] += [val1['key'] + '~' + val2['sql']]
    print(f"{current_datetime()} :: data source id list to create master record - {sql_di_to_process}")

    master_df = read_parquet(sqlContext, master_data_path)
    master_df.createOrReplaceTempView(table_name)
    all_cols = master_df.columns
    cols_w_crs_upd = full_mapping_df[full_mapping_df.cross_update_flag.str.lower() == 'y'].cdm_column.unique().tolist()
    cols_w_crs_upd = [c.strip() for c in cols_w_crs_upd if c.strip() in all_cols]

    for master_ds_id, sql_li in sql_di_to_process.items():
        print(f"{current_datetime()} :: start - iteration - {master_ds_id}")
        crs_upd_sql = f"with base as (select * from {table_name} where lower(data_source_id) = '{master_ds_id.lower()}')"
        bef_cnt = sqlContext.sql(
            f"select 1 from {table_name} where lower(data_source_id) = '{master_ds_id.lower()}'").count()
        alias_li = []
        tmp_dfs = {}
        for i, sql_w_key in enumerate(sql_li):
            key = sql_w_key.split("~")[0]
            sql = sql_w_key.split("~")[-1]
            print(f"tmp{i} -> {sql}")
            tmp_dfs[f"tmp{i}"] = sqlContext.sql(sql)
            tmp_dfs[f"tmp{i}"].createOrReplaceTempView(f"tmp{i}")
            print(f'tmp{i} - count - {tmp_dfs[f"tmp{i}"].count()}')
            alias_li.append(f"tmp{i}")

        fin_sel_li = []
        for c in all_cols:
            if c in cols_w_crs_upd and alias_li:
                col_exp = f"coalesce({str(', '.join([f'{v}.{c}' for v in alias_li]))}, base.{c}) as {c}"
                fin_sel_li.append(col_exp)
            else:
                fin_sel_li.append(f"base.{c}")

        crs_upd_sql += f"select {', '.join(fin_sel_li)} from base "
        for a in alias_li:
            join_cond = ' and '.join([f"base.{k.strip()} = {a}.{k.strip()}" for k in key.split(",")])
            crs_upd_sql += f" left join {a} on {join_cond}"
        print(crs_upd_sql)
        crs_upd_df = sqlContext.sql(crs_upd_sql)
        aft_cnt = crs_upd_df.count()
        print(f"{current_datetime()} :: count after cross-update - {aft_cnt}")
        if bef_cnt != aft_cnt:
            raise Exception(f"counts before and after cross-update not matching")
        crs_upd_df.write.parquet(crs_upd_data_path, "append")
        time.sleep(5)
        print(f"{current_datetime()} :: end - iteration - {master_ds_id}")

    _1, _2 = s3_path_to_bucket_key(master_data_path)
    # delete_s3_folder(_1, _2)
    print(f"{current_datetime()} :: end -  cross update")
    return crs_upd_data_path


def identify_data_source(mas_data_path):
    print(f"{current_datetime()} :: identify data sources")
    pk_li = read_primary_keys(full_mapping_df)
    pk_wo_ds = [p for p in pk_li if p != 'data_source_id']
    join_cond = ' and '.join([f"a.{k} = b.{k}" for k in pk_wo_ds])
    df = read_parquet(sqlContext, mas_data_path).distinct()
    df.createOrReplaceTempView("combined_base_and_master_data")
    bef_cnt = df.count()
    print(f"{current_datetime()} :: count without data source indicator - {bef_cnt}")
    sql = f"with ds_li as (" \
          f"select {', '.join(pk_wo_ds)}, " \
          f"concat_ws(',', array_sort(collect_list(data_source_id))) as data_source_id_list " \
          f"from combined_base_and_master_data " \
          f"group by {', '.join(pk_wo_ds)}) " \
          f"select a.*, b.data_source_id_list " \
          f"from combined_base_and_master_data a join ds_li b " \
          f"on {join_cond}"
    print(sql)
    df_w_ds_li = sqlContext.sql(sql)
    aft_cnt = df_w_ds_li.count()
    print(f"{current_datetime()} :: count with data source indicator - {aft_cnt}")
    if bef_cnt != aft_cnt:
        raise Exception(f"counts before and after data source identification do not match")

    df_w_ds_li.write.parquet(tmp_path, "overwrite")

    _1, _2 = s3_path_to_bucket_key(mas_data_path)
    # delete_s3_folder(_1, _2)

    return tmp_path


def add_data_source_flags(crs_upd_data_path, ds_li):
    print(f"{current_datetime()} :: start -  adding flag fields")
    flg_upd_data_path = remove_slash(tmp_path) + "_flgupd/"
    crs_upd_df = read_parquet(sqlContext, crs_upd_data_path)
    crs_upd_df = crs_upd_df.withColumn("data_source_id_list", split(col("data_source_id_list"), ","))
    crs_upd_df.createOrReplaceTempView("crs_upd_vw")
    bef_cnt = crs_upd_df.count()
    print(f"{current_datetime()} :: count before adding flag fields - {bef_cnt}")

    # fetch all data source ids at granular level
    cai_args = get_cross_upd_args(full_mapping_df)
    print(f"{current_datetime()} :: fetch all data source ids at granular level")
    new_di = {}
    for k1, v1 in cai_args.items():
        d_li = []
        for k2, v2 in v1.items():
            if k2 == "key":
                continue
            d_li += [d.strip() for d in v2["data_source_id"].split(',')]
        new_d_li = []
        for d in d_li:
            if d in new_di:
                new_d_li += new_di[d]
        new_di[k1] = new_d_li + d_li
    print(json.dumps(new_di, indent=2))

    fin_di = {}
    for new_k in ds_li:
        fin_di[new_k] = ["'" + new_k + "'"]
        for k, v in new_di.items():
            if new_k in v:
                fin_di[new_k] += ["'" + k + "'"]
    fin_di = dict([(k, ",".join(v)) for k, v in fin_di.items()])
    print(json.dumps(fin_di, indent=2))

    sel_li = crs_upd_df.columns
    for ds in ds_li:
        col_expr = f"case when array_contains(data_source_id_list, '{ds}') and " \
                   f"array_contains(array({fin_di[ds]}), data_source_id) then 1 else 0 end as {ds.lower()}"
        sel_li.append(col_expr)
    sql = f"select {', '.join(sel_li)} from crs_upd_vw"
    print(sql)
    flg_upd_df = sqlContext.sql(sql)
    flg_upd_df = flg_upd_df.withColumn("data_source_id_list", concat_ws(",", col("data_source_id_list")))
    aft_cnt = flg_upd_df.count()
    print(f"{current_datetime()} :: count after adding flag fields - {aft_cnt}")
    if bef_cnt != aft_cnt:
        raise Exception(f"counts are not matching before and after adding flag fields")
    write_parquet(flg_upd_df, flg_upd_data_path)
    time.sleep(5)
    _1, _2 = s3_path_to_bucket_key(crs_upd_data_path)
    # delete_s3_folder(_1, _2)
    print(f"{current_datetime()} :: end -  adding flag fields")
    return flg_upd_data_path


def append_audit_columns(flg_upd_data_path, mas_ds_li):
    audit_col_data_path = remove_slash(tmp_path) + "_audupd/"

    if s3_path_exists(audit_col_data_path):
        _1, _2 = s3_path_to_bucket_key(audit_col_data_path)
        delete_s3_folder(_1, _2)

    df = read_parquet(sqlContext, flg_upd_data_path)
    df = df.filter(df.data_source_id.isin(mas_ds_li))
    df = df.drop(*audit_cols)

    if hash_reqd.upper() == "Y":
        scd_col_list = sorted(list(set(df.columns) - set(audit_cols)))
        # df = df.withColumn("hashkey", md5(concat_ws("-", *scd_col_list)))
        df = df.withColumn("hashkey", md5(concat_ws("-", *[coalesce(col(c).cast(StringType()), lit('-')) for c in
                                                           sorted(scd_col_list)])))
    df = df \
        .withColumn("add_date", to_date(lit(batch_date), "yyyyMMdd")) \
        .withColumn("delete_date", to_date(lit("99991231"), "yyyyMMdd")) \
        .withColumn("is_active", lit(1)) \
        .withColumn("is_incremental", lit(1)) \
        .withColumn("last_modified_date", to_timestamp(lit(current_datetime()), "yyyy-MM-dd HH:mm:ss"))

    write_parquet(df, audit_col_data_path)

    _1, _2 = s3_path_to_bucket_key(flg_upd_data_path)
    # delete_s3_folder(_1, _2)

    return audit_col_data_path


def load_tgt_data(fin_trf_path):
    print(f"\t{current_datetime()} :: load_tgt_data :: start")
    # apply incremental load functions
    print(f"{current_datetime()} :: load_tgt_data :: apply incremental load functions")
    delete_date = get_prev_date(batch_date)
    if load_fn_name.lower() == 'except_key_column_checksum_load':
        join_cnd = get_scd_join_condition(full_mapping_df)
        fn_call = f"{load_fn_name.lower()}(sqlContext, sc, src_path='{fin_trf_path}', tgt_path='{target_path}', " \
                  f"inc_args='{join_cnd}', del_date='{delete_date}', schema_overwrite='{schema_overwrite}')"
    else:
        fn_call = f"{load_fn_name.lower()}(sqlContext, sc, src_path='{fin_trf_path}', tgt_path='{target_path}', " \
                  f"inc_args={load_fn_args}, del_date='{delete_date}', schema_overwrite='{schema_overwrite}')"
    print(fn_call)
    response = eval(fn_call)
    load_sta = response['status']
    message = response['text']
    if load_sta.upper() != 'SUCCESS':
        raise Exception(message)
    print(f"\t{current_datetime()} :: load_tgt_data :: move data into target path")
    move_s3_folder_spark_with_partition(sqlContext, message, target_path, part_keys)
    print(f"\t{current_datetime()} :: load_tgt_data :: job execution completed")


if __name__ == "__main__":
    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
    try:
        args = getResolvedOptions(sys.argv,
                                  ['S3_BUCKET', 'CONFIG_FILE', 'TABLE_NAME', 'JOB_NAME', 'SPARK_PROPERTIES',
                                   'LOAD_TYPE', 'TGT_NAME', 'SCHEMA_OVERWRITE', 'PARTITION_KEYS'])
        bucket = args['S3_BUCKET']
        config_file = args['CONFIG_FILE']
        table_name = args['TABLE_NAME'].strip().lower()
        tgt_name = args['TGT_NAME'].strip().lower()
        spark_properties = args['SPARK_PROPERTIES']
        schema_overwrite = 'Y' if args['SCHEMA_OVERWRITE'].strip().upper() == 'Y' else 'N'
        part_keys = args['PARTITION_KEYS'].strip().lower()
        load_type = args['LOAD_TYPE'].lower().strip()
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
        print(f"{current_datetime()} :: main :: info - spark_properties : {spark_properties}")
        print(f"{current_datetime()} :: main :: info - load_type        : {load_type}")
        print(f"{current_datetime()} :: main :: info - job_name         : {job_name}")
        print(f"{current_datetime()} :: main :: info - job_run_id       : {job_run_id}")
        print(f"{current_datetime()} :: main :: info - tgt_name         : {tgt_name}")
        print(f"{current_datetime()} :: main :: info - partition keys   : {part_keys}")
        print(f"{current_datetime()} :: main :: info - schema_overwrite : {schema_overwrite}")
    print("*" * format_length)

    print(f"****** {current_datetime()} - START - {table_name} ******")
    try:
        print(f"{current_datetime()} :: main :: validate load type")
        if load_type != "active":
            raise Exception(f"invalid load type - {load_type}")
        print(f"{current_datetime()} :: main :: create spark context and sql context")
        sc, sqlContext = initialize_spark_context()
        print(f"{current_datetime()} :: main :: define params")
        define_params()
        print(f"{current_datetime()} :: main :: combine base data")
        combined_base_path = combine_base_data()
        print(f"{current_datetime()} :: main :: combined base data @ {combined_base_path}")
        cai_arg_keys = list(get_cross_upd_args(full_mapping_df))

        for cai_arg_key in cai_arg_keys:
            print(f"{current_datetime()} :: main :: start - {cai_arg_key}")
            print(f"{current_datetime()} :: main :: create master data")
            master_path = create_master_data(combined_base_path, cai_arg_key)
            print(f"{current_datetime()} :: main :: appended master data @ {master_path}")
            print(f"{current_datetime()} :: main :: end - {cai_arg_key}")

        # identify data sources
        print(f"{current_datetime()} :: main :: append ds id list")
        master_data_w_dsid = identify_data_source(master_path)

        # Get unique list of data source id from source
        read_parquet(sqlContext, master_data_w_dsid).createOrReplaceTempView(table_name)
        print(f"{current_datetime()} :: main :: get unique data source ids")
        ds_li_sql = f"select collect_set(data_source_id) as ds_id from {table_name} "
        ds_li = sqlContext.sql(ds_li_sql).collect()[0]['ds_id']
        print(f"{current_datetime()} :: main :: flags to be created are - {ds_li}")

        print(f"{current_datetime()} :: main :: flagging records")
        flg_upd_path = add_data_source_flags(master_data_w_dsid, ds_li)
        print(f"{current_datetime()} :: main :: flags updated data @ {flg_upd_path}")
        print(f"{current_datetime()} :: main :: append audit columns")
        fin_path = append_audit_columns(flg_upd_path, cai_arg_keys)
        print(f"{current_datetime()} :: main :: audit columns updated data @ {fin_path}")
        print(f"{current_datetime()} :: main :: load data into target")
        load_tgt_data(fin_path)
    except Exception as e:
        print("ERROR DETAILS - ", e)
        print(traceback.format_exc())
        raise e
    print(f"****** {current_datetime()} - END - {table_name} ******")
