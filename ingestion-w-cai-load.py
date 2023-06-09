import sys
import traceback

from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import md5, concat_ws, coalesce

from incremental_load_functions import *

# initialize script variables
format_length = 150


# function to get current date and time
def current_datetime():
    time.sleep(1)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# initialize spark context and sqlcontext
def initialize_spark_context(spark_props):
    sc = SparkContext(conf=SparkConf()
                      # .set("spark.sql.crossJoin.enabled", True)
                      .set("spark.driver.maxResultSize", "0")
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

    # spark.sql.crossJoin.enabled=true

    # Setting S3 max connection to 100
    hc = sc._jsc.hadoopConfiguration()
    hc.setInt("fs.s3.connection.maximum", 100)
    hc.setInt("fs.s3.maxRetries", 20)
    hc.set("fs.s3.multiobjectdelete.enable", "false")

    return sc, sqlContext


def define_params(bucket, config_file, table_name, tgt_name):
    # parse the config file contents
    print(
        f"\n\t{current_datetime()} :: define_params :: "
        f"info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:

        global source_path, target_path, tmp_path, cai_path, full_mapping_df, batch_date, source_path_without_tgt, \
            hash_reqd, load_fn_name, load_fn_args, cai_load_args, audit_cols, mapping_file

        param_data = get_s3_object(bucket, config_file)
        param_contents = json.loads(param_data)

        mapping_file = param_contents["ing_mapping"]
        batch_date = param_contents["batch_date"]
        audit_cols = param_contents["audit_columns"]

        root_path = param_contents["root_path"]
        src_base_dir = param_contents["ing_base_dir"]
        tgt_base_dir = param_contents["ing_w_cai_base_dir"]
        cai_base_dir = param_contents["cai_base_dir"]
        tmp_base_dir = tgt_base_dir + "_temp"

        source_path_wo_prefix = root_path + add_slash(src_base_dir) + add_slash(tgt_name)
        source_path_without_tgt = bucket_key_to_s3_path(bucket, root_path + add_slash(src_base_dir))
        source_path = bucket_key_to_s3_path(bucket, source_path_wo_prefix)
        target_path_wo_prefix = root_path + add_slash(tgt_base_dir) + add_slash(tgt_name) + add_slash(table_name)
        target_path = bucket_key_to_s3_path(bucket, target_path_wo_prefix)
        tmp_path_wo_prefix = root_path + add_slash(tmp_base_dir) + add_slash(tgt_name) + add_slash(table_name)
        tmp_path = bucket_key_to_s3_path(bucket, tmp_path_wo_prefix)
        cai_path_wo_prefix = root_path + add_slash(cai_base_dir)
        cai_path = bucket_key_to_s3_path(bucket, cai_path_wo_prefix)

        print(f"\n{current_datetime()} :: define_params :: reading mapping from {mapping_file}")
        full_mapping_df = read_mapping(mapping_file, table_name, tgt_name)
        # print(mapping_df)
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
        print(
            f"\t{current_datetime()} :: define_params :: error - "
            f"failed to read the config file {config_file} in bucket {bucket}")
        print("\terror details : ", err)
        raise err
    else:
        print(
            f"\t{current_datetime()} :: define_params :: info - "
            f"successfully read the config file {config_file} in bucket {bucket}\n")
        print(f"\t{current_datetime()} :: define_params :: info - source_path             - {source_path}")
        print(f"\t{current_datetime()} :: define_params :: info - target_path             - {target_path}")
        print(f"\t{current_datetime()} :: define_params :: info - temp path               - {tmp_path}")
        print(f"\t{current_datetime()} :: define_params :: info - cai_path                - {cai_path}")
        print(
            f"\n{current_datetime()} :: define_params :: cai load arguments             - {json.dumps(cai_load_args, indent=4)}")
        print(f"\n{current_datetime()} :: define_params :: cai load function name         - {load_fn_name}")
        print(f"\n{current_datetime()} :: define_params :: cai load function args         - {load_fn_args}")
        print(f"\n{current_datetime()} :: define_params :: Hash key generation required ? - {hash_reqd}")
        print("*" * format_length)


def get_cai_load_args(mapping_df):
    cai_arg_li = [s for s in pd.unique(mapping_df.cai_load_arg) if s.strip() != ""]
    return ast.literal_eval(cai_arg_li[0]) if cai_arg_li else None


def generate_oth_tx_sql():
    non_tx_map = full_mapping_df[full_mapping_df.cai_tx.str.strip().str.lower() == '']
    non_tx_map_cols = non_tx_map.cdm_column.unique()
    sel_col_li = [table_name + "." + c.strip() for c in non_tx_map_cols if c.strip() != '']
    from_clause = f"{table_name}"
    where_clause = f""
    oth_tx_map = full_mapping_df[full_mapping_df.cai_tx.str.strip().str.upper() == 'Y']
    if oth_tx_map.empty:
        return None

    for i in oth_tx_map.index:
        _map_typ = oth_tx_map.map_type[i].strip().lower()
        _src_tbl = oth_tx_map.src_table[i].strip()
        _src_aka = oth_tx_map.src_alias[i].strip()
        if _src_aka == '':
            _src_aka = _src_tbl
        _src_col = oth_tx_map.src_column[i].strip()
        _tgt_col = oth_tx_map.cdm_column[i].strip()
        sql_tx = oth_tx_map.sql_tx[i].strip()
        if sql_tx == '':
            sql_tx = f"{_src_aka}.{_src_col}"
        if "join" in _map_typ:
            if load_type == "full":
                # sql_tx += f" and {table_name}.add_date between {_src_aka}.add_date and {_src_aka}.delete_date"
                sql_tx += f" and {_src_aka}.is_active=1"
            from_clause += f" {_map_typ} {_src_tbl} as {_src_aka} on {sql_tx}"
        elif "where" in _map_typ:
            where_clause += f" {sql_tx}"
        else:
            sel_col_li.append(f"{sql_tx} as {_tgt_col}")

    if load_type == "full":
        sel_col_li += [f"{table_name}.add_date", f"{table_name}.delete_date", f"{table_name}.is_active",
                       f"{table_name}.is_incremental"]

    oth_tx_sql = f"select {', '.join(sel_col_li)} from {from_clause}"
    if where_clause != '':
        oth_tx_sql += f" where {where_clause}"

    return oth_tx_sql


def generate_cai_sql():
    non_cai_tx_map = full_mapping_df[full_mapping_df.cai_tx.str.strip().str.lower() != 'get_cai_id']
    select_list = list([table_name + "." + c.strip() for c in pd.unique(non_cai_tx_map.cdm_column) if c.strip() != ''])

    if load_type == "full":
        select_list.append(f"{table_name}.add_date")
        select_list.append(f"{table_name}.delete_date")
        select_list.append(f"{table_name}.is_active")
        select_list.append(f"{table_name}.is_incremental")

    cai_tx_map = full_mapping_df[full_mapping_df.cai_tx.str.strip().str.lower() == 'get_cai_id']
    from_clause = f"{table_name}"

    join_list = {}
    for i in cai_tx_map.index:
        cai_args = ast.literal_eval(cai_tx_map["cai_arg"][i])
        cai_table = cai_args["cai_table"].strip()
        join_cond = " ".join(cai_args["join_condition"].strip().split())
        out_colmn = cai_args["fn_output"].strip()
        cdm_colmn = cai_tx_map["cdm_column"][i].strip()
        key = f"{cai_table}~{join_cond}"
        val = f"{cai_table}.{out_colmn} as {cdm_colmn}"
        if key not in join_list:
            join_list[key] = [val]
        else:
            join_list[key] = join_list[key] + [val]

    for ji, job_key in enumerate(join_list):
        cai_table = job_key.split("~")[0]
        alias = cai_table + "_" + str(ji)
        join_cond = job_key.split("~")[-1].replace(f"{cai_table}.", f"{alias}.")
        select_list += [_x.replace(f"{cai_table}.", f"{alias}.") for _x in join_list[job_key]]
        from_clause += f" \nleft join \n\t{cai_table} as {alias} \non \n\t{join_cond} "

    sql = f'select \n\t' + ",\n\t".join(select_list) + f' \nfrom \n\t{from_clause}'
    return sql


def read_src_data(map_df):
    # print(cai_load_args)
    oth_tx_map = map_df[map_df.cai_tx.str.strip().str.upper() == 'Y']
    cai_tx_map = map_df[map_df.cai_tx.str.strip().str.lower() == 'get_cai_id']

    src_dfs = {}
    for i in cai_tx_map.index:
        cai_args = ast.literal_eval(cai_tx_map["cai_arg"][i])
        cai_table = cai_args["cai_table"]
        if cai_table in src_dfs:
            continue
        if s3_path_exists(cai_path + add_slash(cai_table)):
            src_dfs[cai_table] = read_parquet(sqlContext, cai_path + add_slash(cai_table))
        else:
            raise Exception(f"{cai_path + cai_table} - does not exist")

    src_nm_tbl_dict = get_grouped_tables_by_sources(oth_tx_map)
    print(f"src_nm_tbl_dict :: {src_nm_tbl_dict} ")
    for tbl_src_nm in src_nm_tbl_dict:
        for oth_tbl in src_nm_tbl_dict[tbl_src_nm]:
            if oth_tbl == table_name or oth_tbl == "":
                continue
            oth_table_path = source_path_without_tgt + add_slash(tbl_src_nm) + add_slash(oth_tbl)
            print(f"Starting to read : {oth_table_path}")
            if s3_path_exists(oth_table_path):
                if "master_feeds" == tbl_src_nm.strip().lower():
                    print(f"\t{current_datetime()} :: read_src_data :: read all records from {oth_tbl}")
                    src_dfs[oth_tbl] = read_parquet(sqlContext, oth_table_path)
                else:
                    if load_type == "active":
                        print(f"\t{current_datetime()} :: read_src_data :: read active records from {oth_tbl}")
                        src_dfs[oth_tbl] = read_active_records_from_parquet(sqlContext, oth_table_path)
                    elif load_type == "incremental":
                        print(f"\t{current_datetime()} :: read_src_data :: read active records from {oth_tbl}")
                        src_dfs[oth_tbl] = read_active_records_from_parquet(sqlContext, oth_table_path)
                    elif load_type == "full":
                        print(
                            f"\t{current_datetime()} :: read_src_data :: read all records from {oth_tbl} from {tbl_src_nm}")
                        pks = read_primary_keys(read_mapping(mapping_file, oth_tbl, tbl_src_nm))
                        if not pks:
                            pks = pd.unique(
                                read_mapping(mapping_file, oth_tbl, tbl_src_nm).cdm_column.str.strip()).tolist()
                            pks = [_ for _ in pks if _.strip() != '']
                        src_dfs[oth_tbl] = read_latest_records_from_parquet(sqlContext, oth_table_path, pks)
                    else:
                        raise Exception(f"invalid load type - {load_type}")
            else:
                raise Exception(f"{oth_table_path} - does not exist")

    src_table_path = source_path + add_slash(table_name)
    if s3_path_exists(src_table_path):
        if load_type == "active":
            print(f"\t{current_datetime()} :: read_src_data :: read active records from {table_name}")
            src_dfs[table_name] = read_active_records_from_parquet(sqlContext, src_table_path)
        elif load_type == "incremental":
            print(f"\t{current_datetime()} :: read_src_data :: read incremental records from {table_name}")
            src_dfs[table_name] = read_incremental_records_from_parquet(sqlContext, src_table_path)
        elif load_type == "full":
            print(f"\t{current_datetime()} :: read_src_data :: read all records from {table_name}")
            src_dfs[table_name] = read_parquet(sqlContext, src_table_path)
        else:
            raise Exception(f"invalid load type - {load_type}")
    else:
        raise Exception(f"{src_table_path} - does not exist")

    return src_dfs


def ing_with_cai_load():
    # read all source data into dataframe
    print(f"{current_datetime()} :: ing_with_cai_load :: start - load source data into dataframe")
    src_dfs = read_src_data(full_mapping_df)
    for _t in src_dfs:
        if _t != table_name:
            src_dfs[_t].persist()
        src_dfs[_t].createOrReplaceTempView(_t)
        print(f"\n{current_datetime()} :: ing_with_cai_load :: {_t} - {src_dfs[_t].count()}")

    print(f"{current_datetime()} :: ing_with_cai_load :: end - load source data into dataframe")
    print(f"\n{current_datetime()} :: ing_with_cai_load :: "
          f"count before assigning cai-ids - {src_dfs[table_name].count()}")

    # other transformation sql generation
    print(f"{current_datetime()} :: ing_with_cai_load :: generate sql for other transformations")
    oth_tx_sql = generate_oth_tx_sql()
    if oth_tx_sql is not None:
        print(f"{current_datetime()} :: ing_with_cai_load :: sql for other transformation is \n{oth_tx_sql}")
        oth_tx_op_df = sqlContext.sql(oth_tx_sql)
        oth_tx_op_df.createOrReplaceTempView(table_name)
        # write_parquet_partition_by_col(oth_tx_op_df, part_keys, remove_slash(tmp_path) + "_debug_oth_tx")
    else:
        print(f"{current_datetime()} :: ing_with_cai_load :: No other transformations defined")

    # cai assignment sql generation
    print(f"{current_datetime()} :: ing_with_cai_load :: generate cai sql")
    cai_upd_sql = generate_cai_sql()
    print(f"{current_datetime()} :: ing_with_cai_load :: cai sql is \n{cai_upd_sql}")

    # execute sql and capture results in dataframe
    output_df = sqlContext.sql(cai_upd_sql)
    # output_df.explain(True)
    print(f"\n{current_datetime()} :: ing_with_cai_load :: count after assigning cai-ids - {output_df.count()}")

    # setting column order
    tmp_map_df = full_mapping_df[full_mapping_df.cdm_column.str.strip() != '']
    sel_order = pd.unique(tmp_map_df.cdm_column.str.strip()).tolist()
    sel_order += [_ for _ in output_df.columns if _ not in sel_order]
    output_df = output_df.select(sel_order)

    print(f"\n{current_datetime()} :: ing_with_cai_load :: append audit columns and write unique data in temp location")
    # append audit columns to dataframe
    scd_col_list = sorted([c for c in output_df.columns if c not in audit_cols])
    if hash_reqd is not None and hash_reqd.upper() == "Y":
        output_df = output_df.distinct()
        output_df = output_df.withColumn("hashkey",
                                         md5(concat_ws("-", *[coalesce(col(c).cast(StringType()), lit('-')) for c in
                                                              sorted(scd_col_list)])))
    output_df = output_df.withColumn("last_modified_date", to_timestamp(lit(current_datetime())))
    if load_type == "full":
        write_parquet_partition_by_col(output_df, part_keys, target_path)
    else:
        output_df = output_df \
            .withColumn("add_date", to_date(lit(batch_date), "yyyyMMdd")) \
            .withColumn("delete_date", to_date(lit("99991231"), "yyyyMMdd")) \
            .withColumn("is_active", lit(1)) \
            .withColumn("is_incremental", lit(1))
        if s3_path_exists(target_path):
            write_parquet(output_df, tmp_path)
            print(f"{current_datetime()} :: ing_with_cai_load :: apply incremental load functions")
            del_date = get_prev_date(batch_date)
            fn_call = f"{load_fn_name.lower()}(sqlContext, sc, src_path='{tmp_path}', tgt_path='{target_path}', inc_args={load_fn_args}, del_date='{del_date}', schema_overwrite='{schema_overwrite}')"
            print(fn_call)
            response = eval(fn_call)
            load_sta = response['status']
            message = response['text']
            if load_sta.upper() != 'SUCCESS':
                raise Exception(message)
            print(f"\n{current_datetime()} :: ing_with_cai_load :: move data into target path")
            move_s3_folder_spark_with_partition(sqlContext, message, target_path, part_keys)
        else:
            write_parquet_partition_by_col(output_df, part_keys, target_path)
            # write_parquet(output_df, target_path)
    print(f"\n{current_datetime()} :: ing_with_cai_load :: execution completed")


# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
args = getResolvedOptions(sys.argv,
                          ['S3_BUCKET', 'CONFIG_FILE', 'SPARK_PROPERTIES', 'TABLE_NAME', 'TGT_NAME', 'LOAD_TYPE',
                           'JOB_NAME', 'SCHEMA_OVERWRITE', 'PARTITION_KEYS'])
try:
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    table_name = args['TABLE_NAME'].lower().strip()
    tgt_name = args['TGT_NAME'].lower().strip()
    spark_properties = args['SPARK_PROPERTIES']
    part_keys = args['PARTITION_KEYS'].strip().lower()
    load_type = args['LOAD_TYPE'].lower().strip()
    job_name = args['JOB_NAME']
    job_run_id = args['JOB_RUN_ID']
    schema_overwrite = 'Y' if args['SCHEMA_OVERWRITE'].strip().upper() == 'Y' else 'N'
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket            : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file       : {config_file}")
    print(f"{current_datetime()} :: main :: info - table_name        : {table_name}")
    print(f"{current_datetime()} :: main :: info - tgt_name          : {tgt_name}")
    print(f"{current_datetime()} :: main :: info - spark_properties  : {spark_properties}")
    print(f"{current_datetime()} :: main :: info - load_type         : {load_type}")
    print(f"{current_datetime()} :: main :: info - job_name          : {job_name}")
    print(f"{current_datetime()} :: main :: info - job_run_id        : {job_run_id}")
    print(f"{current_datetime()} :: main :: info - partition keys    : {part_keys}")
    print(f"{current_datetime()} :: main :: info - schema_overwrite  : {schema_overwrite}")
print("*" * format_length)

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
    if load_type not in ["active", "incremental", "full"]:
        raise Exception(f"incorrect load type {load_type}")

    print(f"{current_datetime()} :: main :: step 1 - define params")
    define_params(bucket, config_file, table_name, tgt_name)

    print(f"{current_datetime()} :: main :: step 2 - perform ingestion with CAI load")
    cai_tx_map = full_mapping_df[full_mapping_df.cai_tx.str.strip() != '']

    if cai_load_args is None and cai_tx_map.empty:
        print(f"\n{current_datetime()} :: main :: "
              f"cai load arguments are not defined for cdm_name - {tgt_name}, cdm_table - {table_name}")
        print(f"\n{current_datetime()} :: main :: "
              f"copying data from {source_path + add_slash(table_name)} to {target_path}")
        copy_s3_folder_spark_with_partition(sqlContext, source_path + add_slash(table_name), target_path,
                                            part_keys)
    else:
        ing_with_cai_load()

except Exception as e:
    print("ERROR DETAILS - ", e)
    print(traceback.format_exc())
    raise e
print(f"\n\n****** END - {table_name} ******")
