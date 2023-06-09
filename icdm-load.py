# import built-in libraries
import sys
import traceback

import pandas as pd
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import concat_ws, md5, coalesce

# import user-defined libraries
from incremental_load_functions import *

# initialize script variables
format_length = 150


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


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def define_params(bucket, config_file, table_name, arn):
    # parse the config file contents
    print(
        f"\n\t{current_datetime()} :: define_params :: info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:

        global source_path, target_path, tmp_path, master_path, sql_script_path, json_script_path, \
            custom_flag, tgt_bucket, param_contents, full_mapping_df, audit_columns, batch_date, input_dataset_file

        param_data = get_s3_object(bucket, config_file, arn)
        param_contents = json.loads(param_data)
        tgt_bucket = bucket if arn is None else param_contents["cdm_account_details"]["bucket"]
        audit_columns = param_contents['audit_columns']
        mapping_file = param_contents["icdm_mapping"]
        batch_date = param_contents["batch_date"]

        root_path = param_contents["root_path"]
        input_dataset_file = param_contents["input_dataset"]
        src_base_dir = param_contents["masked_base_dir"]
        # src_base_dir = param_contents["masked_filtered_base_dir"]
        tgt_base_dir = param_contents["icdm_base_dir"]
        tmp_base_dir = tgt_base_dir + "_temp"
        master_base_dir = param_contents["master_base_dir"]
        # master_base_dir = param_contents["master_filtered_base_dir"]
        sql_script_path = param_contents['sql_script_path']
        json_script_path = param_contents['json_script_path']

        source_path_wo_prefix = root_path + add_slash(src_base_dir)
        source_path = bucket_key_to_s3_path(tgt_bucket, source_path_wo_prefix)
        target_path_wo_prefix = root_path + add_slash(tgt_base_dir)
        target_path = bucket_key_to_s3_path(tgt_bucket, target_path_wo_prefix)
        tmp_path_wo_prefix = root_path + add_slash(tmp_base_dir)
        tmp_path = bucket_key_to_s3_path(tgt_bucket, tmp_path_wo_prefix)
        master_path_wo_prefix = root_path + add_slash(master_base_dir)
        master_path = bucket_key_to_s3_path(tgt_bucket, master_path_wo_prefix)

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


def read_src_data_from_sql(df, sql_text):
    if custom_flag == 'Y':
        src_tbl_dict = {"icdm": get_table_list_from_sql(sql_text)}
    else:
        src_name_li = [_.strip().lower() for _ in get_src_name(df).split(",")]
        src_name_alias_li = [f"src{_i + 1}_schema" for _i, _ in enumerate(src_name_li)]
        src_dict = dict(zip(src_name_alias_li, src_name_li))
        # get secondary source table list
        src_tbl_dict = {}
        for src_name_alias in src_name_alias_li:
            _tmp_li = list(set(_.replace(f'{src_name_alias}.', "") for _ in sql_text.split(" ") if src_name_alias in _))
            src_tbl_dict[src_dict[src_name_alias]] = _tmp_li

    src_dfs = {}
    for src_name, src_tbl_li in src_tbl_dict.items():
        for tbl in src_tbl_li:
            tbl_w_src = f"{tbl}" if custom_flag == 'Y' else f"{src_name}_{tbl}"
            if custom_flag == 'Y':
                fin_src_path = source_path + add_slash(tbl)
            else:
                if src_name in param_contents["src_dataset"]:
                    version = param_contents["src_dataset"][src_name]["version"]
                    version = batch_date if version == "" else version
                else:
                    version = batch_date
                fin_src_path = source_path + add_slash(src_name) + add_slash(version) + add_slash(tbl)
                # fin_src_path = source_path + add_slash(src_name) + add_slash(tbl)
            master_src_path = master_path + add_slash(tbl)

            if s3_path_exists(fin_src_path):
                if load_type == "active":
                    print(
                        f"\t{current_datetime()} :: read_src_data :: read active records for {tbl_w_src} from {fin_src_path}")
                    src_dfs[tbl_w_src] = read_active_records_from_parquet(sqlContext, fin_src_path)
                    if 'exclude_per_did' in [_c.lower() for _c in src_dfs[tbl_w_src].columns]:
                        src_dfs[tbl_w_src] = src_dfs[tbl_w_src].filter("nvl(exclude_per_did,0) != 1")
                elif load_type == "incremental":
                    print(
                        f"\t{current_datetime()} :: read_src_data :: read incremental records for {tbl_w_src} from {fin_src_path}")
                    src_dfs[tbl_w_src] = read_incremental_records_from_parquet(sqlContext, fin_src_path)
                    if 'exclude_per_did' in [_c.lower() for _c in src_dfs[tbl_w_src].columns]:
                        src_dfs[tbl_w_src] = src_dfs[tbl_w_src].filter("nvl(exclude_per_did,0) != 1")
                elif load_type == "full":
                    print(
                        f"\t{current_datetime()} :: read_src_data :: read all records for {tbl_w_src} from {fin_src_path}")
                    src_dfs[tbl_w_src] = read_parquet(sqlContext, fin_src_path)
                    if 'exclude_per_did' in [_c.lower() for _c in src_dfs[tbl_w_src].columns]:
                        src_dfs[tbl_w_src] = src_dfs[tbl_w_src].filter("nvl(exclude_per_did,0) != 1")
            elif s3_path_exists(master_src_path):
                print(f"\t{current_datetime()} :: read_src_data :: read all records from {tbl_w_src}")
                src_dfs[tbl_w_src] = read_parquet(sqlContext, master_src_path)
            else:
                print(f"read_src_data : {tbl_w_src} is not a valid table")
                print(f"read_src_data : {fin_src_path} and {master_src_path} - does not exist")

    # identifying final set of audit columns
    global fin_audit_columns
    fin_audit_columns = audit_columns
    return src_dfs


def read_src_data(map_df):
    src_dfs = {}
    src_nm_tbl_dict = get_grouped_tables_by_sources(map_df)
    print(f"{current_datetime()}:: list of tables to read :: {src_nm_tbl_dict}")
    # get driving table
    drv_tbl = get_main_table(map_df)
    print(f"\t{current_datetime()} :: read_src_data :: driving table is - {drv_tbl}")

    print(f"\t{current_datetime()} :: read_src_data :: reading source tables into dataframe")
    for src_name, sec_tbl_li in src_nm_tbl_dict.items():
        for tbl in sec_tbl_li:
            if custom_flag == 'Y':
                fin_src_path = source_path + add_slash(tbl)
            else:
                if src_name in param_contents["src_dataset"]:
                    version = param_contents["src_dataset"][src_name]["version"]
                    version = batch_date if version == "" else version
                else:
                    version = batch_date
                fin_src_path = source_path + add_slash(src_name) + add_slash(version) + add_slash(tbl)
                # fin_src_path = source_path + add_slash(src_name) + add_slash(tbl)
            master_src_path = master_path + add_slash(tbl)
            if s3_path_exists(fin_src_path):
                if tbl == drv_tbl:
                    if load_type == "active":
                        print(
                            f"\t{current_datetime()} :: read_src_data :: read active records for {tbl} from {fin_src_path}")
                        src_dfs[tbl] = read_active_records_from_parquet(sqlContext, fin_src_path)
                        if 'exclude_per_did' in [_c.lower() for _c in src_dfs[tbl].columns]:
                            src_dfs[tbl] = src_dfs[tbl].filter("nvl(exclude_per_did,0) != 1")
                    elif load_type == "incremental":
                        print(
                            f"\t{current_datetime()} :: read_src_data :: read incremental records for {tbl} from {fin_src_path}")
                        src_dfs[tbl] = read_incremental_records_from_parquet(sqlContext, fin_src_path)
                        if 'exclude_per_did' in [_c.lower() for _c in src_dfs[tbl].columns]:
                            src_dfs[tbl] = src_dfs[tbl].filter("nvl(exclude_per_did,0) != 1")
                    elif load_type == "full":
                        print(
                            f"\t{current_datetime()} :: read_src_data :: read all records for {tbl} from {fin_src_path}")
                        src_dfs[tbl] = read_parquet(sqlContext, fin_src_path)
                        if 'exclude_per_did' in [_c.lower() for _c in src_dfs[tbl].columns]:
                            src_dfs[tbl] = src_dfs[tbl].filter("nvl(exclude_per_did,0) != 1")
                else:
                    if load_type in ["incremental", "active"]:
                        print(
                            f"\t{current_datetime()} :: read_src_data :: read active records for {tbl} from {fin_src_path}")
                        src_dfs[tbl] = read_active_records_from_parquet(sqlContext, fin_src_path)
                        if 'exclude_per_did' in [_c.lower() for _c in src_dfs[tbl].columns]:
                            src_dfs[tbl] = src_dfs[tbl].filter("nvl(exclude_per_did,0) != 1")
                    elif load_type == "full":
                        print(
                            f"\t{current_datetime()} :: read_src_data :: read all records for {tbl} from {fin_src_path}")
                        src_dfs[tbl] = read_parquet(sqlContext, fin_src_path)
                        if 'exclude_per_did' in [_c.lower() for _c in src_dfs[tbl].columns]:
                            src_dfs[tbl] = src_dfs[tbl].filter("nvl(exclude_per_did,0) != 1")
            elif s3_path_exists(master_src_path):
                print(f"\t{current_datetime()} :: read_src_data :: read all records from {tbl}")
                src_dfs[tbl] = read_parquet(sqlContext, master_src_path)
            else:
                isFileMarkedRequired = check_src_file_validity(bucket, param_contents, "ICDM", tbl.lower(), src_name)
                if isFileMarkedRequired:
                    raise Exception(
                        f"Table {tbl} and source {src_name} marked required in input dataset but file not present in the s3 path {fin_src_path}")
                else:
                    print(
                        f"load_all_tables -  {tbl} marked unavailable in input dataset, skipping the current iteration...")
                    continue

    # identifying final set of audit columns
    global fin_audit_columns
    if drv_tbl in src_dfs:
        fin_audit_columns = list(set(src_dfs[drv_tbl].columns) & set(audit_columns))
    else:
        fin_audit_columns = audit_columns
    return src_dfs


def build_icdm_sql(map_df):
    p_tbl = '' if get_main_table(map_df) is None else get_main_table(map_df)
    # print("p_tbl ========== ", p_tbl)
    join_tp_dict = {"active": " and <alias:$2>.is_active = 1",
                    "date_based": " and <alias:$1>.add_date between <alias:$2>.add_date and <alias:$2>.delete_date"
                    }
    new_join_tp_li = [tp for tp in pd.unique(map_df["full_load_join_type"]) if tp.strip() != ''
                      and tp.strip().lower() not in join_tp_dict]
    if new_join_tp_li:
        raise Exception(f"build_icdm_sql - these join types not handled - {new_join_tp_li}")
    for k, v in join_tp_dict.items():
        map_df["full_load_join_type"] = map_df["full_load_join_type"].replace(k, v)

    if load_type == "full":
        map_df["sql_tx"] = map_df["sql_tx"] + " " + map_df["full_load_join_type"]

    query_from = "select distinct trim(src_table) as source " \
                 "from map_df " \
                 "where trim(src_table)<>'' and trim(src_table) is not null " \
                 "and trim(src_table) " \
                 "not in (select trim(src_table) from map_df where lower(map_type) like '%join%')"
    from_list = ps.sqldf(query_from)["source"].tolist()
    from_clause = " ".join(from_list)

    query_join = "select map_type || ' '|| trim(src_table) || ' as ' || (case when trim(src_alias)='' then trim(src_table) else trim(src_alias) end) || " \
                 "' ON ' || replace(replace(trim(sql_tx), '<alias:$2>', case when trim(src_alias)='' then trim(src_table) else trim(src_alias) end),'<alias:$1>', '" + p_tbl + "')  as source " \
                                                                                                                                                                               "from map_df where lower(map_type) like '%join%'"
    join_list = ps.sqldf(query_join)["source"].tolist()
    join_clause = " \n".join(join_list)

    query_where = "select trim(sql_tx) as source from map_df where lower(trim(map_type))='where' "
    where_list = ps.sqldf(query_where)["source"].tolist()
    where_clause = '' if where_list == [] else " WHERE " + where_list[0]

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
    select_list = ps.sqldf(query_select)["source"].tolist()
    if load_type == "full" and from_clause + '.*' not in select_list:
        select_list = select_list + [f"{from_clause}.{c}" for c in fin_audit_columns if c.lower().strip() != 'hashkey']
    select_clause = ',\n\t'.join(select_list)

    src_query = "SELECT \n\t" + select_clause + " \nFROM " + from_clause + " \n" + join_clause + where_clause
    return src_query


def get_icdm_load_args(inc_args):
    # define global variables
    global load_args, load_fn_name, load_fn_args, hash_reqd

    load_args = inc_args["load_fn"] if "load_fn" in inc_args else None
    load_fn_name = list(load_args)[0] if load_args is not None else None
    load_fn_args = load_args[load_fn_name] if load_args is not None else None
    hash_reqd = load_fn_args["hash_key_reqd"] if load_args is not None else 'N'

    print(
        f"\t{current_datetime()} :: get_icdm_load_args :: info - incremental load arguments     - {json.dumps(load_args, indent=4)}")
    print(f"\t{current_datetime()} :: get_icdm_load_args :: info - incremental load function name - {load_fn_name}")
    print(f"\t{current_datetime()} :: get_icdm_load_args :: info - incremental load function args - {load_fn_args}")
    print(f"\t{current_datetime()} :: get_icdm_load_args :: info - Hash key generation required ? - {hash_reqd}")


def create_union_structure(df_di):
    fin_df_dict = {}
    for _part_nm, df_li in df_di.items():
        if df_li:
            fin_df_dict[_part_nm] = df_li
    if fin_df_dict == {}:
        print(f"{current_datetime()} :: create_union_structure :: nothing to union")
        return fin_df_dict

    # identify all the dataframes under different partitions
    all_df_li = []
    for p_nm, df_li in df_di.items():
        all_df_li = all_df_li + df_li

    # get unique columns and its datatype
    unique_col_li = get_all_columns_from_df_list(all_df_li)
    print(unique_col_li)

    new_df_di = {}
    # create combined structure
    for p_nm, df_li in df_di.items():
        print(f"\t{current_datetime()} :: create_union_structure :: start :: partition - {p_nm}")
        df_len = len(df_li)
        if df_len == 0:
            raise Exception(f"create_union_structure : df_li is empty for {p_nm}")
        new_df_li = []
        for i, df in enumerate(df_li):
            print(f"\t{current_datetime()} :: create_union_structure :: start :: df - {i + 1}")
            print(df.columns)
            for c in unique_col_li:
                if c not in df.columns:
                    df = df.withColumn(c, lit(None).cast(unique_col_li[c]))
                else:
                    df = df.withColumn(c, col(c).cast(unique_col_li[c]))
            new_df_li.append(df)
            print(f"\t{current_datetime()} :: create_union_structure :: end :: df - {i + 1}")
        new_df_di[p_nm] = new_df_li
        print(f"\t{current_datetime()} :: create_union_structure :: end :: partition - {p_nm}")

    return new_df_di


def read_and_transform():
    # read incremental arguments from mapping
    print(f"\t{current_datetime()} :: read_and_transform :: read incremental load arguments")
    inc_args = get_inc_load_args(full_mapping_df)
    if inc_args is None:
        raise Exception(f"read_and_transform - incremental load arguments are not defined for cdm_table - {table_name}")

    # identify the partitions
    if "partitioned_load" in inc_args:
        part_li = [a.strip().lower() for a in inc_args["partitioned_load"]]
        if "default" in part_li:
            raise Exception("read_and_transform - Unexpected partition name 'default'")
    else:
        part_li = ["default"]
    print(f"\t{current_datetime()} :: read_and_transform :: partition list to iterate is - {part_li}")

    df_dict = {}
    # iterate through each partition
    for part_nm in part_li:
        print(f"\t{current_datetime()} :: read_and_transform :: start :: partition - {part_nm}")

        # filter mapping for current partition and derive final target path and temp path
        if part_nm.lower().strip() == "default":
            part_mapping_df = full_mapping_df
            load_arg = inc_args
        else:
            part_mapping_df = full_mapping_df[full_mapping_df.partition_name.str.strip().str.lower() == part_nm]
            load_arg = inc_args["partitioned_load"][part_nm]

        # initialize load arguments
        get_icdm_load_args(load_arg)

        out_df_li = []  # list of output dataframes
        # get rule order and rule set set list
        ro_rs_li = get_ruleorder_ruleset(part_mapping_df)

        for ro, rs_li in ro_rs_li.items():
            print(f"\t{current_datetime()} :: read_and_transform :: start :: rule order {ro}")
            for rs in rs_li:
                print(f"\t{current_datetime()} :: read_and_transform :: start :: rule set {rs}")
                curr_mapping = part_mapping_df[(part_mapping_df.rule_order == ro) & (part_mapping_df.rule_set == rs)]
                src_mapping_df = curr_mapping[curr_mapping.fn_tx.str.strip() == '']
                fn_mapping_df = curr_mapping[curr_mapping.fn_tx.str.strip() != '']
                fn_mapping_df = fn_mapping_df.reset_index(drop=True)
                fn_mapping_df["iteration_id"] = (fn_mapping_df.index / 10).astype(int)
                # src_name = get_src_name(curr_mapping)
                sql_file_nm = get_sql_file_name(curr_mapping)  # get sql file name
                print(f"sql_file_nm - {sql_file_nm}")

                # read source data into dataframes for current mapping
                print(f"\t{current_datetime()} :: read_and_transform :: read source data")
                if sql_file_nm is None:
                    src_dfs = read_src_data(src_mapping_df)
                else:
                    sql_script_file = add_slash(sql_script_path) + sql_file_nm
                    if not s3_path_exists(bucket_key_to_s3_path(bucket, sql_script_file)):
                        raise Exception(f"{bucket_key_to_s3_path(bucket, sql_script_file)} does not exist")

                    src_name_li = [_.strip().lower() for _ in get_src_name(src_mapping_df).split(",")]
                    src_name_alias_li = [f"src{_i + 1}_schema" for _i, _ in enumerate(src_name_li)]
                    src_dict = dict(zip(src_name_alias_li, src_name_li))
                    icdm_sql = get_s3_object(bucket, sql_script_file).decode('utf-8').replace("\n", " \n")
                    src_dfs = read_src_data_from_sql(src_mapping_df, icdm_sql)
                    if len(src_dfs) == 0:
                        raise Exception(f"could not load source data into dataframe")
                    for src_name_alias in src_name_alias_li:
                        icdm_sql = icdm_sql.replace(f"{src_name_alias}.", f"{src_dict[src_name_alias]}_")

                if len(src_dfs) != 0:
                    for src_tbl, src_df in src_dfs.items():
                        src_df.createOrReplaceTempView(src_tbl)

                    # get icdm source sql for current mapping
                    if sql_file_nm is None:
                        icdm_sql = build_icdm_sql(src_mapping_df)
                    print(
                        f"\t{current_datetime()} :: read_and_transform :: sql to execute for rule order = {ro} and rule set = {rs} is \n")
                    print(icdm_sql)

                    out_df = sqlContext.sql(icdm_sql)
                    out_df.createOrReplaceTempView("src")

                    if not fn_mapping_df.empty:
                        print(
                            f"\t{current_datetime()} :: read_and_transform :: iterating through function transformations \n")
                        src_cdm_map_dict = dict(zip(src_mapping_df.src_column, src_mapping_df.cdm_column))

                        iteration_list = pd.unique(fn_mapping_df.iteration_id)
                        print(f"{current_datetime()} - identified {len(iteration_list)} iterations")
                        for iter_id in iteration_list:
                            print(f"{current_datetime()} - start - {iter_id}")
                            fn_mapping_df_filt = fn_mapping_df[fn_mapping_df.iteration_id == iter_id]
                            for index in fn_mapping_df_filt.index:
                                fn = f"{fn_mapping_df_filt['fn_tx'][index]}"
                                fn_arg_1 = f"{fn_mapping_df_filt['fn_arg'][index]}"
                                fn_arg_2 = f"{fn_mapping_df_filt['cdm_column'][index]}"
                                fn_arg_3 = f"{src_cdm_map_dict[fn_mapping_df_filt['src_column'][index]]}"
                                fn_call = f"{fn}({fn_arg_1}, '{fn_arg_2}', '{fn_arg_3}')"
                                lookup_tbl, from_clause, select_clause = eval(fn_call)
                                lkp_src_name = f"{fn_mapping_df_filt['src_name'][index]}"
                                select_list = ['src.' + c for c in out_df.columns]
                                select_list.append(select_clause)

                                if lkp_src_name in param_contents["src_dataset"]:
                                    lkp_version = param_contents["src_dataset"][lkp_src_name]["version"]
                                    lkp_version = batch_date if lkp_version == "" else lkp_version
                                else:
                                    lkp_version = batch_date
                                lkp_path = source_path + add_slash(lkp_src_name) + add_slash(lkp_version) + add_slash(
                                    lookup_tbl)

                                # lkp_path = source_path + add_slash(lkp_src_name) + add_slash(lookup_tbl)

                                if load_type in ["active", "incremental"]:
                                    lkp_df = read_active_records_from_parquet(sqlContext, lkp_path)
                                else:
                                    lkp_df = read_active_records_from_parquet(sqlContext, lkp_path)
                                lkp_df.createOrReplaceTempView(lookup_tbl)
                                fn_sql = f"select {','.join(select_list)} {from_clause}"
                                print(f"SQL after applying fn_tx on {fn_arg_2} is :- {fn_sql}")
                                out_df = sqlContext.sql(fn_sql)
                                out_df.createOrReplaceTempView('src')

                            write_parquet(out_df, tmp_path + table_name + f"fn_iter_{iter_id}")

                            out_df = read_parquet(sqlContext, tmp_path + table_name + f"fn_iter_{iter_id}")
                            out_df.createOrReplaceTempView("src")
                            print(f"{current_datetime()} - end - {iter_id}")

                    distinct_row_mapping = curr_mapping[curr_mapping.src_unique_row_arg.str.strip() != '']
                    if not distinct_row_mapping.empty:
                        print(
                            f"{current_datetime()} :: read_and_transform :: Found duplicate delete script. Starting..")
                        json_script_path_full = bucket_key_to_s3_path(bucket, json_script_path) + \
                                                distinct_row_mapping['src_unique_row_arg'].iloc[0]
                        if s3_path_exists(json_script_path_full):
                            print(f"{current_datetime()} :: read_and_transform :: reading from {json_script_path_full}")
                            drop_dup_query_dict = json.loads(get_s3_object(bucket, add_slash(json_script_path) +
                                                                           distinct_row_mapping[
                                                                               'src_unique_row_arg'].iloc[
                                                                               0]))
                        else:
                            drop_dup_query_dict = json.loads(distinct_row_mapping['src_unique_row_arg'].iloc[0])
                        out_df = get_distinct_rows(sqlContext, out_df, drop_dup_query_dict, table_name)
                        print(f"\t{current_datetime()} :: read_and_transform :: Duplicate delete script ended.")

                    # append audit columns to dataframe
                    if hash_reqd.upper() == "Y":
                        scd_col_list = sorted(list(set(out_df.columns) - set(audit_columns)))
                        out_df = out_df.distinct()
                        out_df = out_df.withColumn("hashkey", md5(concat_ws("-", *[
                            coalesce(col(c).cast(StringType()), lit('-')) for c in sorted(scd_col_list)])))

                    out_df = out_df.withColumn("last_modified_date", to_timestamp(lit(current_datetime())))

                    if load_type in ['active', 'incremental']:
                        out_df = out_df \
                            .withColumn("add_date", to_date(lit(batch_date), "yyyyMMdd")) \
                            .withColumn("delete_date", to_date(lit("99991231"), "yyyyMMdd")) \
                            .withColumn("is_active", lit(1)) \
                            .withColumn("is_incremental", lit(1))

                    print(f"\t{current_datetime()} :: read_and_transform :: {ro} - {rs} - count {out_df.count()}")

                    out_df_li.append(out_df)
                    print(f"\t{current_datetime()} :: read_and_transform :: end :: rule set {rs}")

            print(f"\t{current_datetime()} :: read_and_transform :: end :: rule order {ro}")
        df_dict[part_nm] = out_df_li
        print(f"\t{current_datetime()} :: read_and_transform :: end :: partition - {part_nm}")
    new_df_dict = create_union_structure(df_dict)
    return new_df_dict


def load_tgt_data(df_dict):
    # read incremental arguments from mapping
    print(f"\t{current_datetime()} :: load_tgt_data :: read incremental load arguments")
    inc_args = get_inc_load_args(full_mapping_df)
    if inc_args is None:
        raise Exception(f"load_tgt_data - incremental load arguments are not defined for cdm_table - {table_name}")

    # identify the partitions
    if "partitioned_load" in inc_args:
        part_li = [a.strip().lower() for a in inc_args["partitioned_load"]]
        if "default" in part_li:
            raise Exception("load_tgt_data - Unexpected partition name 'default'")
    else:
        part_li = ["default"]
    print(f"\t{current_datetime()} :: load_tgt_data :: partition list to iterate is - {part_li}")

    # get combined structure
    print(f"\t{current_datetime()} :: load_tgt_data :: get combined structure")
    sel_order = df_dict[list(df_dict)[0]][0].columns

    # iterate through each partition
    for part_nm, new_df_li in df_dict.items():
        print(f"\t{current_datetime()} :: load_tgt_data :: start :: partition - {part_nm}")

        # filter mapping for current partition and derive final target path and temp path
        if part_nm.lower().strip() == "default":
            part_mapping_df = full_mapping_df
            fin_tmp_path = tmp_path + add_slash(table_name)
            fin_tgt_path = target_path + add_slash(table_name)
            load_arg = inc_args
        else:
            fin_tmp_path = tmp_path + add_slash(table_name) + f"src_name={part_nm}/"
            fin_tgt_path = target_path + add_slash(table_name) + f"src_name={part_nm}/"
            part_mapping_df = full_mapping_df[full_mapping_df.src_name.str.strip().str.lower() == part_nm]
            load_arg = inc_args["partitioned_load"][part_nm]
        print(f"\t{current_datetime()} :: load_tgt_data :: target path - {fin_tgt_path}")
        print(f"\t{current_datetime()} :: load_tgt_data :: temp   path - {fin_tmp_path}")

        # initialize load arguments
        get_icdm_load_args(load_arg)

        # if load type is full, overwrite target; otherwise load into temp and apply incremental logic
        if load_type == "full" or (not s3_path_exists(fin_tgt_path)):
            print(f"\t{current_datetime()} :: load_tgt_data :: start - load into target path")

            fin_part_keys = []

            for i, df in enumerate(new_df_li):
                print(f"\t start - iteration - {i} - count - {df.count()}")
                if i == 0:
                    for _k in part_keys.split(","):
                        part_key = _k.strip()
                        if part_key in df.columns:
                            fin_part_keys.append(part_key)

                    if fin_part_keys:
                        print(f"partition columns {fin_part_keys} found in schema, writing with partitioning.")
                        df.select(sel_order).write.partitionBy(*fin_part_keys).parquet(path=fin_tgt_path,
                                                                                       mode="Overwrite",
                                                                                       compression="snappy")
                    else:
                        print(f"partition columns {part_keys} not found in schema, writing without partitioning.")
                        df.select(sel_order).write.parquet(path=fin_tgt_path,
                                                           mode="Overwrite",
                                                           compression="snappy")
                else:
                    if fin_part_keys:
                        print(f"partition columns {fin_part_keys} found in schema, writing with partitioning.")
                        df.select(sel_order).write.partitionBy(*fin_part_keys).parquet(path=fin_tgt_path,
                                                                                       mode="Append",
                                                                                       compression="snappy")
                    else:
                        print(f"partition columns {part_keys} not found in schema, writing without partitioning.")
                        df.select(sel_order).write.parquet(path=fin_tgt_path,
                                                           mode="Append",
                                                           compression="snappy")
                print(f"\t  end  - iteration - {i}")
            print(f"\t{current_datetime()} :: load_tgt_data :: end - load into target path")
        else:
            # load into temp path
            print(f"\t{current_datetime()} :: load_tgt_data :: start - load into temp path")
            for i, df in enumerate(new_df_li):
                print(f"\t start - iteration - {i} - count - {df.count()}")
                if i == 0:
                    df.select(sel_order).write.parquet(path=fin_tmp_path, mode="Overwrite", compression="snappy")
                else:
                    df.select(sel_order).write.parquet(path=fin_tmp_path, mode="Append", compression="snappy")
                print(f"\t  end  - iteration - {i}")
            print(f"\t{current_datetime()} :: load_tgt_data :: end - load into temp path")

            # apply incremental logic
            print(f"\t{current_datetime()} :: load_tgt_data :: apply incremental load functions")
            delete_date = get_prev_date(batch_date)
            if load_fn_name.lower() == 'except_key_column_checksum_load':
                join_cnd = get_scd_join_condition(full_mapping_df)
                fn_call = f"{load_fn_name.lower()}(sqlContext, sc, src_path='{fin_tmp_path}', tgt_path='{fin_tgt_path}', inc_args='{join_cnd}', del_date='{delete_date}', schema_overwrite='{schema_overwrite}')"
            else:
                fn_call = f"{load_fn_name.lower()}(sqlContext, sc, src_path='{fin_tmp_path}', tgt_path='{fin_tgt_path}', inc_args={load_fn_args}, del_date='{delete_date}', schema_overwrite='{schema_overwrite}')"
            print(fn_call)
            response = eval(fn_call)
            load_sta = response['status']
            message = response['text']
            if load_sta.upper() != 'SUCCESS':
                raise Exception(message)
            print(f"\t{current_datetime()} :: load_tgt_data :: move data into target path")
            print(f"\tsource path = {message}\n\ttarget data = {fin_tgt_path}")
            move_s3_folder_spark_with_partition(sqlContext, message, fin_tgt_path, part_keys)
            print(f"\t{current_datetime()} :: load_tgt_data :: data movement to target path completed")
        print(f"\t{current_datetime()} :: load_tgt_data :: end :: partition - {part_nm}")
    print(f"\n{current_datetime()} :: load_tgt_data :: job completed successfully")


if __name__ == "__main__":
    # read the glue code arguments
    print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
    try:
        args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'TABLE_NAME', 'SPARK_PROPERTIES', 'LOAD_TYPE',
                                             'JOB_NAME', 'ARN', 'SCHEMA_OVERWRITE', 'PARTITION_KEYS'])
        bucket = args['S3_BUCKET']
        config_file = args['CONFIG_FILE']
        table_name = args['TABLE_NAME'].lower().strip()
        spark_properties = args['SPARK_PROPERTIES']
        load_type = args['LOAD_TYPE'].lower().strip()
        job_name = args['JOB_NAME']
        job_run_id = args['JOB_RUN_ID']
        arn = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None
        schema_overwrite = 'Y' if args['SCHEMA_OVERWRITE'].strip().upper() == 'Y' else 'N'
        part_keys = args['PARTITION_KEYS'].strip().lower()
    except Exception as err:
        print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
        print(f"{current_datetime()} :: main :: info - bucket                  : {bucket}")
        print(f"{current_datetime()} :: main :: info - config_file             : {config_file}")
        print(f"{current_datetime()} :: main :: info - table_name              : {table_name}")
        print(f"{current_datetime()} :: main :: info - spark_properties        : {spark_properties}")
        print(f"{current_datetime()} :: main :: info - load_type               : {load_type}")
        print(f"{current_datetime()} :: main :: info - job_name                : {job_name}")
        print(f"{current_datetime()} :: main :: info - job_run_id              : {job_run_id}")
        print(f"{current_datetime()} :: main :: info - arn                     : {arn}")
        print(f"{current_datetime()} :: main :: info - partition keys          : {part_keys}")
        print(f"{current_datetime()} :: main :: info - schema_overwrite        : {schema_overwrite}")
    print("*" * format_length)

    # config_file = "dev/config/dev_params.json"
    # bucket = "eurekapatient-j1-dev"
    # arn = None
    # table_name = "sp_transaction"
    # load_type = "full"

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
    #
    # print(f"broadcast_threshold_value :: {broadcast_threshold_value}")
    # print(f"shuffle_partition_value :: {shuffle_partition_value}")
    try:
        print(f"{current_datetime()} :: main :: step 0 - Initialize Spark Context with the defined properties")
        sc, sqlContext = initialize_spark_context(spark_properties)
        print(f"Spark set properties :: {sc.getConf().getAll()}")

        if load_type != "active" and table_name in ["patient", "diagnosis_code"]:
            raise Exception(f"{table_name} - should always be loaded with active")

        if load_type not in ["active", "incremental", "full"]:
            raise Exception(f"incorrect load type {load_type}")

        print(f"{current_datetime()} :: main :: step 1 - define params")
        define_params(bucket, config_file, table_name, arn)
        # fin_audit_columns = audit_columns
        # print(build_icdm_sql(full_mapping_df))
        print(f"{current_datetime()} :: main :: step 2 - read data from source and apply transformations")
        df_di = read_and_transform()
        if df_di == {}:
            print(f"{current_datetime()} :: main :: nothing to load")
        else:
            print(f"{current_datetime()} :: main :: step 3 - load into target by applying incremental rules")
            load_tgt_data(df_di)
    except Exception as e:
        print("ERROR DETAILS - ", e)
        print(traceback.format_exc())
        raise e
    print(f"\n\n****** END - {table_name} ******")
