# import built-in libraries
import sys
import traceback

from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import monotonically_increasing_id, explode, split, posexplode, collect_list, max

# import user-defined libraries
from cdm_utilities import *

# initialize script variables
format_length = 150


def initialize_spark_context(spark_props):
    sc = SparkContext(conf=SparkConf()
                      .set("spark.driver.maxResultSize", "0")
                      .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
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


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def define_params(bucket, config_file, tgt_name, table_name, layer):
    # parse the config file contents
    print(
        f"\n{current_datetime()} :: define_params :: info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:
        global tgt_path_full, ing_mapping_df
        tgt_path_full = tgt_version_path_full + add_slash(table_name)
        print(f"\n{current_datetime()} :: define_params :: reading mapping from {ing_mapping_file}")
        ing_mapping_df = read_mapping(ing_mapping_file, table_name, tgt_name)
        if ing_mapping_df.empty:
            raise Exception(f"No mapping available for cdm_name - {tgt_name}, cdm_table - {table_name}")
    except Exception as err:
        print(
            f"\n{current_datetime()} :: define_params :: error - failed to read the config file {config_file} in bucket {bucket}")
        print("error details : ", err)
        raise err
    else:
        print(f"\n{current_datetime()} :: define_params :: info - target_path          : {tgt_path_full}")
    print("*" * format_length)


def get_src_path(table_name):
    # identify final source path
    print(f"\n{current_datetime()} :: get_src_path :: read main source - {table_name}")
    global fin_src_path, fin_src_lkp_path

    main_src0_path = src0_path_full + add_slash(tgt_name) + add_slash(table_name)
    main_src1_path = src1_path_full + add_slash(tgt_name) + add_slash(table_name)
    main_src2_path = src2_path_full + add_slash(tgt_name) + add_slash(version) + add_slash(table_name)
    main_src3_path = src3_path_full + add_slash(tgt_name) + add_slash(version) + add_slash(table_name)
    # fin_src_lkp_path = main_src1_path
    if src3_path_full != '' and s3_path_exists(main_src3_path):
        fin_src_path = main_src3_path
    elif src2_path_full != '' and s3_path_exists(main_src2_path):
        fin_src_path = main_src2_path
    elif s3_path_exists(main_src1_path):
        fin_src_path = main_src1_path
    elif s3_path_exists(main_src0_path):
        fin_src_path = main_src0_path
    else:
        raise Exception(f"{main_src0_path}, {main_src1_path} and {main_src2_path} - does not exist")

    if s3_path_exists(main_src1_path):
        fin_src_lkp_path = main_src1_path
    elif s3_path_exists(main_src0_path):
        fin_src_lkp_path = main_src0_path
    else:
        raise Exception(f"{main_src0_path} and {main_src1_path} - does not exist")
    print(f"{current_datetime()} - fin_src_path    = {fin_src_path}")
    print(f"{current_datetime()} - fin_src_lkp_path= {fin_src_lkp_path}")


def read_src_data(map_df, table_name):
    """
    This function reads all source tables from s3 path into dataframe
    and returns dictionary of dataframes
    :param map_df:
    :return:
    """
    src_dfs = {}
    global fin_audit_cols, v_select_order

    # read source data into dataframe
    if load_type == "active":
        print(f"\n{current_datetime()} :: read_src_data :: read active records from {fin_src_path}")
        src_df = read_active_records_from_parquet(sqlContext, fin_src_path)
    elif load_type == "incremental":
        print(f"\n{current_datetime()} :: read_src_data :: read incremental records from {fin_src_path}")
        src_df = read_incremental_records_from_parquet(sqlContext, fin_src_path)
    elif load_type == "full":
        print(f"\n{current_datetime()} :: read_src_data :: read all records from {fin_src_path}")
        src_df = read_parquet(sqlContext, fin_src_path)
    else:
        raise Exception(f"invalid load type - {load_type}")

    if src_df is None:
        raise Exception(f"read_src_data - could not load {fin_src_path} into dataframe as {table_name}")
    src_dfs[table_name] = src_df
    fin_audit_cols = list(set(src_df.columns) & set(audit_cols))
    v_select_order = [_ for _ in src_dfs[table_name].columns if _ not in fin_audit_cols]
    print(f"v_select_order - {v_select_order}")
    # read lookup data into dataframe
    lookup_map_df = map_df[map_df.rule_type.str.strip().str.lower() == "lookup"]
    for i in lookup_map_df.index:
        filtered_map = lookup_map_df[lookup_map_df.index == i]
        lkp_tbl = lookup_map_df["lookup"][i] if "lookup" in lookup_map_df else ''
        add_lkp_arg_dict = lookup_map_df["lookup_arg_addl"][i] if "lookup_arg_addl" in lookup_map_df else ''
        lkp_src = lookup_map_df["lookup_src"][i] if "lookup_src" in lookup_map_df else ''
        lkp_typ = lookup_map_df["lookup_type"][i] if "lookup_type" in lookup_map_df else ''
        lkp_cnd = lookup_map_df["sql_tx"][i] if "sql_tx" in lookup_map_df else ''
        lkp_aka = lookup_map_df["lookup_alias"][i] if "lookup_alias" in lookup_map_df else ''
        lkp_orig = lookup_map_df["lookup_origin"][i].strip().lower() if "lookup_origin" in lookup_map_df else 'target'
        lkp_orig = 'target' if lkp_orig.strip() == '' else lkp_orig
        full_load_typ = lookup_map_df["full_load_join_type"][i] if "full_load_join_type" in lookup_map_df else ''

        if lkp_aka == '':
            raise Exception(f"read_src_data :: lookup alias not defined for {lkp_typ} \n")

        print(f"\n{current_datetime()} :: read_src_data :: read lookup source - {lkp_tbl}")

        if lkp_src.strip().lower() == "master":
            lkp_path = master_path_full + add_slash(lkp_tbl)
            print(f"\n{current_datetime()} :: read_src_data :: reading lookup source - {lkp_tbl} from {lkp_path}")
            src_dfs[lkp_aka] = read_parquet(sqlContext, lkp_path)
        elif lkp_src.strip().lower() == "cai":
            lkp_path = cai_path_full + add_slash(lkp_tbl)
            print(f"\n{current_datetime()} :: read_src_data :: reading lookup source - {lkp_tbl} from {lkp_path}")
            src_dfs[lkp_aka] = read_parquet(sqlContext, lkp_path)
        else:
            if lkp_src in param_contents["src_dataset"]:
                lkp_version = param_contents["src_dataset"][lkp_src]["version"]
                lkp_version = batch_date if lkp_version == "" else lkp_version
            else:
                lkp_version = batch_date

            if lkp_orig == 'target':
                lkp_path = tgt_root_path_full + add_slash(lkp_src) + add_slash(lkp_version) + add_slash(lkp_tbl)
                print(
                    f"\n{current_datetime()} :: read_src_data :: reading target lookup - {lkp_tbl} for load_type ::: {load_type} and {full_load_typ} from {lkp_path}")
                if load_type == "full":
                    if full_load_typ.strip().lower() == "latest_record":
                        pkeys = read_primary_keys(read_mapping(ing_mapping_file, lkp_tbl, lkp_src))
                        print(f"{current_datetime()} :: read_src_data :: reading from {lkp_path}")
                        src_dfs[lkp_aka] = read_latest_records_from_parquet(sqlContext, lkp_path, pkeys)
                    elif full_load_typ.strip().lower() == "active":
                        print(f"{current_datetime()} :: read_src_data :: reading from {lkp_path}")
                        src_dfs[lkp_aka] = read_active_records_from_parquet(sqlContext, lkp_path)
                    else:
                        print(f"{current_datetime()} :: read_src_data :: reading from {lkp_path}")
                        src_dfs[lkp_aka] = read_parquet(sqlContext, lkp_path)
                else:
                    print(f"{current_datetime()} :: read_src_data :: reading from {lkp_path}")
                    src_dfs[lkp_aka] = read_active_records_from_parquet(sqlContext, lkp_path)
            elif lkp_orig == 'source':
                lkp0_path = src0_path_full + add_slash(lkp_src) + add_slash(lkp_tbl)
                lkp1_path = src1_path_full + add_slash(lkp_src) + add_slash(lkp_tbl)
                lkp2_path = src2_path_full + add_slash(lkp_src) + add_slash(lkp_version) + add_slash(lkp_tbl)
                if src2_path_full != '' and s3_path_exists(lkp2_path):
                    lkp_path = lkp2_path
                elif s3_path_exists(lkp1_path):
                    lkp_path = lkp1_path
                elif s3_path_exists(lkp0_path):
                    lkp_path = lkp0_path
                else:
                    raise Exception(f"{lkp1_path} and {lkp2_path} - does not exist")
                # print(lkp_path)
                # lkp_path = rreplace(fin_src_lkp_path, add_slash(table_name), add_slash(lkp_tbl))
                print(
                    f"\n{current_datetime()} :: read_src_data :: reading source lookup - {lkp_tbl} for load_type ::: {load_type} and {full_load_typ} from {lkp_path}")
                if load_type == "full":
                    if full_load_typ.strip().lower() == "latest_record":
                        pkeys = read_primary_keys(read_mapping(ing_mapping_file, lkp_tbl, lkp_src))
                        print(f"{current_datetime()} :: read_src_data :: reading from {lkp_path}")
                        src_dfs[lkp_aka] = read_latest_records_from_parquet(sqlContext, lkp_path, pkeys)
                    elif full_load_typ.strip().lower() == "active":
                        print(f"{current_datetime()} :: read_src_data :: reading from {lkp_path}")
                        src_dfs[lkp_aka] = read_active_records_from_parquet(sqlContext, lkp_path)
                    else:
                        print(f"{current_datetime()} :: read_src_data :: reading from {lkp_path}")
                        src_dfs[lkp_aka] = read_parquet(sqlContext, lkp_path)
                else:
                    print(f"{current_datetime()} :: read_src_data :: reading from {lkp_path}")
                    src_dfs[lkp_aka] = read_active_records_from_parquet(sqlContext, lkp_path)
            else:
                raise Exception(f"incorrect lookup origin value {lkp_orig}")

        if src_dfs[lkp_aka] is None:
            raise Exception(f"read_src_data :: could not load {lkp_tbl} as {lkp_aka} into dataframe\n")

        # applying additional lookup arg
        if len(add_lkp_arg_dict) != 0:
            print("add_lkp_arg_dict", add_lkp_arg_dict)
            col_list = add_lkp_arg_dict['lookup_cols'] if 'lookup_cols' in add_lkp_arg_dict else None
            lookup_filter = add_lkp_arg_dict['lookup_filter'] if 'lookup_filter' in add_lkp_arg_dict else None
            if lookup_filter:
                src_dfs[lkp_aka] = src_dfs[lkp_aka].where(lookup_filter)
            if col_list:
                col_list = col_list.split(',')
                col_list = [_.strip() for _ in col_list if _ != '']
                if full_load_typ == 'date_based':
                    col_list += ['add_date', 'delete_date']
                print("Columns in picture: ", col_list)
                src_dfs[lkp_aka] = src_dfs[lkp_aka].selectExpr(*col_list).distinct()

        # if lookup type is pattern, derive all values falling under pattern
        # and load into temp path; consider this as lookup and proceed with equi join
        if lkp_typ.strip().lower() == "pattern":
            lkp_path = load_derived_masters_from_patterns(filtered_map, src_dfs, table_name)
            src_dfs[lkp_aka] = read_parquet(sqlContext, lkp_path)
            if src_dfs[lkp_aka] is None:
                raise Exception(f"read_src_data :: could not load {lkp_aka} into dataframe\n")

    # if recoding is required, apply it and read recoded data as source data
    recode_mapping = map_df[(map_df['custom'].str.strip().str.upper() == 'Y') &
                            (map_df.rule_type.str.strip().str.lower() != "lookup")]
    if len(recode_mapping.index) > 1:
        raise Exception(f"multiple levels of recoding not enabled in current version")
    if not recode_mapping.empty:
        rule = recode_mapping['rule'].unique()[0].strip().lower().replace("class:", '')
        column = recode_mapping['object_name'].unique().tolist()[0]
        print(f"execute custom function - {rule}")
        fn_call = f"{rule}(recode_mapping, lookup_map_df, src_dfs, table_name)"
        print(fn_call)
        recoded_path = eval(fn_call)
        src_dfs[table_name] = read_parquet(sqlContext, recoded_path)
        if src_dfs[table_name] is None:
            raise Exception(f"read_src_data :: could not load {table_name} into dataframe\n")

    return src_dfs


def load_derived_masters_from_patterns(df, src_dfs, table_name):
    lkp_tbl = pd.unique(df["lookup"])[0] if "lookup" in df else ''
    lkp_src = pd.unique(df["lookup_src"])[0] if "lookup_src" in df else ''
    lkp_typ = pd.unique(df["lookup_type"])[0] if "lookup_type" in df else ''
    lkp_cnd = pd.unique(df["sql_tx"])[0] if "sql_tx" in df else ''
    lkp_aka = pd.unique(df["lookup_alias"])[0] if "lookup_alias" in df else ''
    custom_flg = pd.unique(df["custom"])[0] if "custom" in df else ''

    src_dfs[lkp_aka].createOrReplaceTempView(lkp_tbl)
    src_dfs[table_name].createOrReplaceTempView(table_name)

    lkp_col_lst = [c.lower() for c in src_dfs[lkp_aka].columns]
    rhs = [r for r in lkp_cnd.split('=') if (lkp_aka + "." in r) or (r.strip().lower() in lkp_col_lst)][0]
    lhs = list(set(lkp_cnd.split('=')) - {rhs})[0]
    src_col = lhs.split(".")[-1].strip()
    cnd1 = """((position('*' in """ + rhs + """) > 0 and """ + lhs + """ like replace(""" + rhs + """,'*','') || '%')"""
    cnd2 = """(position('*' in """ + rhs + """) = 0 and """ + lhs + """ = """ + rhs + """))"""
    tmp_lkp_cnd = cnd1 + ' or ' + cnd2

    if custom_flg == "Y":
        temp_df = src_dfs[table_name].withColumn(src_col, explode(split(src_dfs[table_name][src_col], "\^")))
        temp_df.createOrReplaceTempView(table_name)

    rhs_ = rhs.split(".")[-1].strip().lower()
    oth_cols = ', '.join([c for c in src_dfs[lkp_aka].columns if c.strip().lower() != rhs_])
    new_lkp_sql = f"with {table_name} as  " \
                  f"(select distinct {lhs} from {table_name}) " \
                  f"select {lhs} as {rhs_}, {oth_cols} " \
                  f"from {table_name} " \
                  f"join {lkp_tbl} as {lkp_aka} " \
                  f"on {tmp_lkp_cnd}"
    print(f"\n{current_datetime()} :: load_derived_masters_from_patterns :: "
          f"lookup SQL to perform pattern matching - \n{new_lkp_sql}")
    new_lkp_df = sqlContext.sql(new_lkp_sql).distinct()

    print(f"\n{current_datetime()} :: load_derived_masters_from_patterns :: "
          f"start : write new lookup in temp path")
    write_parquet(new_lkp_df, tmp_path_full + add_slash(table_name + "_" + lkp_aka))
    print(f"\n{current_datetime()} :: load_derived_masters_from_patterns :: "
          f"end : write new lookup in temp path")
    return tmp_path_full + add_slash(table_name + "_" + lkp_aka)


def get_recoded_dx_code(df, lkp_map_df, src_df_dict, table_name):
    lkp_tbl = pd.unique(lkp_map_df["lookup"])[0] if "lookup" in df else ''
    obj_nm = pd.unique(df["object_name"])[0]
    lkp_cnd = pd.unique(lkp_map_df["sql_tx"])[0] if "sql_tx" in lkp_map_df else ''
    lkp_aka = pd.unique(lkp_map_df["lookup_alias"])[0] if "lookup_alias" in df else ''

    src_df_dict[lkp_aka].createOrReplaceTempView(lkp_aka)
    src_df_dict[table_name].createOrReplaceTempView(table_name)

    not_recoded_cols = [c for c in src_df_dict[table_name].columns if c.strip().lower() != obj_nm.strip().lower()]

    # adding unique_id column which creates a unique number for each row;
    src_df = src_df_dict[table_name].withColumn("unique_id", monotonically_increasing_id())
    write_parquet(src_df, tmp_path_full + table_name + "_dx_recoded_src")
    src_df = read_parquet(sqlContext, tmp_path_full + table_name + "_dx_recoded_src")
    src_df.createOrReplaceTempView(table_name)

    # capture unique_id and column to mask in a temp dataframe
    temp_df = src_df.select(obj_nm, "unique_id")
    temp_df = temp_df.select("unique_id", posexplode(split(temp_df[obj_nm], "\^")).alias("dx_index", obj_nm))
    # temp_df.show(5, False)
    write_parquet(temp_df, tmp_path_full + table_name + "_dx_recoded_temp")
    temp_df = read_parquet(sqlContext, tmp_path_full + table_name + "_dx_recoded_temp")
    # temp_df.show(5, False)
    temp_df.createOrReplaceTempView("temp_" + table_name)

    masking_sql = f" select unique_id, dx_index, " \
                  f" case when {lkp_aka}.code_alias1 is not null then null else {obj_nm} end as {obj_nm} " \
                  f" from temp_{table_name} " \
                  f" left join {lkp_aka} " \
                  f" on {lkp_cnd.replace(table_name + '.', 'temp_' + table_name + '.')} "
    print(masking_sql)
    masking_df = sqlContext.sql(masking_sql)
    w = Window.partitionBy('unique_id').orderBy('dx_index')
    sorted_list_df = masking_df.withColumn('sorted_list', collect_list(obj_nm).over(w)).groupBy('unique_id').agg(
        max('sorted_list').alias(obj_nm))
    # sorted_list_df.show(5, False)
    sorted_list_df.createOrReplaceTempView("sorted_list_df")

    masking_sql_2 = f" select unique_id, " \
                    f" concat_ws('^', {obj_nm}) as {obj_nm} " \
                    f" from sorted_list_df "
    print(masking_sql_2)
    masking_df_2 = sqlContext.sql(masking_sql_2)
    # masking_df_2.show(5, False)
    masking_df_2.createOrReplaceTempView("masked")
    fin_sql = f" select {', '.join(not_recoded_cols)}, " \
              f" case when trim(m.{obj_nm}) = '' then null else m.{obj_nm} end as {obj_nm}" \
              f" from {table_name} s" \
              f" left join masked m" \
              f" on s.unique_id = m.unique_id"

    print(fin_sql)
    fin_df = sqlContext.sql(fin_sql)

    write_parquet(fin_df, remove_slash(tgt_path_full) + "_tmp/")
    print("Moving from tmp to tgt...")
    move_s3_folder_spark(sqlContext, remove_slash(tgt_path_full) + "_tmp/", tgt_path_full)
    # cleaning-up temp path
    print(f"start : cleaning temporary path")
    delete_s3_folder_spark(sqlContext, tmp_path_full + table_name + "_dx_recoded_src")
    delete_s3_folder_spark(sqlContext, tmp_path_full + table_name + "_dx_recoded_temp")
    print(f"end : cleaning temporary path")
    return tgt_path_full


def get_recoded_payer_plan(df, lkp_map_df, src_df_dict, table_name):
    lkp_tbl = pd.unique(lkp_map_df["lookup"])[0] if "lookup" in df else ''
    obj_nm = pd.unique(df["object_name"])[0]
    lkp_aka = pd.unique(lkp_map_df["lookup_alias"])[0] if "lookup_alias" in df else ''

    src_df_dict[lkp_aka].createOrReplaceTempView(lkp_tbl)
    src_df_dict[table_name].createOrReplaceTempView(table_name)

    # print(df)

    # get values from columns which should be recoded and load into recoded_payer_plan
    col_di = df["parameters"].tolist()[0]
    ele_li = [v.strip() for k, v in col_di.items() if "element_name" in k]
    cls_li = [v.strip() for k, v in col_di.items() if "element_class" in k]
    ele_cls_di = dict(zip(ele_li, cls_li))
    # print(ele_cls_di)

    # get values from columns which should be recoded and load into recoded_payer_plan
    sql_li = [f"select nvl({column},'def@ult') as name from {table_name}" for column in ele_li]
    union_sql = ' union '.join(sql_li)
    print(union_sql)

    # read recoded_payer_plan
    recoded_pp = read_parquet(sqlContext, master_path_full + f"{tgt_name}_{table_name}_recoded_payer_plan")
    if recoded_pp is not None:
        recoded_pp.createOrReplaceTempView("recoded_payer_plan")
        max_id = sqlContext.sql("select max(id) as max_id from recoded_payer_plan").collect()[0]["max_id"]
    else:
        recoded_pp = sqlContext.sql(f"select 0 as id, "
                                    f"cast(null as string) as name, "
                                    f"{', '.join(src_df_dict[lkp_aka].columns)} "
                                    f"from restricted_payer_plan where 1<>1 ")
        recoded_pp.createOrReplaceTempView("recoded_payer_plan")
        max_id = 0

    insert_sql = f"with src as  \
                ( {union_sql} ), \
                derived as \
                ( \
                  select *, row_number() over (partition by name order by rpp.priority nulls last) as rn \
                  from src \
                  left join {lkp_tbl} rpp \
                  on ((lower(match_type) = 'pattern' and lower(src.name) like '%'||lower(rpp.plan_or_payer_name_or_payer_group)||'%') \
                  or (lower(match_type) = 'exact' and src.name = rpp.plan_or_payer_name_or_payer_group)) \
                ) \
                select row_number() over (order by name) + {max_id} as id, " \
                 f"case when name = 'def@ult' then null else name end as name, " \
                 f"{', '.join(src_df_dict[lkp_aka].columns)} \
                from derived  \
                where rn = 1 and not exists " \
                 f"(select 1 from recoded_payer_plan b where nvl(b.name,'def@ult')=derived.name) \
                "
    print(insert_sql)
    insert_df = sqlContext.sql(insert_sql)
    cnt = insert_df.count()
    print(f"No of new records to load in recoded_payer_plan : {cnt}")

    if cnt == 0:
        print("Nothing to insert; skipping write operation")
    else:
        final_df = union_dataframes([recoded_pp, insert_df]).coalesce(1)
        print(f"No of records in recoded_payer_plan : after load : {final_df.count()}")
        write_parquet(final_df,
                      master_path_full + f"{tgt_name}_{table_name}_recoded_payer_plan",
                      master_path_full + f"{tgt_name}_{table_name}_tmp_recoded_payer_plan")

    # read updated recoded_payer_plan
    recoded_pp = read_parquet(sqlContext, master_path_full + f"{tgt_name}_{table_name}_recoded_payer_plan")
    recoded_pp.createOrReplaceTempView("recoded_payer_plan")

    # derive flag to identify whether recoded or not
    ing_col_li = src_df_dict[table_name].columns
    select_list = ['src.' + c for c in ing_col_li]
    from_clause = f" {table_name} as src"
    flag = []
    fin_pattern = []
    for ind, rec_col in enumerate(ele_cls_di):
        alias = "rpp" + str(ind + 1)
        from_clause += f" left join recoded_payer_plan as {alias} on nvl(src.{rec_col},'def@ult') = nvl({alias}.name,'def@ult')"
        flag.append(f"{alias}.plan_or_payer_name_or_payer_group is not null")
        fin_pattern.append(f"{alias}.plan_or_payer_name_or_payer_group")
    # print(from_clause)
    flag = f"case when {' or '.join(flag)} then 1 else 0 end as {obj_nm}"
    fin_pattern = f"coalesce({', '.join(fin_pattern)}) as fin_pattern"
    select_list.append(flag)
    select_list.append(fin_pattern)

    tmp1_sql = f"select {', '.join(select_list)} " \
               f"from {from_clause} "
    print(tmp1_sql)
    tmp1_df = sqlContext.sql(tmp1_sql)
    tmp1_df.createOrReplaceTempView("tmp1")

    # add default recoding patterns
    tmp2_sql = f"select tmp1.*, rpp.plan_name as pp_def_plan_name, " \
               f"rpp.organization_name as pp_def_organization_name, " \
               f"rpp.admin_name as pp_def_admin_name " \
               f"from tmp1 left join {lkp_tbl} rpp " \
               f"on upper(tmp1.fin_pattern) = upper(rpp.plan_or_payer_name_or_payer_group)"
    print(tmp2_sql)
    tmp2_df = sqlContext.sql(tmp2_sql)
    tmp2_df.createOrReplaceTempView("tmp2")

    # Apply recoding to masked table and create new table in masked layer
    ing_col_li = tmp2_df.columns
    select_list = ['tmp2.' + c for c in (set(ing_col_li) - set(ele_li))]
    from_clause = f" tmp2"
    for ind, rec_col in enumerate(ele_cls_di):
        r = ele_cls_di[rec_col]
        alias = "rpp" + str(ind + 1)
        from_clause += f" left join recoded_payer_plan as {alias} on nvl(tmp2.{rec_col},'def@ult') = nvl({alias}.name,'def@ult')"
        sel_item = f"case when {obj_nm} = 0 then tmp2.{rec_col} " \
                   f"when tmp2.{rec_col} is null or trim(tmp2.{rec_col})='' then tmp2.{rec_col} " \
                   f"when {alias}.{r} is null then replace(pp_def_{r}, '<x>', {alias}.id) " \
                   f"else replace({alias}.{r},'<x>',{alias}.id) end as {rec_col}"
        select_list.append(sel_item)

    new_sql = f"select {', '.join(select_list)} " \
              f"from {from_clause} "
    print(new_sql)
    new_df = sqlContext.sql(new_sql)
    new_df = new_df.drop(*["pp_def_plan_name", "pp_def_organization_name", "pp_def_admin_name"])
    write_parquet(new_df, remove_slash(tgt_path_full) + "_tmp/")
    move_s3_folder_spark(sqlContext, remove_slash(tgt_path_full) + "_tmp/", tgt_path_full)
    print("recoding completed successfully")
    return tgt_path_full


def read_deid_mapping(mapping_file, **kwargs):
    """
    This function reads deid mapping file and additional filters.
    Filters are passed as key-val args where key is one among mapping headers and val is value underneath it
    Multiple conditions can be passed while calling function; function iterates through each condition nd applies it.
    :param mapping_file:
    :param kwargs:
    :return:
    """
    buc, key = s3_path_to_bucket_key(mapping_file)
    mapping_content = get_s3_object(buc, key)
    mapping_pdf = pd.read_csv(BytesIO(mapping_content), dtype=str).fillna('')
    mapping_pdf["src_name"] = mapping_pdf["src_name"].str.split(',')
    mapping_pdf = mapping_pdf.explode('src_name')
    mapping_pdf = mapping_pdf[mapping_pdf.is_active_map.str.strip() == '1']
    mapping_pdf = mapping_pdf.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    query_cond = []
    for k, v in kwargs.items():
        if v:
            query_cond.append(f"{k} == '{v}'")
    query_cond = ' & '.join(query_cond)
    # print(f"\n{current_datetime()} :: read_deid_mapping :: mapping filter condition is \n{query_cond}")
    mapping_pdf = mapping_pdf.query(query_cond)
    mapping_pdf["rule_order"] = mapping_pdf["rule_order"].replace('', '0').astype(str).astype(int)
    mapping_pdf["rule_set"] = mapping_pdf["rule_set"].replace('', '9999').astype(str).astype(int)
    return mapping_pdf


def get_class_sqltx_dict():
    """
    This function reads complete deid mapping for rule_type=class and
    returns class name and its derivation(sql_tx) in dictionary format.
    :return:
    """
    map_df = read_deid_mapping(mask_mapping_file, rule_type='class')
    map_df["src_name"] = map_df["src_name"].str.split(',')
    map_df = map_df.explode('src_name')
    if load_type == "full":
        join_tp = {"active": " ",
                   "date_based": " and <alias:$1>.add_date between <alias:$2>.add_date and <alias:$2>.delete_date",
                   "latest_record": " "
                   }
        new_join_tp_li = [tp for tp in pd.unique(map_df["full_load_join_type"]) if tp.strip() != ''
                          and tp.strip().lower() not in join_tp]
        if new_join_tp_li:
            raise Exception(f"get_class_sqltx_dict - these join types not handled - {new_join_tp_li}")

        for k, v in join_tp.items():
            map_df["full_load_join_type"] = map_df["full_load_join_type"].replace(k, v)

        map_df["sql_tx"] = map_df["sql_tx"] + " " + map_df["full_load_join_type"]

    di = dict(zip(map_df.src_name + "_class:" + map_df.type_object, map_df.sql_tx))
    di["_"] = ''
    return di


def get_class_lookup_dict():
    """
    This function reads complete deid mapping for rule_type=class and
    returns class name and lookup arguments in dictionary format.
    :return:
    """
    map_df = read_deid_mapping(mask_mapping_file, rule_type='class')
    map_df["src_name"] = map_df["src_name"].str.split(',')
    map_df = map_df.explode('src_name')
    di = dict(zip(map_df["src_name"] + "_class:" + map_df["type_object"], map_df["lookup_arg"]))
    di["_"] = ''
    return di


def get_class_add_lookup_dict():
    """
    This function reads complete deid mapping for rule_type=class and
    returns class name and additional lookup arguments in dictionary format.
    :return:
    """
    map_df = read_deid_mapping(mask_mapping_file, rule_type='class')
    map_df["src_name"] = map_df["src_name"].str.split(',')
    map_df = map_df.explode('src_name')
    di = dict(zip(map_df["src_name"] + "_class:" + map_df["type_object"], map_df["lookup_arg_addl"]))
    di["_"] = ''
    return di


def get_class_full_load_typ_dict():
    """
    This function reads complete deid mapping for rule_type=class and
    returns class name and full load join type in dictionary format.
    :return:
    """
    map_df = read_deid_mapping(mask_mapping_file, rule_type='class')
    map_df["src_name"] = map_df["src_name"].str.split(',')
    map_df = map_df.explode('src_name')
    di = dict(zip(map_df["src_name"] + "_class:" + map_df["type_object"], map_df["full_load_join_type"]))
    di["_"] = ''
    return di


def get_processed_mapping(df, table_name):
    """
    This function reads deid mapping for specific table, rule order & rule set,
    substitutes parameters with actual values, applies formatting
    and returns processed mapping
    :param df:
    :return:
    """
    # DG: 12 Aug 21: Added following to handle upper case column names
    df['rule_type'] = df['rule_type'].str.strip().str.lower()
    df['layer'] = df['layer'].str.strip().str.lower()
    df['src_name'] = df['src_name'].str.strip().str.lower()
    df['object_name'] = df['object_name'].str.strip().str.lower()

    # get class to sql_tx and class to lookup args mapping
    class_sqltx_di = get_class_sqltx_dict()
    class_lookup_di = get_class_lookup_dict()
    class_add_lookup_di = get_class_add_lookup_dict()
    class_full_load_typ_di = get_class_full_load_typ_dict()

    # cleansing parameters defined in raw mapping
    # primary alias is replaced with table name, lookup alias is replaced with list of lookup tables
    # parameters are formatted as dictionary for easy processing in next level
    # updated enumerated values
    def format_params(x):
        if x.strip() == '':
            return 'element_name:default;alias:primary_alias,lookup_alias'
        elif 'element_name' in x and 'alias' not in x:
            return f"{x};alias:primary_alias,lookup_alias"
        elif 'element_name' not in x and 'alias' in x:
            return f"element_name:default;{x}"
        else:
            return x

    df['parameters'] = df['parameters'].apply(lambda x: format_params(x))

    df['parameters'] = df.apply(lambda x: x['parameters'].
                                replace('lookup_alias', x['lookup_alias']).
                                replace("element_name:default", f"element_name:{x['object_name']}").
                                replace("primary_alias", table_name).split(";"),
                                axis=1)
    df['parameters'] = df.apply(
        lambda x: dict([(f'<{item.split(":")[0]}:$i>', item.split(":")[1].split(",")) for item in x['parameters'] if
                        item.strip() != '']),
        axis=1)
    for i in df.index:
        param = df['parameters'][i]
        new_param = {}
        for m, n_li in param.items():
            for ind, n in enumerate(n_li):
                new_param[m.replace('$i', f'${ind + 1}')] = n
        df['parameters'][i] = new_param

    # cleansing sql transformations / join conditions in sql_tx
    # enumerate through parameters and update sql_tx expressions
    df["sql_tx"] = df.apply(lambda x: x["sql_tx"] if x["sql_tx"] != '' else (
        class_sqltx_di[f'{x["src_name"]}_{x["rule"]}'] if f'{x["src_name"]}_{x["rule"]}' in class_sqltx_di else
        class_sqltx_di[f'_{x["rule"]}']),
                            axis=1)
    for i in df.index:
        param = df['parameters'][i]
        sql_tx = df["sql_tx"][i]
        for k, v in param.items():
            sql_tx = sql_tx.replace(k, v)
        df["sql_tx"][i] = sql_tx

    # cleansing lookup arguments and lookup arguments are formatted as dictionary
    # for each key in lookup argument, create additional column in mapping with associated values
    df["lookup_arg"] = df.apply(lambda x: x["lookup_arg"].split(";") if x["lookup_arg"] != '' else (
        class_lookup_di[f'{x["src_name"]}_{x["rule"]}'] if f'{x["src_name"]}_{x["rule"]}' in class_lookup_di else
        class_lookup_di[f'_{x["rule"]}']).split(";"),
                                axis=1)
    df['lookup_arg'] = df.apply(
        lambda x: dict([(item.split(":")[0], item.split(":")[1]) for item in x['lookup_arg'] if item.strip() != '']),
        axis=1)

    df["lookup_arg_addl"] = df.apply(lambda x: x["lookup_arg_addl"].split(";") if x["lookup_arg_addl"] != '' else (
        class_add_lookup_di[
            f'{x["src_name"]}_{x["rule"]}'] if f'{x["src_name"]}_{x["rule"]}' in class_add_lookup_di else
        class_add_lookup_di[f'_{x["rule"]}']).split(";"),
                                     axis=1)

    df['lookup_arg_addl'] = df['lookup_arg_addl'].apply(
        lambda x: dict([(item.split(":")[0], item.split(":")[1]) for item in x if item.strip() != '']))

    for i in df['lookup_arg'].index:
        lkp_args = df['lookup_arg'][i]
        if str(lkp_args).strip() != '':
            for k, v in lkp_args.items():
                if k not in df:
                    df[k] = ''
                df[k][i] = v

    # full load join types defined in full_load_join_type
    df["full_load_join_type"] = df.apply(lambda x: x["full_load_join_type"] if x["full_load_join_type"] != '' else (
        class_full_load_typ_di[
            f'{x["src_name"]}_{x["rule"]}'] if f'{x["src_name"]}_{x["rule"]}' in class_full_load_typ_di else
        class_full_load_typ_di[f'_{x["rule"]}']),
                                         axis=1)

    return df


def get_from_clause(df, table_name):
    """
    This function reads cleansed mapping, filters for rule_type=lookup
    and generates from clause with appropriate joins
    :param df:
    :return:
    """
    df = df[df.rule_type.str.lower().str.strip() == "lookup"]
    from_clause = table_name
    for i in df.index:
        lkp_nm = df['lookup'][i].strip()
        lkp_alias_nm = df['lookup_alias'][i].strip()
        lkp_tp = df['lookup_type'][i].strip().lower() if "lookup_type" in df else None
        join_cond = df['sql_tx'][i].strip()
        if lkp_nm != '':
            if lkp_alias_nm == '':
                raise Exception(f"lookup alias for {lkp_nm} is {lkp_alias_nm}")
            from_clause += f" \nleft join \n\t{lkp_alias_nm} \non \n\t{join_cond}"
    return from_clause


def get_select_clause(df, base_cols, masked_cols, table_name):
    """
    This function reads cleansed mapping and list of columns on which masking is already applied.
    All ingestion columns and if any new columns created as part of masking are considered as universal set.
    Iterate through each column, if masking is applicable, pick sql_tx else retain column as is.
    :param df:
    :param masked_cols:
    :return:
    """
    df = df[df.rule_type.str.lower().str.strip() == "element"]
    map_dict = dict(zip(df.object_name, df.sql_tx))
    # ing_cols = [c.strip().lower() for c in pd.unique(ing_mapping_df.cdm_column) if c.strip() != '']
    ing_cols = [c.strip().lower() for c in base_cols if c.strip() != '']
    deid_cols = [c.strip().lower() for c in pd.unique(df.object_name)]
    masked_cols = [c.strip().lower() for c in masked_cols]
    print(masked_cols)
    all_cols = set(ing_cols + deid_cols + masked_cols + fin_audit_cols)
    select_list = []
    for c in all_cols:
        if c in map_dict:
            select_list.append(f"{map_dict[c]} as {c}")
        else:
            select_list.append(f"{table_name}.{c}")
    return ',\n\t'.join(select_list)


def build_deid_sql(deid_map_upd, base_cols, table_name):
    """
    In this function,
        1. Iterate through each rule order and rule set
        1.1. If recoding is applicable call custom function
        1.2. If recoding is not applicable generate sql and capture in dictionary
        2. Return sql dictionary
    :param deid_map_upd:
    :return:
    """
    ro_rs_li = get_ruleorder_ruleset(deid_map_upd)
    print(ro_rs_li)
    masked_cols = []
    sql_li = {}
    # iterate through each rule in rules order and rule set list
    for ro, rs_li in ro_rs_li.items():
        sql_li[ro] = {}
        print(f"\n{current_datetime()} :: build_deid_sql :: start :: rule order {ro}")
        for rs in rs_li:
            print(f"\n{current_datetime()} :: build_deid_sql :: start :: rule set {rs}")
            # get current mapping
            curr_mapping = deid_map_upd[(deid_map_upd.rule_order == ro) & (deid_map_upd.rule_set == rs)]
            recode_mapping = curr_mapping[(curr_mapping['custom'].str.strip().str.upper() == 'Y') &
                                          (curr_mapping.rule_type.str.strip().str.lower() != "lookup")]
            if len(recode_mapping.index) > 1:
                raise Exception(f"multiple levels of recoding not enabled in current version")
            if not recode_mapping.empty:
                rule = recode_mapping['rule'].unique()[0].strip().lower().replace("class:", '')
                columns = recode_mapping['object_name'].unique().tolist()
                print(f"custom function already completed for - {rule} & columns are - {columns}")
            else:
                columns = [c for c in curr_mapping['object_name'].unique() if c != '']
                from_clause = get_from_clause(curr_mapping, table_name)
                select_clause = get_select_clause(curr_mapping, base_cols, masked_cols, table_name)
                sql = f"select \n\t{select_clause} \nfrom \n\t{from_clause}"
                sql_li[ro][rs] = sql
            masked_cols = list(set(masked_cols) | set(columns))
            print(f"\n{current_datetime()} :: build_deid_sql :: end :: rule set {rs}")
        print(f"\n{current_datetime()} :: build_deid_sql :: end :: rule order {ro}")
    return sql_li


def sort_sql_dict(sql_dict):
    new_di = {}
    print(sorted(list(sql_dict.keys())))
    for k1 in sorted(list(sql_dict.keys())):
        sub_di = sql_dict[k1]
        new_di[k1] = {}
        for k2 in sorted(list(sub_di.keys())):
            new_di[k1][k2] = sub_di[k2]
    return new_di


def transform_and_load(src_di, sql_di, table_name, map_df):
    for s_nm, s_df in src_di.items():
        if s_nm != table_name:
            s_df.persist()
        print(f"{current_datetime()} ::transform_and_load : count - {s_nm} : {s_df.count()}")
        s_df.createOrReplaceTempView(s_nm)

    # print(sql_di)
    new_sql_di = sort_sql_dict(sql_di)
    # print(new_sql_di)

    prev_inst_vw = table_name
    new_sql_components = []
    for ro, rs_sql in new_sql_di.items():
        for rs, sql in rs_sql.items():
            inst = f"{ro}_{rs}"
            inst_sql = sql.replace(f"from \n\t{table_name}", f"from \n\t{prev_inst_vw}").replace(table_name + '.',
                                                                                                 prev_inst_vw + '.')
            inst_vw = table_name + "_" + inst
            new_sql_components.append(f" {inst_vw} as ({inst_sql}) ")
            prev_inst_vw = inst_vw

    tgt_typ = get_target_type(ing_mapping_df)
    if len(new_sql_components) != 0:
        if "dim" in tgt_typ:
            fin_sql = "with " + ', \n'.join(new_sql_components) + f" \nselect distinct * from {prev_inst_vw} "
        else:
            fin_sql = "with " + ', \n'.join(new_sql_components) + f" \nselect * from {prev_inst_vw} "
        print(fin_sql)
        fin_out_df = sqlContext.sql(fin_sql)
        fin_sel_order = [_ for _ in v_select_order]
        for _ in fin_out_df.columns:
            if _ not in v_select_order and _ not in fin_audit_cols:
                fin_sel_order.append(_)
        fin_sel_order += fin_audit_cols
        fin_out_df = fin_out_df.select(*fin_sel_order)

        # fin_out_df = fin_out_df.persist()
        print(f"{current_datetime()} ::transform_and_load : count - final : {fin_out_df.count()}")

        # write_parquet(fin_out_df, tgt_path_full)
        if is_last_rule_order(map_df):
            print(f"{current_datetime()} :: transform_and_load : this is last rule order for {table_name}")
            fin_out_df = standardize_columns(fin_out_df)

        if is_first_rule_order(map_df):
            print(f"{current_datetime()} :: transform_and_load : this is first rule order for {table_name}; "
                  f"writing into {tgt_path_full}")
            write_parquet_partition_by_col(fin_out_df, part_keys, tgt_path_full)
            return None
        else:
            print(f"{current_datetime()} :: transform_and_load : this is not first rule order for {table_name}; "
                  f"writing into {tmp_path_full + table_name}")
            write_parquet(fin_out_df, tmp_path_full + table_name)
            return tmp_path_full + add_slash(table_name)
    else:
        print(f"{current_datetime()} :: transform_and_load : no sql to execute")
        return None


def standardize_columns(spark_df):
    if "standard_name" in ing_mapping_df.columns:
        print(f"{current_datetime()} :: transform_and_load : running column name standardization")
        column_dict_df = ing_mapping_df[ing_mapping_df.cdm_column.str.strip() != '']
        # column_dict_df.loc[column_dict_df['standard_name'] == '', 'standard_name'] = column_dict_df['cdm_column']
        column_dict = dict(zip(ing_mapping_df.cdm_column, ing_mapping_df.standard_name))
        print(f"{current_datetime()} :: transform_and_load : existing to new column name mapping is \n"
              f"{json.dumps(column_dict, indent=2)}")
        for _c, _s in column_dict.items():
            if _s.strip() == "":
                _s = _c
            spark_df = spark_df.withColumnRenamed(_c, _s)
        # select_expr = ', '.join([f'"{x} as {y}"' for x, y in column_dict.items()])
        # spark_df = spark_df.selectExpr(select_expr)
    else:
        print(f"{current_datetime()} :: transform_and_load : column name standardization not defined")
    return spark_df


def deid_ro_wise_call(bucket, config_file, tgt_name, layer, load_type, table_name, rule_order=None):
    print(f"\n\n****** START - {table_name} ******")
    try:
        if load_type not in ["full", "incremental", "active"]:
            raise Exception(f"deid_ro_wise_call - incorrect load type {load_type}")

        print(f"{current_datetime()} :: deid_ro_wise_call :: step 1 - define params")
        define_params(bucket, config_file, tgt_name, table_name, layer)
        get_src_path(table_name)
        print(f"{current_datetime()} :: deid_ro_wise_call :: step 2 - start - {layer}")
        _map_df = read_deid_mapping(mask_mapping_file, src_name=tgt_name, type_object=table_name,
                                    layer=layer, rule_order=rule_order)
        if _map_df.empty and (rule_order is None or rule_order == 0):
            print(
                f"\n{current_datetime()} :: deid_ro_wise_call :: mapping not available for src_name - {tgt_name}, type_object - {table_name}")
            # read source data into dataframe
            if load_type == "active":
                print(f"\n{current_datetime()} :: read active records from {fin_src_path} ")
                df = read_active_records_from_parquet(sqlContext, fin_src_path)
            elif load_type == "incremental":
                print(f"\n{current_datetime()} :: read incremental records from {fin_src_path} ")
                df = read_incremental_records_from_parquet(sqlContext, fin_src_path)
            elif load_type == "full":
                print(f"\n{current_datetime()} :: read all records from {fin_src_path} ")
                df = read_parquet(sqlContext, fin_src_path)
            else:
                raise Exception(f"invalid load type - {load_type}")
            print(f"\n{current_datetime()} :: write into {tgt_path_full}")
            df = standardize_columns(df)
            # df.write.parquet(tgt_path_full, "Overwrite", compression='snappy')
            write_parquet_partition_by_col(df, part_keys, tgt_path_full)
        elif _map_df.empty and rule_order is not None and rule_order == 0:
            raise Exception(f"deid_ro_wise_call :: mapping not available for src_name - {tgt_name}, "
                            f"type_object - {table_name} and rule_order={rule_order}")
        else:
            print(
                f"\n{current_datetime()} :: deid_ro_wise_call :: step 2.1 - process mapping to derive lookup and rules info")
            deid_map_upd = get_processed_mapping(_map_df, table_name)
            print("processed mapping", deid_map_upd.head(10))
            print(f"\n{current_datetime()} :: deid_ro_wise_call :: step 2.2 - read source data")
            src_df_dict = read_src_data(deid_map_upd, table_name)
            print(f"\n{current_datetime()} :: deid_ro_wise_call :: step 2.3 - build sql to apply transformation")
            sqls = build_deid_sql(deid_map_upd, src_df_dict[table_name].columns, table_name)
            print(json.dumps(sqls, indent=4))
            print(f"\n{current_datetime()} :: deid_ro_wise_call :: step 2.4 - apply transformation and load")
            if len(sqls) != 0:
                temp_path = transform_and_load(src_df_dict, sqls, table_name, filtered_df)
                if temp_path:
                    print(f"\n{current_datetime()} :: deid_ro_wise_call :: step 2.5 - move from temp to target")
                    move_s3_folder_spark_with_partition(sqlContext, temp_path, tgt_path_full, part_keys)
                else:
                    print(
                        f"\n{current_datetime()} :: deid_ro_wise_call :: step 2.5 - nothing to move. temp path empty.")
            else:
                print(
                    f"\n{current_datetime()} :: deid_ro_wise_call ::no sqls to execute. Nothing to write further.")
        print(f"{current_datetime()} :: deid_ro_wise_call :: step 2 - end - {layer}")

    except Exception as e:
        print("ERROR DETAILS - ", e)
        print(traceback.format_exc())
        raise e
    print(f"\n\n****** END - {table_name} ******")


def define_common_params(bucket, config_file, tgt_name):
    print(
        f"\n{current_datetime()} :: define_common_params :: info - reading the config file {config_file} in bucket {bucket} ...\n")
    try:
        global root_path, master_path_full, mask_mapping_file, ing_mapping_df, audit_cols, param_contents, batch_date, version, ing_mapping_file, input_dataset, tmp_path_full, \
            cai_path_full, src0_path_full, src1_path_full, src2_path_full, src3_path_full, tgt_version_path_full, tgt_root_path_full

        param_data = get_s3_object(bucket, config_file)
        param_contents = json.loads(param_data)
        input_dataset = param_contents["input_dataset"]
        ing_mapping_file = param_contents["ing_mapping"]
        mask_mapping_file = param_contents["deid_mapping"]
        batch_date = param_contents["batch_date"]

        if tgt_name in param_contents["src_dataset"]:
            version = param_contents["src_dataset"][tgt_name]["version"]
            version = batch_date if version == "" else version
        else:
            version = batch_date
        audit_cols = param_contents["audit_columns"]
        root_path = param_contents["root_path"]
        master_base_dir = param_contents["master_base_dir"]
        master_path = root_path + add_slash(master_base_dir)
        master_path_full = bucket_key_to_s3_path(bucket, master_path)

        cai_base_dir = param_contents["cai_base_dir"]
        cai_path = root_path + add_slash(cai_base_dir)
        cai_path_full = bucket_key_to_s3_path(bucket, cai_path)

        if layer == "de-identification":
            src0_base_dir = param_contents["ing_base_dir"]
            src1_base_dir = param_contents["ing_w_cai_base_dir"]
            src2_base_dir = param_contents["cleansing_base_dir"]
            src3_base_dir = param_contents["masked_base_dir"]
            tgt_base_dir = param_contents["masked_base_dir"]
            tmp_base_dir = tgt_base_dir + "_temp"
            tmp_path = root_path + add_slash(tmp_base_dir) + add_slash(tgt_name) + add_slash(version)
            tmp_path_full = bucket_key_to_s3_path(bucket, tmp_path)

            src0_path = root_path + add_slash(src0_base_dir)
            src0_path_full = bucket_key_to_s3_path(bucket, src0_path)
            src1_path = root_path + add_slash(src1_base_dir)
            src1_path_full = bucket_key_to_s3_path(bucket, src1_path)
            src2_path = root_path + add_slash(src2_base_dir)
            src2_path_full = bucket_key_to_s3_path(bucket, src2_path)
            src3_path = root_path + add_slash(src3_base_dir)
            src3_path_full = bucket_key_to_s3_path(bucket, src3_path)
            tgt_root_path = root_path + add_slash(tgt_base_dir)
            tgt_root_path_full = bucket_key_to_s3_path(bucket, tgt_root_path)
            tgt_version_path = root_path + add_slash(tgt_base_dir) + add_slash(tgt_name) + add_slash(version)
            tgt_version_path_full = bucket_key_to_s3_path(bucket, tgt_version_path)

        elif layer == "cleansing":
            src0_base_dir = param_contents["ing_base_dir"]
            src1_base_dir = param_contents["ing_w_cai_base_dir"]
            src3_base_dir = param_contents["cleansing_base_dir"]
            tgt_base_dir = param_contents["cleansing_base_dir"]
            tmp_base_dir = tgt_base_dir + "_temp"
            tmp_path = root_path + add_slash(tmp_base_dir) + add_slash(tgt_name) + add_slash(version)
            tmp_path_full = bucket_key_to_s3_path(bucket, tmp_path)

            src0_path = root_path + add_slash(src0_base_dir)
            src0_path_full = bucket_key_to_s3_path(bucket, src0_path)
            src1_path = root_path + add_slash(src1_base_dir)
            src1_path_full = bucket_key_to_s3_path(bucket, src1_path)
            src2_path_full = ''
            src3_path = root_path + add_slash(src3_base_dir)
            src3_path_full = bucket_key_to_s3_path(bucket, src3_path)
            # src3_path_full = ''
            tgt_root_path = root_path + add_slash(tgt_base_dir)
            tgt_root_path_full = bucket_key_to_s3_path(bucket, tgt_root_path)
            tgt_version_path = root_path + add_slash(tgt_base_dir) + add_slash(tgt_name) + add_slash(version)
            tgt_version_path_full = bucket_key_to_s3_path(bucket, tgt_version_path)

    except Exception as err:
        print(
            f"\n{current_datetime()} :: define_common_params :: error - failed to read the config file {config_file} in bucket {bucket}")
        print("error details : ", err)
        raise err
    else:
        print(
            f"\n{current_datetime()} :: define_common_params :: info - successfully read the config file {config_file} in bucket {bucket}\n")
        print(
            f"\n{current_datetime()} :: define_common_params :: info - deid mapping_file    : {mask_mapping_file}")
        print(f"\n{current_datetime()} :: define_common_params :: info - input_dataset        : {input_dataset}")
        print(f"\n{current_datetime()} :: define_common_params :: info - source_path 1        : {src1_path_full}")
        print(f"\n{current_datetime()} :: define_common_params :: info - source_path 2        : {src2_path_full}")
        print(f"\n{current_datetime()} :: define_common_params :: info - source_path 3        : {src3_path_full}")
        print(
            f"\n{current_datetime()} :: define_common_params :: info - tgt_version_path_full       : {tgt_version_path_full}")
    print("*" * format_length)


def cleanup_tgt_base_path(table):
    if s3_path_exists(tgt_version_path_full + add_slash(table)):
        print(
            f"\n{current_datetime()} :: cleanup_tgt_base_path :: cleaning -  : {tgt_version_path_full + add_slash(table)}")
        delete_s3_folder_spark(sqlContext, tgt_version_path_full + add_slash(table))


def check_rule_order_validity(df):
    rule_orders = df.rule_order.unique().tolist()
    print("rule_orders::", rule_orders)
    print("rule_order", rule_order)
    if rule_order not in rule_orders:
        raise Exception(f'Rule order {rule_order} not present for target {tgt_name} and table {table_name}')
    else:
        rule_orders.sort()
        # check if previous rule order present
        if rule_order > rule_orders[0]:
            # check if target already has file
            if not s3_path_exists(tgt_version_path_full):
                raise Exception(
                    f"Rule order {rule_order} for table {table_name} could not be executed as target path does not have previous rule order data")
        else:
            cleanup_tgt_base_path(table_name)


def is_last_rule_order(df):
    rule_orders = df.rule_order.unique().tolist()
    print("rule_orders::", rule_orders)
    print("rule_order", rule_order)
    if rule_order not in rule_orders:
        raise Exception(f'Rule order {rule_order} not present for target {tgt_name} and table {table_name}')
    else:
        rule_orders.sort()
        # check if previous rule order present
        if rule_order == rule_orders[-1]:
            return True
        else:
            return False


def is_first_rule_order(df):
    rule_orders = df.rule_order.unique().tolist()
    print("rule_orders::", rule_orders)
    print("rule_order", rule_order)
    if rule_order not in rule_orders:
        raise Exception(f'Rule order {rule_order} not present for target {tgt_name} and table {table_name}')
    else:
        rule_orders.sort()
        # check if previous rule order present
        if rule_order == rule_orders[0]:
            return True
        else:
            return False


print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
args = getResolvedOptions(sys.argv,
                          ['S3_BUCKET', 'CONFIG_FILE', 'SPARK_PROPERTIES', 'TGT_NAME', 'LAYER', 'TABLE_NAME',
                           'LOAD_TYPE', 'JOB_NAME', 'RULE_ORDER', 'PARTITION_KEYS'])

try:
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    table_name = args['TABLE_NAME'].lower().strip()
    tgt_name = args['TGT_NAME'].lower().strip()
    spark_properties = args['SPARK_PROPERTIES']
    layer = args['LAYER'].lower().strip()
    load_type = args['LOAD_TYPE'].lower().strip()
    job_name = args['JOB_NAME']
    job_run_id = args['JOB_RUN_ID']
    rule_order = int(args['RULE_ORDER'])
    part_keys = args['PARTITION_KEYS'].strip().lower()
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
    print(f"{current_datetime()} :: main :: info - layer            : {layer}")
    print(f"{current_datetime()} :: main :: info - load_type        : {load_type}")
    print(f"{current_datetime()} :: main :: info - partition keys   : {part_keys}")
    print(f"{current_datetime()} :: main :: info - job_name         : {job_name}")
    print(f"{current_datetime()} :: main :: info - job_run_id       : {job_run_id}")
print("*" * format_length)
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

print(
    f"{current_datetime()} :: main :: step 0 - Initialize Spark Context with the defined properties")
sc, sqlContext = initialize_spark_context(spark_properties)
print(f"Spark set properties :: {sc.getConf().getAll()}")

print(f"register user defined functions")
sqlContext.udf.register("cleanseDateUDF", cleanseDate)
sqlContext.udf.register("valStrListUDF", valStrList)

print(f"{current_datetime()} :: main :: step 1 : Initializing common params")
define_common_params(bucket, config_file, tgt_name)
print(f"{current_datetime()} :: main :: step 2 : Check and delete previous files from target")
filtered_df = read_deid_mapping(mask_mapping_file, src_name=tgt_name,
                                layer=layer, type_object=table_name)
if filtered_df.empty and rule_order != 0 and rule_order is not None:
    raise Exception(f"deid_ro_wise_call :: mapping not available for src_name - {tgt_name}, "
                    f"type_object - {table_name} and rule_order={rule_order}")

if not filtered_df.empty:
    check_rule_order_validity(filtered_df)
    deid_ro_wise_call(bucket, config_file, tgt_name, layer, load_type, table_name, rule_order)
else:
    cleanup_tgt_base_path(table_name)
    deid_ro_wise_call(bucket, config_file, tgt_name, layer, load_type, table_name)
