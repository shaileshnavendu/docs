import re

import pandas as pd
import pandasql as ps
from s3_utils import *
from io import BytesIO
from pyspark.sql.functions import lit, udf, col, row_number, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.types import *
import ast
from datetime import datetime, timedelta
import time
import json
import uuid
import math

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 2000000)
pd.set_option('display.max_colwidth', 10000)
pd.options.mode.chained_assignment = None


def get_src_avlblt(file_path, src_nm):
    buc, key = s3_path_to_bucket_key(file_path)
    mapping_content = get_s3_object(buc, key)
    mapping_pdf = pd.read_csv(BytesIO(mapping_content), dtype=str).fillna("")
    mapping_pdf = mapping_pdf if src_nm is None else mapping_pdf[
        mapping_pdf.src.str.lower().str.strip() == src_nm]
    return_list = mapping_pdf.is_file_available.unique()
    if len(return_list) != 1:
        raise Exception(f"Unique value not present for is_file_avlbl for src - {src_nm}")
    else:
        return return_list[0]


def read_mapping(file_name, target_table=None, dataset=None, arn=None):
    """
    This function reads mapping file in csv format
    and returns contents with pandas dataframe.
    :param arn:
    :param file_name: This is mapping file name with full path.
    :param target_table: If target table name is not passed, no filter will be applied.
    :param dataset: If cdm_name name is not passed, no filter will be applied.
    :return: pandas dataframe for given cdm_name and table.
    """
    buc, key = s3_path_to_bucket_key(file_name)
    mapping_content = get_s3_object(buc, key, arn)
    mapping_pdf = pd.read_csv(BytesIO(mapping_content), dtype=str).fillna("")
    mapping_pdf = mapping_pdf[mapping_pdf.is_active_map.str.strip() == "1"]
    mapping_pdf = mapping_pdf if target_table is None else mapping_pdf[
        mapping_pdf.cdm_table.str.lower().str.strip() == target_table.lower()]
    mapping_pdf = mapping_pdf if dataset is None else mapping_pdf[
        mapping_pdf.cdm_name.str.lower().str.strip() == dataset]
    mapping_pdf["rule_order"] = mapping_pdf["rule_order"].replace('', '9999').astype(str).astype(int)
    mapping_pdf["rule_set"] = mapping_pdf["rule_set"].replace('', '9999').astype(str).astype(int)
    return mapping_pdf


def read_parquet(sqlContext, filename, schema=None):
    """
    This function reads parquet file
    and returns contents with spark dataframe.
    :param sqlContext: SQL context.
    :param filename: Name of the file along with path.
    :param schema: If schema is passed, dataframe will be created with given schema.
    :return: If parquet file read successfully, return dataframe, otherwise None
    """
    try:
        if isinstance(filename, list):
            fin_file_li = []
            for f in filename:
                if s3_path_exists(f):
                    fin_file_li.append(f)
            filename = fin_file_li

        df = sqlContext.read.format('parquet') \
            .option("schema", schema) \
            .load(filename)
    except Exception as err:
        print(err)
        return None
    else:
        return df


def read_csv(sqlContext, filename, schema=None, delimiter=",", quote="", headers_flg=True):
    """
    This function reads delimited file,
    and returns contents with spark dataframe.
    :param sqlContext: SQL context.
    :param filename: Name of the file along with path.
    :param schema: If schema is passed, dataframe will be created with given schema.
    :param delimiter: This is column separator in file; default is ",".
    :param quote: Quote escape character
    :return: If parquet file read successfully, return dataframe, otherwise None
    """
    try:
        df = sqlContext.read.format('csv') \
            .option("encoding", "utf-8") \
            .option("header", headers_flg) \
            .option("schema", schema) \
            .option("delimiter", delimiter) \
            .option("quote", quote) \
            .option("inferSchema", False).load(filename)
    except Exception as err:
        print(err)
        return None
    else:
        return df


def get_prev_date(batch_date):
    prev_dt = datetime.strptime(batch_date, '%Y%m%d') - timedelta(days=1)
    return datetime.strftime(prev_dt, '%Y%m%d')


def read_active_records_from_parquet(sqlContext, filename, schema=None):
    """
    This function reads parquet file, filters for active records
    and returns contents with spark dataframe.
    :param sqlContext: SQL context.
    :param filename: Name of the file along with path.
    :param schema: If schema is passed, dataframe will be created with given schema.
    :return: If parquet file read successfully, return dataframe, otherwise None
    """
    try:
        if s3_path_exists(add_slash(filename) + "is_active=1/"):
            print(f'reading - {add_slash(filename) + "is_active=1/"}')
            df = sqlContext.read.format('parquet') \
                .option("schema", schema) \
                .load(add_slash(filename) + "is_active=1/")
            df = df.withColumn("is_active", lit(1))
        else:
            print(f'reading - {add_slash(filename)} and filtering for is_active=1')
            df = sqlContext.read.format('parquet') \
                .option("schema", schema) \
                .load(filename).filter(col("is_active") == 1)
    except Exception as err:
        print(err)
    else:
        return df


def read_incremental_records_from_parquet(sqlContext, filename, schema=None):
    """
    This function reads parquet file, filters for incremental records
    and returns contents with spark dataframe.
    :param sqlContext: SQL context.
    :param filename: Name of the file along with path.
    :param schema: If schema is passed, dataframe will be created with given schema.
    :return: If parquet file read successfully, return dataframe, otherwise None
    """
    try:
        df = sqlContext.read.format('parquet') \
            .option("schema", schema) \
            .load(filename).filter(col("is_incremental") == 1)
    except Exception as err:
        print(err)
    else:
        return df


def write_parquet(df, tgt_path, temp_path=None):
    """
    This function reads data from spark dataframe and
    loads into parquet file on s3 path.
    :param df: Spark dataframe
    :param tgt_path: Target S3 path
    :param temp_path: Temporary S3 path
    :return:
    """
    try:
        if temp_path is None:
            # load dataframe into target path
            print(f"write_parquet :: info - writing parquet into {tgt_path}\n")
            df.write.parquet(tgt_path, "Overwrite", compression='snappy')

        else:
            bucket, tgt_key = s3_path_to_bucket_key(tgt_path)
            bucket, temp_key = s3_path_to_bucket_key(temp_path)
            # Write parquet files into temp location
            print(f"write_parquet :: info - start - writing into {temp_path}\n")
            df.write.parquet(temp_path, "Overwrite", compression='snappy')
            print(f"write_parquet :: info - end - writing into {temp_path}\n")

            # Move parquet files to stage folder
            time.sleep(30)
            print(f"write_parquet :: info - delete existing files {tgt_key}\n")
            delete_s3_folder(bucket, tgt_key)
            print(f"write_parquet :: info - move new files from temp path {temp_key} to final target path {tgt_key}.\n")
            move_s3_folder(bucket, temp_key, tgt_key)

    except Exception as e:
        print("error details : ", e)
        raise e
        # return "FAIL"
    else:
        return "SUCCESS"


def write_parquet_partition_by_col(df, part_col, tgt_path, temp_path=None):
    """
    This function reads data from spark dataframe and
    loads into parquet file on s3 path.
    :param part_col: column name upon which partition should be applied
    :param df: Spark dataframe
    :param tgt_path: Target S3 path
    :param temp_path: Temporary S3 path
    :return:
    """
    try:
        if temp_path is None:
            # load dataframe into target path
            print(f"write_parquet :: info - writing parquet into {tgt_path}\n")
            fin_part_keys = []
            for _k in part_col.split(","):
                part_key = _k.strip()
                if part_key in df.columns:
                    fin_part_keys.append(part_key)
            if fin_part_keys:
                print(f"partition columns {fin_part_keys} found in schema, writing with partitioning.")
                df.write.partitionBy(*fin_part_keys).parquet(path=tgt_path, mode="overwrite", compression="snappy")
            else:
                print(f"partition columns {part_col} not found in schema, writing without partitioning.")
                df.write.parquet(path=tgt_path, mode="overwrite", compression="snappy")

        else:
            bucket, tgt_key = s3_path_to_bucket_key(tgt_path)
            bucket, temp_key = s3_path_to_bucket_key(temp_path)
            # Write parquet files into temp location
            print(f"write_parquet :: info - start - writing into {temp_path}\n")
            fin_part_keys = []
            for _k in part_col.split(","):
                part_key = _k.strip()
                if part_key in df.columns:
                    fin_part_keys.append(part_key)
            if fin_part_keys:
                print(f"partition columns {fin_part_keys} found in schema, writing with partitioning.")
                df.write.partitionBy(*fin_part_keys).parquet(path=temp_path, mode="overwrite", compression="snappy")
            else:
                print(f"partition columns {part_col} not found in schema, writing without partitioning.")
                df.write.parquet(path=temp_path, mode="overwrite", compression="snappy")
            print(f"write_parquet :: info - end - writing into {temp_path}\n")

            # Move parquet files to stage folder
            time.sleep(30)
            print(f"write_parquet :: info - delete existing files {tgt_key}\n")
            delete_s3_folder(bucket, tgt_key)
            print(f"write_parquet :: info - move new files from temp path {temp_key} to final target path {tgt_key}.\n")
            move_s3_folder(bucket, temp_key, tgt_key)

    except Exception as e:
        print("error details : ", e)
        raise e
        # return "FAIL"
    else:
        return "SUCCESS"


def write_parquet_with_partition(df, tgt_path, temp_path=None, no_of_partitions=100):
    """
    This function reads data from spark dataframe and
    loads into parquet file on s3 path.
    :param no_of_partitions:
    :param df: Spark dataframe
    :param tgt_path: Target S3 path
    :param temp_path: Temporary S3 path
    :return:
    """
    try:
        df1 = df.repartition(no_of_partitions)
        print("Number of partitions (to write) : ", df1.rdd.getNumPartitions())
        if temp_path is None:
            # load dataframe into target path
            print(f"write_parquet :: info - writing parquet into {tgt_path}\n")
            df1.write.parquet(tgt_path, "Overwrite", compression='snappy')

        else:
            bucket, tgt_key = s3_path_to_bucket_key(tgt_path)
            bucket, temp_key = s3_path_to_bucket_key(temp_path)
            # Write parquet files into temp location
            df1.write.parquet(temp_path, "Overwrite", compression='snappy')
            print(f"write_parquet :: info - end - writing into parquet file in temp path\n")

            # Move parquet files to stage folder
            time.sleep(30)
            print(f"write_parquet :: info - delete existing files {tgt_key}\n")
            delete_s3_folder(bucket, tgt_key)
            print(f"write_parquet :: info - move new files from temp path {temp_key} to final target path {tgt_key}.\n")
            move_s3_folder(bucket, temp_key, tgt_key)

    except Exception as e:
        print("error details : ", e)
        raise e
        # return "FAIL"
    else:
        return "SUCCESS"


def build_src_sql(df):
    """
    This function takes pandas dataframe as input,
    generates sql and returns as output.
    :param df: Pandas dataframe for mapping, for given src_name and table.
    :return: Generated sql
    """
    query_select = """ 
select
    case
        when lower(trim(datatype)) = 'date' then 'to_date('
        when lower(trim(datatype)) = 'timestamp' then 'to_timestamp('
        else 'cast('
    end ||
    case
        when coalesce(trim(sql_tx), '')|| coalesce(trim(src_column), '')= '' then 'null'
        when trim(sql_tx)= '' then '' || trim(src_table)|| '.' || trim(src_column)|| ''
        else trim(sql_tx)
    end ||
    case
        when lower(trim(datatype)) in ('date', 'timestamp') then ','''
        || case
            when format is null
            or trim(format) = '' then 'MM/dd/yyyy HH:mm:ss a'
            else trim(format)
        end || ''''
        when trim(datatype) is null
        or trim(datatype)= ''
        or trim(datatype) like '%varchar%' then ' as varchar'
        || case
            when trim(length) = '' then '(256)'
            when substr(trim(length), 1, 1) = '(' then trim(length)
            when substr(trim(length), 1, 1) = '-' then '(' ||((trim(length))*-1) || ')'
            else '(' || trim(length)|| ')'
        end
        else ' as ' || trim(datatype)|| trim(length)
    end || ') as ' || lower(trim(cdm_column)) as source
from
    df
where
    trim(cdm_column) is not null
    and trim(cdm_column) <> ''
"""

    query_from = "select distinct trim(src_table) as source from df where trim(src_table)<>'' and trim(src_table) is not null " \
                 "and trim(src_table) not in (select trim(src_table) from df where lower(map_type) like '%join%')"
    query_join = "select ' ' || map_type || ' ' || trim(src_table) || ' ON ' || trim(sql_tx) as source from df where lower(map_type) like '%join%'"

    query_where = "select trim(sql_tx) as source from df where lower(trim(map_type))='where' "

    select_list = ps.sqldf(query_select)["source"].tolist()
    select_clause = " , ".join(select_list)
    from_list = ps.sqldf(query_from)["source"].tolist()
    from_clause = " ".join(from_list)
    join_list = ps.sqldf(query_join)["source"].tolist()
    join_clause = " ".join(join_list)
    where_list = ps.sqldf(query_where)["source"].tolist()
    where_clause = '' if where_list == [] else " WHERE " + where_list[0]
    src_query = "SELECT " + select_clause + " FROM " + from_clause + join_clause + where_clause
    return src_query


def union_dataframes(df_array):
    """
    This function reads list of spark dataframes,
    create a combined structure and merge rows from all dataframes.
    :param df_array: List of spark dataframes.
    :return: combined dataframe.
    """
    # Create a list of all the column names
    if len(df_array) < 1:
        raise Exception("Array is empty; union_dataframes function requires minimum one dataframe")

    cols = set()
    for df in df_array:
        for x in df.columns:
            cols.add(x)
    cols = sorted(cols)

    # Create a dictionary with all the dataframes
    dfs = {}
    for i, df in enumerate(df_array):
        # New name for the key, the dataframe is the value
        new_name = 'df' + str(i)
        dfs[new_name] = df
        # Loop through all column names. Add the missing columns to the dataframe (with value 0)
        for x in cols:
            if x not in df.columns:
                dfs[new_name] = dfs[new_name].withColumn(x, lit(None))
        dfs[new_name] = dfs[new_name].select(cols)  # Use 'select' to get the columns sorted

    # Now put it altogether with a loop (union)
    result_df = dfs['df0']  # Take the first dataframe, add the others to it
    dfs.pop('df0')  # Remove the first one, because it is already in the result
    result_df = result_df.distinct()
    for x in dfs:
        result_df = result_df.union(dfs[x]).distinct()
    return result_df


def load_target_entities(mapping_file, cdm_table, new_entity, dataset, arn=None):
    # get target column list for new entity
    ne_map = read_mapping(mapping_file, new_entity, dataset, arn)
    ne_cols_li = [c.strip() for c in pd.unique(ne_map.cdm_column) if c.strip() != '']

    # get mapping for cdm table and filter it for new entity mapping
    cdm_mapping = read_mapping(mapping_file, cdm_table, dataset, arn)
    src_mapping_df = cdm_mapping[
        [(new_entity in x.replace(" ", "").lower().split(",")) for x in cdm_mapping['target_column_entity']]]
    # print(src_mapping_df)
    # capture column lists for accurate mapping.
    src_columns = src_mapping_df["cdm_column"].unique().tolist()
    cdm_columns = cdm_mapping["cdm_column"].unique().tolist()

    if src_mapping_df.empty:
        return None
    else:
        d = {}  # dictionary to hold source to target mapping
        i = set()  # store instance name
        for tc in ne_cols_li:
            if tc in src_columns:  # for source and target column names matching exactly
                d[tc] = tc
                i.add('')
            else:  # for source and target column names not matching exactly
                for sc in src_columns:
                    if tc in sc:  # they are prefixed or suffixed
                        d[sc] = tc
                        i.add(sc.replace(tc, ''))
                    else:  # pattern match : numbered columns
                        pattern = ''.join([c for c in sc if not c.isdigit()])
                        i.add(''.join([c for c in sc if c.isdigit()]))
                        pattern = pattern.replace("__", "_")
                        # print(pattern)
                        if pattern in tc:  # pattern match with mapped columns
                            d[sc] = tc
                        elif tc in cdm_columns:  # exact match outside mapping - specially for config columns
                            d[tc] = tc
                        else:
                            d[tc] = 'cast(null as string)'  # if nothing matches set it null
        # print(d)
        # split dictionary into blocks
        new_d = {}
        for _i, ele in enumerate(i):
            select_col = []
            print(f"load_target_entities : Generating query for block : {_i}")
            for k, v in d.items():
                if ele in k:
                    select_col.append(f"{v} as {k}")
            new_d[ele] = select_col
        print(json.dumps(new_d, indent=4))
        return new_d


def get_target_column_entity_list(mapping_file, cdm_table, tgt_name, arn=None):
    """
    This functions picks unique list from target_column_entity column of mapping
    :param tgt_name: data source name
    :param arn:
    :param mapping_file: Mapping file name with path.
    :param cdm_table: CDM table name for which target_column_entity list should be generated.
    :return: List of target_column_entity
    """
    tbl_list = read_mapping(mapping_file, cdm_table, dataset=tgt_name, arn=arn)[
        'target_column_entity'].to_list()  # Get list of entities
    te_lst = set()  # set to capture the entities; if multiple instances found, single instance will be returned.
    # Iterate through list of entities; if comma separated values found; separate them into different entities.
    for ent in tbl_list:
        ent_arr = ent.split(",")
        for e in ent_arr:
            if e == "":
                continue
            te_lst.add(e.strip())
    return list(te_lst)


def get_target_type(df):
    """
    This function reads mapping dataframe and
    returns target_type
    :param df: mapping dataframe (pandas df)
    :return: target_type
    """
    query_1 = "select distinct lower(trim(target_type)) as target_type from df where trim(target_type)<>'' and target_type is not null"
    target_type = ps.sqldf(query_1)["target_type"].tolist()
    return target_type[0] if target_type != [] else ''


def get_table_prefix(dataset, market=None):
    """
    This reads src_name name and market name as input and
    derives table prefix.
    :param dataset: sphub, claims etc
    :param market: ac, ai, pc etc
    :return: concatenated string
    """
    if dataset is None or dataset == "":
        if market == "" or market is None:
            return ''
        else:
            return market + "/"
    else:
        if market == "" or market is None:
            return dataset + "/"
        else:
            return dataset + "/" + market + "/"


def get_scd_type(df):
    query_1 = "select distinct upper(scd) as scd_indicator from df "
    scd_indicator = ps.sqldf(query_1)["scd_indicator"].tolist()
    query_2 = "select distinct upper(map_type) as map_type from df "
    map_types = ps.sqldf(query_2)["map_type"].tolist()

    if ("PRIMARY" in map_types) and ("Y" in scd_indicator):
        scd_type = "SCD-2"
    elif "PRIMARY" in map_types:
        scd_type = "SCD-1"
    else:
        scd_type = "INSERT"

    return scd_type


def read_primary_keys(df):
    tgt_key_list = list(set(df[df.map_type.str.lower().str.strip() == 'primary']["cdm_column"]))
    return tgt_key_list


def get_scd_join_condition(df):
    tgt_key = read_primary_keys(df)
    cond_list = []
    for key in tgt_key:
        cond_list.append("src." + key + " = " + "tgt." + key)
    return ' and '.join(cond_list)


def subtract_dataframes(df1, df2):
    # Create a list of all the column names
    cols = set(df1.columns)
    for col in df2.columns:
        cols.add(col)
    cols = sorted(cols)

    for x in cols:
        if x not in df1.columns:
            df1 = df1.withColumn(x, lit(None))
        if x not in df2.columns:
            df2 = df2.withColumn(x, lit(None))
    df1 = df1.select(cols)  # Use 'select' to get the columns sorted
    df2 = df2.select(cols)

    result_df = df1.subtract(df2)
    return result_df


def scd2(sqlContext, src_df, tgt_df, join_cnd):
    src_df.createOrReplaceTempView("src")
    tgt_df.createOrReplaceTempView("tgt")

    unchanged_df1 = sqlContext.sql("select tgt.* from src join tgt "
                                   "on " + join_cnd + " and src.is_active = tgt.is_active "
                                                      "and src.hashkey = tgt.hashkey")
    # print("Capturing unchanged records b/w source and target : ", unchanged_df1.count())

    unchanged_df2 = sqlContext.sql("select tgt.* from tgt where tgt.is_active = 0")
    # print("Capturing inactive records from target: ", unchanged_df2.count())

    insert_df1 = sqlContext.sql("select src.* from src join tgt "
                                "on " + join_cnd + " and src.is_active = tgt.is_active "
                                                   "and src.hashkey != tgt.hashkey")
    # print("Capturing changed records b/w source and target : for insert: ", insert_df1.count())

    insert_df2 = sqlContext.sql("select src.* from src left join tgt "
                                "on " + join_cnd + " and src.is_active = tgt.is_active "
                                                   "where tgt.hashkey is null")
    # print("Capturing new records from source: ", insert_df2.count())

    update_df1 = sqlContext.sql("select tgt.* from src join tgt "
                                "on " + join_cnd + " and src.is_active = tgt.is_active "
                                                   "and src.hashkey != tgt.hashkey")
    update_df1 = update_df1.withColumn("delete_date", lit(datetime.now())) \
        .withColumn("last_modified_date", lit(datetime.now())) \
        .withColumn("is_active", lit(0))
    # print("Capturing changed records b/w source and target : for update: ", update_df1.count())

    update_df2 = sqlContext.sql("select tgt.* from src right join tgt "
                                "on " + join_cnd + " and src.is_active = tgt.is_active "
                                                   "where src.hashkey is null")
    update_df2 = update_df2.withColumn("delete_date", lit(datetime.now())) \
        .withColumn("last_modified_date", lit(datetime.now())) \
        .withColumn("is_active", lit(0))
    # print("Capturing deleted records from target: ", update_df2.count())

    # print("Combining all datasets")
    output_df = union_dataframes([unchanged_df1, unchanged_df2, update_df1, update_df2, insert_df1, insert_df2])
    # print(f"count - target (after load) {output_df.count()}")
    return output_df


def scd1(sqlContext, src_df, tgt_df, join_cnd):
    src_df.createOrReplaceTempView("src")
    tgt_df.createOrReplaceTempView("tgt")

    unchanged_df1 = sqlContext.sql("select tgt.* from src join tgt "
                                   "on " + join_cnd + " and src.is_active = tgt.is_active "
                                                      "and src.hashkey = tgt.hashkey")
    print("Capturing unchanged records b/w source and target : ", unchanged_df1.count())

    insert_df2 = sqlContext.sql("select src.* from src left join tgt "
                                "on " + join_cnd + " and src.is_active = tgt.is_active "
                                                   "where tgt.hashkey is null")
    print("Capturing new records from source: ", insert_df2.count())

    id_col = join_cnd.split("=")[0].split(".")[-1].strip().replace("_source_value", "_id")
    other_cols = ', '.join(['src.' + c for c in src_df.columns if c != id_col])
    update_df1 = sqlContext.sql(f"select tgt.{id_col}, {other_cols} from src join tgt "
                                f"on " + join_cnd + " and src.is_active = tgt.is_active "
                                                    "and src.hashkey != tgt.hashkey")
    update_df1 = update_df1.withColumn("add_date", lit(datetime.now())) \
        .withColumn("last_modified_date", lit(datetime.now())) \
        .withColumn("is_active", lit(1))
    print("Capturing changed records b/w source and target : for update: ", update_df1.count())

    # print("Combining all datasets")
    output_df = union_dataframes([unchanged_df1, update_df1, insert_df2])
    # print(f"count - target (after load) {output_df.count()}")
    return output_df


def copy_s3_folder_spark(sqlContext, src_path, tgt_path, write_mode="Overwrite"):
    try:
        df = read_parquet(sqlContext, src_path)
        df.write.parquet(path=tgt_path, mode=write_mode, compression="snappy")
    except Exception as e:
        print(f"could not copy data from {src_path} to {tgt_path}")
        print(f"error detail : {e}")
        raise e
    else:
        return True


def copy_s3_folder_spark_with_partition(sqlContext, src_path, tgt_path, part_col, write_mode="Overwrite"):
    try:
        df = read_parquet(sqlContext, src_path)
        fin_part_keys = []
        for _k in part_col.split(","):
            part_key = _k.strip()
            if part_key in df.columns:
                fin_part_keys.append(part_key)
        if fin_part_keys:
            print(f"partition columns {fin_part_keys} found in schema, writing with partitioning.")
            df.write.partitionBy(*fin_part_keys).parquet(path=tgt_path, mode=write_mode, compression="snappy")
        else:
            print(f"partition columns {part_col} not found in schema, writing without partitioning.")
            df.write.parquet(path=tgt_path, mode=write_mode, compression="snappy")
    except Exception as e:
        print(f"could not copy data from {src_path} to {tgt_path}")
        print(f"error detail : {e}")
        raise e
    else:
        return True


def move_s3_folder_spark(sqlContext, src_path, tgt_path, write_mode="Overwrite"):
    try:
        buc, key = s3_path_to_bucket_key(src_path)
        df = read_parquet(sqlContext, src_path)
        df.write.parquet(path=tgt_path, mode=write_mode, compression="snappy")
        time.sleep(10)
        # empty_df = sqlContext.createDataFrame([], df.schema)
        # empty_df.write.parquet(path=src_path, mode="Overwrite", compression="snappy")
        delete_s3_folder(buc, key)
    except Exception as e:
        print(f"could not copy data from {src_path} to {tgt_path}")
        print(f"error detail : {e}")
        raise e
    else:
        return True


def move_s3_folder_spark_with_partition(sqlContext, src_path, tgt_path, part_col, write_mode="Overwrite"):
    try:
        buc, key = s3_path_to_bucket_key(src_path)
        df = read_parquet(sqlContext, src_path)
        fin_part_keys = []
        for _k in part_col.split(","):
            part_key = _k.strip()
            if part_key in df.columns:
                fin_part_keys.append(part_key)
        if fin_part_keys:
            print(f"partition columns {fin_part_keys} found in schema, writing with partitioning.")
            df.write.partitionBy(*fin_part_keys).parquet(path=tgt_path, mode=write_mode, compression="snappy")
        else:
            print(f"partition columns {part_col} not found in schema, writing without partitioning.")
            df.write.parquet(path=tgt_path, mode=write_mode, compression="snappy")

        time.sleep(10)
        # empty_df = sqlContext.createDataFrame([], df.schema)
        # empty_df.write.parquet(path=src_path, mode="Overwrite", compression="snappy")
        delete_s3_folder(buc, key)
    except Exception as e:
        print(f"could not copy data from {src_path} to {tgt_path}")
        print(f"error detail : {e}")
        raise e
    else:
        return True


def delete_s3_folder_spark(sqlContext, path):
    try:
        buc, key = s3_path_to_bucket_key(path)
        # temp_schema = StructType([StructField('dummy', StringType(), False)])
        # empty_df = sqlContext.createDataFrame([], temp_schema)
        # empty_df.write.parquet(path=path, mode="Overwrite", compression="snappy")
        delete_s3_folder(buc, key)
    except Exception as e:
        print(f"could not delete data from {path}")
        print(f"error detail : {e}")
        raise e
    else:
        return True


def get_credentials_from_sm(secret_name, region_name):
    if secret_name is not None and region_name is not None:
        secrets_client = boto3.client('secretsmanager', region_name=region_name)
        db_credentials = secrets_client.get_secret_value(SecretId=secret_name)
        return db_credentials
    else:
        raise Exception("invalid secret name '{secret_name'} or region '{region_name}'")


def read_redshift_query(sqlContext, redshift_driver, redshift_url, s3_temp_dir, redshift_role, query):
    try:
        read_redshift_df = sqlContext.read \
            .format(redshift_driver) \
            .option("url", redshift_url) \
            .option("query", query) \
            .option("tempdir", s3_temp_dir) \
            .option("aws_iam_role", redshift_role) \
            .load()
    except Exception as err:
        print("error details : ", err)
        return None

    return read_redshift_df


def read_redshift_table(sqlContext, redshift_driver, redshift_url, s3_temp_dir, redshift_role, schema, table):
    counter = 0
    read_redshift_df = None
    while counter < 5:
        try:
            read_redshift_df = sqlContext.read \
                .format(redshift_driver) \
                .option("url", redshift_url) \
                .option("dbtable", schema + '.' + table) \
                .option("tempdir", s3_temp_dir) \
                .option("aws_iam_role", redshift_role) \
                .load()
            break
        except Exception as err:
            if counter == 4:
                print("error details : ", err)
                raise Exception(err)
            else:
                time.sleep(30)
                print("Retrying redshift connection again...")
            counter += 1
    if not read_redshift_df:
        raise Exception(f'Could not read redshift table :: {schema}.{table}. Check connection parameters')
    return read_redshift_df


def write_to_redshift(sqlContext, df, redshift_driver, redshift_url, redshift_schema, redshift_table,
                      s3_temp_dir, redshift_role, write_mode):
    try:
        df_s3_write = df.write \
            .format(redshift_driver) \
            .option("url", redshift_url) \
            .option("dbtable", redshift_schema + "." + redshift_table) \
            .option("tempdir", s3_temp_dir) \
            .option("aws_iam_role", redshift_role) \
            .mode(write_mode) \
            .save()
    except Exception as err:
        print(f"s3_parquet_to_redshift :: error - function write_to_redshift failed !!!")
        print("error details : ", err)
        raise err
    else:
        status = True

    return status


# get_randomized_id = udf(lambda: str(uuid.uuid4().int >> 64), StringType())


def lookup(args, cdm_col, src_col):
    lkp_tbl = args["lookup_table"]
    lkp_col = args['lookup_field']
    lkp_filter = args['lookup_filter']
    fn_output = args['fn_output']

    from_clause = f" from src left join (select {lkp_col}, {fn_output} as {cdm_col} from {lkp_tbl} where {lkp_filter}) lkp on lkp.{lkp_col} = src.{src_col}"
    select_clause = f" lkp.{cdm_col}"
    return lkp_tbl, from_clause, select_clause


def get_weights(src_cnt):
    a = 300000000
    num_parts = src_cnt // a + 1
    weights = []
    for i in range(num_parts):
        if i < num_parts - 1:
            weights.append(math.trunc(10000 / num_parts) / 10000)
        else:
            weights.append(1.0 - sum(weights))
    print(f"weights : {weights}")
    return weights


def get_main_table(df):
    """
    This function takes pandas dataframe as input,
    identifies main table and returns as output string.
    :param df: Pandas dataframe for mapping, for given src_name and table.
    :return: main table name
    """
    query_from = "select distinct lower(trim(src_table)) as source from df where trim(src_table)<>'' and trim(src_table) is not null " \
                 "and trim(src_table) not in (select trim(src_table) from df where lower(map_type) like '%join%')"

    from_list = ps.sqldf(query_from)["source"].tolist()
    if len(from_list) != 1:
        print(f"error :- could not identify driving table")
        return None
    else:
        from_clause = " ".join(from_list)
        return from_clause


def get_main_table_with_dataset(df):
    """
    This function takes pandas dataframe as input,
    identifies main table and returns as output string.
    :param df: Pandas dataframe for mapping, for given src_name and table.
    :return: main table name
    """
    query_from = "select distinct trim(src_name) || '*' || trim(src_table) as source from df where trim(src_table)<>'' and trim(src_table) is not null " \
                 "and trim(src_table) not in (select trim(src_table) from df where lower(map_type) like '%join%')"

    from_list = ps.sqldf(query_from)["source"].tolist()
    if len(from_list) != 1:
        print(f"error :- could not identify driving table")
        return None
    else:
        from_clause = " ".join(from_list)
        return from_clause


def rreplace(s, old, new):
    """
    This function replaces last occurrence of substring with new string.
    :param s: Main string
    :param old: String to search
    :param new: String to replace
    :return: String where old substring is replaced with new
    """
    return (s[::-1].replace(old[::-1], new[::-1], 1))[::-1]


#####################################################################################################################

def count_check(src_count_obj, src_tbl_list, tgt_df, config_file_path=None, version=None, mkt_bkt=None):
    Record_Count = []
    if str(type(src_count_obj)) == "<class 'str'>":
        red_version = version[-4:]
        mkt_bkt = mkt_bkt.upper()
        bucket, key_path = s3_path_to_bucket_key(config_file_path)
        filename = get_s3_object(bucket, key_path)
        param_contents = json.loads(filename)
        table_reference = param_contents["src_dataset"]["claims"]["table_count_reference"]
        print(f"Trying to source the count for the table {src_tbl_list} from {src_count_obj}")
        src_tbl_prefix = src_tbl_list.replace(src_tbl_list.split('_')[0] + '_' + src_tbl_list.split('_')[1] + '_', '')
        buc, key = s3_path_to_bucket_key(src_count_obj)
        src_count_content = get_s3_object(buc, key)
        sheet = 'Fact and Dimension'
        tbl_df = pd.read_excel(BytesIO(src_count_content), sheet, dtype=str).fillna("")
        src_tbl_filter = "V_JAN_" + mkt_bkt + "_" + red_version + "_" + table_reference[src_tbl_prefix]
        print(f"src_tbl_filter : {src_tbl_filter}")
        if table_reference[src_tbl_prefix] == "WP_PLAN_XREF":
            tbl_df = tbl_df[(tbl_df['Table Name'].str.contains(table_reference[src_tbl_prefix]))]
        else:
            tbl_df = tbl_df[tbl_df['Table Name'] == src_tbl_filter]
        Record_Count.append(int(tbl_df['Record Count']))

    elif str(type(src_count_obj)) == "<class 'list'>":
        source_count = 0
        if str(type(src_count_obj[0])) == "<class 'pyspark.sql.dataframe.DataFrame'>":
            for item in src_count_obj:
                source_count = source_count + int(item.count())
        elif str(type(src_count_obj[0])) in ("<class 'int'>", "<class 'str'>"):
            for item in src_count_obj:
                source_count = source_count + int(item)
        Record_Count.append(source_count)
    elif str(type(src_count_obj)) == "<class 'pyspark.sql.dataframe.DataFrame'>":
        Record_Count.append(src_count_obj.count())
    else:
        Record_Count.append("NO MATCH")
    if str(type(tgt_df)) == "<class 'list'>":
        target_count = 0
        if str(type(tgt_df[0])) == "<class 'pyspark.sql.dataframe.DataFrame'>":
            for item in tgt_df:
                target_count = target_count + int(item.count())
        elif str(type(tgt_df[0])) in ("<class 'int'>", "<class 'str'>"):
            for item in tgt_df:
                target_count = target_count + int(item)
        Record_Count.append(target_count)
    elif str(type(tgt_df)) == "<class 'pyspark.sql.dataframe.DataFrame'>":
        Record_Count.append(tgt_df.count())
    else:
        Record_Count.append("NO MATCH")

    return Record_Count


def get_table_list_from_sql(sql):
    replace_list = ['\n', '(', ')', '*', '=']
    for i in replace_list:
        sql = sql.replace(i, ' ')
    txt = sql.split()
    src_tbl_list = []
    for i in range(1, len(txt)):
        if txt[i - 1].lower() in ['from', 'join'] and txt[i].lower() != 'select':
            src_tbl_list.append(txt[i].lower())
    return list(set(src_tbl_list))


def get_source_list(mapping_df):
    src_li = [src.strip().lower() for src in pd.unique(mapping_df.src_name) if src.strip() != '']
    return src_li


def get_ruleorder_ruleset(mapping_df):
    mapping_df["rule_order"] = mapping_df["rule_order"].replace('', '9999').astype(str).astype(int)
    mapping_df["rule_set"] = mapping_df["rule_set"].replace('', '9999').astype(str).astype(int)

    ro_rs_raw = mapping_df.groupby(['rule_order']).agg({'rule_set': set}).agg(dict)['rule_set']
    ro_rs_sorted = {}
    for k1 in sorted(list(ro_rs_raw.keys())):
        ro_rs_sorted[k1] = sorted(ro_rs_raw[k1])
    return ro_rs_sorted


def get_source_table_list(mapping_df):
    src_li = [src.strip().lower() for src in pd.unique(mapping_df.src_table) if src.strip() != '']
    return src_li


def get_sql_file_name(mapping_df):
    sql_tx_li = [s for s in pd.unique(mapping_df.sql_tx) if s.strip().endswith(".sql")]
    return sql_tx_li[0] if sql_tx_li else None


def get_all_columns_from_df_list(spark_df_li):
    all_cols = {}
    for df in spark_df_li:
        for k, v in dict(df.dtypes).items():
            if k in all_cols:
                if all_cols[k] == 'string':
                    continue
            all_cols[k] = v
    return all_cols


def get_inc_load_args(mapping_df):
    inc_arg_li = [s for s in pd.unique(mapping_df.inc_arg) if s.strip() != ""]
    return ast.literal_eval(inc_arg_li[0]) if inc_arg_li else None


def get_custom_flag(mapping_df):
    custom_flg = [flg.strip() for flg in pd.unique(mapping_df.custom.str.upper()) if flg.strip() == 'Y']
    return custom_flg[0] if len(custom_flg) == 1 else 'N'


def get_cdm_to_src_tables(mapping_df):
    table_dict = mapping_df.groupby(['cdm_table']).agg({'src_table': set}).agg(dict)['src_table']
    for k, v in table_dict.items():
        li = []
        for i in v:
            if i.strip() != '':
                li.append(i)
        table_dict[k] = li
    return table_dict


def has_column_in_spark_df(spark_df, column):
    for c in spark_df.columns:
        if c.strip().lower() == column.strip().lower():
            return True
    return False


def get_first_existing_path(*paths):
    for path in paths:
        if s3_path_exists(path):
            return path
    return None


def get_derived_entity_list_per_source(mapping_file, tgt_name, arn=None):
    mapping_df = read_mapping(mapping_file, dataset=tgt_name, arn=arn)
    table_list = get_cdm_to_src_tables(mapping_df)
    der_ent = []
    for t in table_list:
        if (t != '') and not (table_list[t]):
            der_ent.append(t)
    return der_ent


def get_derived_entity_list_per_table(mapping_file, cdm_table, tgt_name, arn=None):
    list1 = get_derived_entity_list_per_source(mapping_file, tgt_name=tgt_name, arn=arn)
    list2 = get_target_column_entity_list(mapping_file, cdm_table, tgt_name, arn)
    return list(set(list1) & set(list2))


def is_derived_table(mapping_file, table_name, tgt_name, arn=None):
    if table_name in get_derived_entity_list_per_source(mapping_file, tgt_name, arn):
        return True
    else:
        return False


def get_spark_type(datatype):
    if datatype is None or datatype == '':
        return StringType()
    elif datatype.lower() == 'string':
        return StringType()
    elif datatype.lower() == 'date':
        return DateType()
    elif datatype.lower() == 'timestamp':
        return TimestampType()
    elif datatype.lower() in ['boolean', "bit", "bool"]:
        return BooleanType()
    elif datatype.lower() == 'binary':
        return BinaryType()
    elif datatype.lower() == 'short':
        return ShortType()
    elif datatype.lower() == 'integer':
        return IntegerType()
    elif datatype.lower() == 'long':
        return LongType()
    elif datatype.lower() == 'float':
        return FloatType()
    elif datatype.lower() == 'double':
        return DoubleType()
    elif datatype.lower() == 'decimal':
        return DecimalType()
    elif datatype.lower() == 'byte':
        return ByteType()
    elif datatype.lower() == 'null':
        return NullType()
    else:
        return StringType()


def get_linkage_flag(mapping_df):
    lnk_flg = [flg for flg in pd.unique(mapping_df.linkage.str.upper().str.strip()) if flg != '']
    return lnk_flg[0] if len(lnk_flg) == 1 else None


def check_is_available_flag(bucket, config_file, layer, table_name, src_name=None):
    """
    Check if source File is marked Available
    :param bucket:
    :param config_file:
    :param layer:
    :param table_name:
    :param src_name:
    :return:
    """
    param_data = get_s3_object(bucket, config_file)
    param_contents = json.loads(param_data)
    layer_list = ['ICDM', 'INGESTION', 'INPUT_DATASET']
    mapping_key_dict = {"ICDM": "icdm_mapping", "INGESTION": "ing_mapping",
                        "INPUT_DATASET": "input_dataset"}
    if layer.upper() in layer_list:
        print(f"Processing for {table_name} started under layer - {layer.upper()}")
        idx = layer_list.index(layer.upper())
        if idx < 2:
            mapping_key = mapping_key_dict[layer_list[idx]]
            print(f"mapping_key : {mapping_key}")
            mapping_file_path = param_contents[mapping_key]
            _, file_path = s3_path_to_bucket_key(mapping_file_path)
            print(f"Mapping file_path for {layer.upper()} : {file_path}")
            mapping_content = get_s3_object(bucket, file_path)
            mapping_df = pd.read_csv(BytesIO(mapping_content), dtype=str).fillna('NA')
            if idx == 0:
                element_df = mapping_df[(mapping_df.src_table.str.upper() != 'NA') &
                                        (mapping_df.cdm_table.str.lower() == table_name.lower())][
                    ["src_name", "src_table"]]
            else:
                element_df = mapping_df[(mapping_df.cdm_name.str.lower() == src_name.lower()) &
                                        (mapping_df.src_table.str.upper() != 'NA') &
                                        (mapping_df.cdm_table.str.lower() == table_name.lower())][
                    ["src_name", "src_table"]]

            required_list = element_df.drop_duplicates().to_dict('records')
            print(f"required_list :: {required_list}")

            if idx == 0:
                custom_element_df = mapping_df[(mapping_df.cdm_table.str.lower() == table_name.lower())]
            else:
                custom_element_df = mapping_df[(mapping_df.cdm_name.str.lower() == src_name.lower()) &
                                               (mapping_df.cdm_table.str.lower() == table_name.lower())]
            custom_flg = get_custom_flag(custom_element_df)

            if custom_flg == 'Y':
                layer = layer_list[idx]
            else:
                layer = layer_list[idx + 1]
            output_list = []
            for dict_key in required_list:
                tbl_nm = dict_key["src_table"]
                if custom_flg == 'Y':
                    source_name = src_name
                else:
                    source_name = dict_key["src_name"]
                returned_value = check_is_available_flag(bucket, config_file, layer, tbl_nm, source_name)
                print(f"returned_value :: {returned_value}")
                if str(type(returned_value)) == "<class 'list'>":
                    for item in returned_value:
                        if item not in output_list:
                            output_list.append(item)

            return output_list

        else:
            print(f"Processing for {table_name} started under layer - {layer.upper()}")
            mapping_key = mapping_key_dict[layer_list[idx]]
            mapping_file_path = param_contents[mapping_key]
            _, file_path = s3_path_to_bucket_key(mapping_file_path)
            mapping_content = get_s3_object(bucket, file_path)
            mapping_df = pd.read_csv(BytesIO(mapping_content), dtype=str).fillna('NA')
            element_df = mapping_df[(mapping_df.src.str.lower() == src_name.lower()) &
                                    (mapping_df.dataset.str.lower() == table_name.lower())][
                ["src", "dataset", "data_source_id", "data_source", "is_file_available"]]
            print(f"element_df :: {element_df}")
            is_avlbl_list = element_df.drop_duplicates().to_dict('records')
            print(f"is_avlbl_list :: {is_avlbl_list}")

            return is_avlbl_list


def call_if_file_available(bucket, config_file, layer, table_dict):
    final_list_of_output = []
    for key1 in table_dict:
        list_of_tables = table_dict[key1]
        for key2 in list_of_tables:
            output_list = check_is_available_flag(bucket, config_file, layer, key2, key1)
            for key in output_list:
                if key not in final_list_of_output:
                    final_list_of_output.append(key)

    return final_list_of_output


def generate_file_level_summary(sqlcontext, src_path, target_summary_temp_path, extract_tbl_nm,
                                generation_time=datetime.now().strftime('%Y-%m-%d'),
                                file_type='parquet', delimiter=','):
    """
    :param sqlcontext: spark sql context
    :param src_path: source folder path
    :param generation_time: summary generation date (default current date in '%Y-%m-%d')
    :param file_type: file extension (default parquet)
    :param delimiter: file data separator (default ',') :: eligible only for delimited files
    :return: extract_file_ctl_tbl_path : generated csv filepath

    This function takes in source folder path, fetches list of individual files,
    loads them in spark dataframe and perform count operation, calculates file size in bytes.
    The record count is then put into csv file in s3 under outbound/<version>/ctl_files_temp/file_control_table.
    """
    if s3_path_exists(src_path):
        # extract_tbl_nm = src_path.split('/')[-1].strip()
        extract_file_ctl_tbl_path = target_summary_temp_path + f'ctl_files_temp/file_control_table/{extract_tbl_nm}_file_control_table.csv'
        bucket, path = s3_path_to_bucket_key(add_slash(src_path))
        bucket, output_summary_key = s3_path_to_bucket_key(extract_file_ctl_tbl_path)
        if s3_path_exists(extract_file_ctl_tbl_path):
            delete_s3_object(bucket, output_summary_key)
        object_ls = []
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')

        pages = paginator.paginate(Bucket=bucket, Prefix=path)
        for page in pages:
            for obj in page['Contents']:
                object_ls.append(obj['Key'])
        count = 0
        for item in object_ls:
            parquet_file_nm = item.split('/')[-1]
            folder_path = add_slash(extract_tbl_nm)
            response = s3_client.head_object(Bucket=bucket, Key=item)
            file_size = response['ContentLength']
            file_size_kb = round(file_size / 1024, 1)
            if file_type == 'parquet':
                extract_df = read_parquet(sqlcontext, bucket_key_to_s3_path(bucket, item))
            elif file_type == 'delimited':
                extract_df = read_csv(sqlcontext, bucket_key_to_s3_path(bucket, item),
                                      delimiter=delimiter)
            row_count = extract_df.count()
            parquet_data = [[folder_path, parquet_file_nm, file_size, generation_time, row_count]]
            parquet_df_count = pd.DataFrame(parquet_data, columns=['Path', 'file_name',
                                                                   'size_KB', 'export_date', 'row_counts'])
            if count == 0:
                parquet_df_count.to_csv(extract_file_ctl_tbl_path, mode='a', header=True, index=False)
            else:
                parquet_df_count.to_csv(extract_file_ctl_tbl_path, mode='a', header=False, index=False)
            count += 1
        return extract_file_ctl_tbl_path
    else:
        raise Exception(f"Path {src_path} does not exist")


def generate_table_level_summary(sqlcontext, src_path, target_summary_temp_path, extract_tbl_nm,
                                 generation_time=datetime.now().strftime('%Y-%m-%d'),
                                 file_type='parquet', delimiter=','):
    """
    :param sqlcontext: spark sql context
    :param src_path: source folder path
    :param generation_time: summary generation date (default current date in '%Y-%m-%d')
    :param file_type: file extension (default parquet)
    :param delimiter: file data separator (default ',') :: eligible only for delimited files
    :return: extract_ctl_ttl_tbl_path : generated csv filepath

    This function takes in file/folder path, loads it into spark dataframe,
    performs file count and record count operation on the entire dataset and writes in
    csv file in s3 under outbound/<version>/ctl_files_temp/control_total_table.
    """
    if s3_path_exists(src_path):
        # extract_tbl_nm = src_path.split('/')[-1].strip()
        extract_ctl_ttl_tbl_path = target_summary_temp_path + f'ctl_files_temp/control_total_table/{extract_tbl_nm}_control_total_table.csv'
        bucket, path = s3_path_to_bucket_key(add_slash(src_path))
        bucket, output_summary_key = s3_path_to_bucket_key(extract_ctl_ttl_tbl_path)
        if s3_path_exists(extract_ctl_ttl_tbl_path):
            delete_s3_object(bucket, output_summary_key)
        folder_path = add_slash(extract_tbl_nm)
        object_ls = []
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=path)
        for page in pages:
            for obj in page['Contents']:
                object_ls.append(obj['Key'])
        file_count = len(object_ls)
        if file_type == 'parquet':
            df = read_parquet(sqlcontext, src_path, schema=None)
        elif file_type == 'delimited':
            df = read_csv(sqlcontext, src_path, delimiter=delimiter)
        record_count = df.count()
        print(f"record_count : {record_count}")
        data = [[folder_path, record_count, generation_time, file_count]]
        print(data)
        df_count = pd.DataFrame(data, columns=['Path', 'Row_Count', 'Export_Date', 'File_Count'])
        time.sleep(10)
        print(df_count)
        df_count.to_csv(extract_ctl_ttl_tbl_path, mode='a', header=True, index=False)
        time.sleep(10)
        return extract_ctl_ttl_tbl_path
    else:
        raise Exception(f"Path {src_path} does not exist")


def generate_table_level_summary_v2(map_df, load_type, sqlcontext, src_path, target_summary_temp_path, extract_tbl_nm,
                                    generation_time=datetime.now().strftime('%Y-%m-%d'),
                                    file_type='parquet', delimiter=','):
    """
    :param sqlcontext: spark sql context
    :param src_path: source folder path
    :param generation_time: summary generation date (default current date in '%Y-%m-%d')
    :param file_type: file extension (default parquet)
    :param delimiter: file data separator (default ',') :: eligible only for delimited files
    :return: extract_ctl_ttl_tbl_path : generated csv filepath

    This function takes in file/folder path, loads it into spark dataframe,
    performs file count and record count operation on the entire dataset and writes in
    csv file in s3 under outbound/<version>/ctl_files_temp/control_total_table.
    """
    if s3_path_exists(src_path):
        # extract_tbl_nm = src_path.split('/')[-1].strip()
        extract_ctl_ttl_tbl_path = target_summary_temp_path + f'ctl_files_temp/control_total_table_v2/{extract_tbl_nm}_control_total_table_v2.csv'
        bucket, path = s3_path_to_bucket_key(add_slash(src_path))
        bucket, output_summary_key = s3_path_to_bucket_key(extract_ctl_ttl_tbl_path)
        if s3_path_exists(extract_ctl_ttl_tbl_path):
            delete_s3_object(bucket, output_summary_key)
        folder_path = add_slash(extract_tbl_nm)
        object_ls = []
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=path)
        for page in pages:
            for obj in page['Contents']:
                object_ls.append(obj['Key'])
        file_count = len(object_ls)
        if file_type == 'parquet':
            df = read_parquet(sqlcontext, src_path, schema=None)
        elif file_type == 'delimited':
            df = read_csv(sqlcontext, src_path, delimiter=delimiter)
        record_count = df.count()
        data_version = add_slash(src_path).replace(add_slash(extract_tbl_nm), '').split("/")[-2]
        if 'zorder_by' in map_df.columns:
            zorder_by = map_df["zorder_by"][0] if map_df["zorder_by"][0] != '' else 'NA'
        else:
            zorder_by = 'NA'
        if 'load_function' in map_df.columns:
            load_function = map_df["load_function"][0] if map_df["load_function"][0] != '' else 'NA'
        else:
            load_function = 'NA'
        if 'azure_inbound_schema' in map_df.columns:
            azure_inbound_schema = map_df["azure_inbound_schema"][0] if map_df["azure_inbound_schema"][
                                                                            0] != '' else 'NA'
        else:
            azure_inbound_schema = 'NA'
        print(f"record_count : {record_count}")
        data = [[remove_slash(extract_tbl_nm), add_slash(src_path), generation_time, file_count, record_count,
                 data_version, azure_inbound_schema, load_type,
                 load_function, zorder_by]]
        print(data)
        df_count = pd.DataFrame(data, columns=['EXTRACT_TABLE_NAME', 'EXTRACT_FOLDER_PATH', 'EXPORT_DATE',
                                               'FILE_COUNT', 'RECORD_COUNT', 'DATA_VERSION', 'SCHEMA_NAME',
                                               'LOAD_TYPE', 'LOAD_FUNCTION', 'ZORDER_BY'])
        time.sleep(10)
        print(df_count)
        df_count.to_csv(extract_ctl_ttl_tbl_path, mode='a', header=True, index=False)
        time.sleep(10)
        return extract_ctl_ttl_tbl_path
    else:
        raise Exception(f"Path {src_path} does not exist")


def generate_data_dictionary_summary(sqlcontext, src_path, target_summary_temp_path, table_type, extract_tbl_nm,
                                     file_type='parquet',
                                     delimiter=','):
    """
    :param sqlcontext: spark sql context
    :param src_path: source folder path
    :param file_type: file extension (default parquet)
    :param delimiter: file data separator (default ',') :: eligible only for delimited files
    :return: extract_data_dict_tbl_path : generated csv filepath

    This function takes in file/folder path, loads it into spark dataframe,
    checks for column name and datatype, writes in  csv file in s3 under outbound/<version>/ctl_files_temp/data_dictionary_layout.
    """
    if s3_path_exists(src_path):
        # extract_tbl_nm = src_path.split('/')[-1].strip()
        extract_data_dict_tbl_path = target_summary_temp_path + f'ctl_files_temp/{table_type}_data_dictionary_layout/{extract_tbl_nm}_data_dictionary_layout.csv'
        bucket, output_summary_key = s3_path_to_bucket_key(extract_data_dict_tbl_path)
        if s3_path_exists(extract_data_dict_tbl_path):
            delete_s3_object(bucket, output_summary_key)

        folder_path = add_slash(extract_tbl_nm)
        if file_type == 'parquet':
            df = read_parquet(sqlcontext, src_path, schema=None)
        elif file_type == 'delimited':
            df = read_csv(sqlcontext, src_path, delimiter=delimiter)
        df_datatype_ls = df.dtypes
        count = 0
        for ls_item in df_datatype_ls:
            field_name = ls_item[0]
            field_type = ls_item[1]
            field_data = [[folder_path, field_name, field_type]]
            field_data_df = pd.DataFrame(field_data, columns=['folder', 'field_name', 'data_type'])
            if count == 0:
                field_data_df.to_csv(extract_data_dict_tbl_path, mode='a', header=True, index=False)
            else:
                field_data_df.to_csv(extract_data_dict_tbl_path, mode='a', header=False, index=False)
            count += 1
        return extract_data_dict_tbl_path
    else:
        raise Exception(f"Path {src_path} does not exist")


def collect_file_level_summary(src_path, output_path):
    """
    :param src_bucket: s3 bucket
    :param src_path: source folder path till the outbound/<version>/
    :return: summarized csv path
    """
    src_bucket, path = s3_path_to_bucket_key(src_path)
    summary_input_path = path + '/ctl_files_temp/file_control_table'
    object_ls = []
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=src_bucket, Prefix=summary_input_path)
    for page in pages:
        if 'Contents' in page.keys():
            for obj in page['Contents']:
                object_ls.append(bucket_key_to_s3_path(src_bucket, obj['Key']))
    summary_output_path = output_path + '/file_control/file_control_table.csv'
    if len(object_ls) > 0:
        df_res = pd.concat(map(pd.read_csv, object_ls), ignore_index=True)
        df_res["Path"] = df_res["Path"].str.rstrip("/")
        df_res.to_csv(summary_output_path, header=True, index=False, sep="|")

    return summary_output_path


def collect_table_level_summary(src_path, output_path):
    """
    :param src_bucket: s3 bucket
    :param src_path: source folder path till the ctl_files_temp
    :return: summarized csv path
    """
    src_bucket, path = s3_path_to_bucket_key(src_path)
    summary_input_path = path + '/ctl_files_temp/control_total_table/'
    object_ls = []
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=src_bucket, Prefix=summary_input_path)
    for page in pages:
        if 'Contents' in page.keys():
            for obj in page['Contents']:
                object_ls.append(bucket_key_to_s3_path(src_bucket, obj['Key']))
    summary_output_path = output_path + '/control_totals/control_total_table.csv'
    if len(object_ls) > 0:
        df_res = pd.concat(map(pd.read_csv, object_ls), ignore_index=True)
        df_res["Path"] = df_res["Path"].str.rstrip("/")
        df_res.to_csv(summary_output_path, header=True, index=False, sep="|")
    return summary_output_path


def collect_table_level_summary_v2(src_path, output_path):
    """
    :param src_bucket: s3 bucket
    :param src_path: source folder path till the ctl_files_temp
    :return: summarized csv path
    """
    src_bucket, path = s3_path_to_bucket_key(src_path)
    summary_input_path = path + '/ctl_files_temp/control_total_table_v2/'
    object_ls = []
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=src_bucket, Prefix=summary_input_path)
    for page in pages:
        if 'Contents' in page.keys():
            for obj in page['Contents']:
                object_ls.append(bucket_key_to_s3_path(src_bucket, obj['Key']))
    summary_output_path = output_path + '/ctl_files/control_total_table_v2.csv'
    if len(object_ls) > 0:
        df_res = pd.concat(map(pd.read_csv, object_ls), ignore_index=True).fillna("NA")
        df_res.to_csv(summary_output_path, header=True, index=False)
    return summary_output_path


def collect_data_dictionary_summary(src_path, output_path):
    """
    :param src_bucket: s3 bucket
    :param src_path: source folder path till the ctl_files_temp
    :return: summarized csv path
    """
    src_bucket, path = s3_path_to_bucket_key(src_path)
    output_paths = []
    for table_type in ['dimension', 'fact']:
        summary_input_path = path + f'/ctl_files_temp/{table_type}_data_dictionary_layout'
        summary_input_full_path = bucket_key_to_s3_path(src_bucket, summary_input_path)
        if not s3_path_exists(summary_input_full_path):
            continue
        object_ls = []
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=src_bucket, Prefix=summary_input_path)
        for page in pages:
            if 'Contents' in page.keys():
                for obj in page['Contents']:
                    object_ls.append(bucket_key_to_s3_path(src_bucket, obj['Key']))
        summary_output_path = output_path + f'/schema_control/{table_type}_data_dictionary_layout.csv'
        if len(object_ls) > 0:
            df_res = pd.concat(map(pd.read_csv, object_ls), ignore_index=True)
            df_res.to_csv(summary_output_path, header=True, index=False, sep=",")
        output_paths.append(summary_output_path)
    return output_paths


def get_distinct_rows(sqlContext, df, drop_dup_query_dict, tbl):
    col_list = df.columns
    print(col_list)
    df.createOrReplaceTempView(tbl)
    df_dict = {}
    for key in drop_dup_query_dict:
        primary_sql = drop_dup_query_dict[key].strip()
        df_dict[key] = sqlContext.sql(primary_sql)
        df_dict[key].createOrReplaceTempView(key)
    fin_cols = [_ for _ in df_dict[key].columns if _ in col_list]
    print(fin_cols)
    out_df = df_dict[key].select(fin_cols)
    return out_df


def read_latest_records_from_parquet(sqlContext, filename, key=[], schema=None):
    """
    This function reads parquet file, sorts records based on add_date desc, drop duplicates based on key provided
    and returns contents with spark dataframe.
    :param sqlContext: SQL context.
    :param filename: Name of the file along with path.
    :param key: An array of primary keys on which duplicates will be deleted.
    :param schema: If schema is passed, dataframe will be created with given schema.
    :return: If parquet file read successfully, return dataframe, otherwise None
    """
    try:
        df = sqlContext.read.format('parquet') \
            .option("schema", schema) \
            .load(filename)
        df = df.withColumn("row_number",
                           row_number().over(Window.partitionBy(key).orderBy(col("add_date").desc_nulls_last())))
        df = df.filter(df.row_number == "1")
        df = df.drop(col('row_number'))
        # df = df.sort(df.add_date.desc_nulls_last()).drop_duplicates(subset = key)
    except Exception as err:
        print(err)
    else:
        return df


def check_src_file_validity(bucket, param_contents, layer, table_name, src_name=None):
    print("Started check_src_file_validity with ", table_name, src_name)
    mapping_key_dict = {"ICDM": "icdm_mapping", "INGESTION": "ing_mapping",
                        "INPUT_DATASET": "input_dataset"}
    mapping_file_path = param_contents[mapping_key_dict["INPUT_DATASET"]]
    _, file_path = s3_path_to_bucket_key(mapping_file_path)
    mapping_content = get_s3_object(bucket, file_path)
    input_dataset_df = pd.read_csv(BytesIO(mapping_content), dtype=str).fillna('NA')
    print("layer::", layer)
    if layer.upper() == 'INGESTION':
        input_dataset_row = input_dataset_df[
            ((input_dataset_df.src.str.strip().str.lower() == src_name) &
             (input_dataset_df.table_name.str.strip().str.lower() == table_name))]
        print("input_dataset_row is_file_available::", input_dataset_row.is_file_available.unique())
        if '1' in input_dataset_row.is_file_available.unique() or 1 in input_dataset_row.is_file_available.unique():
            return True
        else:
            return False
    elif layer.upper() != 'INGESTION':
        ing_mapping_file_path = param_contents[mapping_key_dict['INGESTION']]
        _, ing_file_path = s3_path_to_bucket_key(ing_mapping_file_path)
        ing_mapping_content = get_s3_object(bucket, ing_file_path)
        ing_mapping_df = pd.read_csv(BytesIO(ing_mapping_content), dtype=str).fillna('NA')
        ing_mapping_df = ing_mapping_df[(ing_mapping_df.cdm_name.str.strip().str.lower() == src_name) &
                                        (ing_mapping_df.cdm_table.str.strip().str.lower() == table_name)][
            ["src_name", "src_table"]]
        ing_src_name_li = list(ing_mapping_df.src_name.unique())
        ing_src_table_li = list(ing_mapping_df.src_table.unique())
        ing_src_name_li = [x.lower() for x in ing_src_name_li]
        ing_src_table_li = [x.lower() for x in ing_src_table_li]
        print("ing_src_name_li, ing_src_table_li::", ing_src_name_li, ing_src_table_li)

        if len(ing_src_name_li) == 0:
            raise Exception("ERROR :: check_src_file_validity :: Unique Source Name from ingestion mapping is empty ")
        elif len(ing_src_table_li) == 0:
            raise Exception(
                "ERROR :: check_src_file_validity :: Unique Source Table name from ingestion mapping is empty")

        input_dataset_row = input_dataset_df[((input_dataset_df.src.str.strip().str.lower().isin(ing_src_name_li)) &
                                              (input_dataset_df.dataset.str.strip().str.lower().isin(
                                                  ing_src_table_li)))]
        print("input_dataset_row is_file_available::", input_dataset_row.is_file_available.unique())

        if '1' in input_dataset_row.is_file_available.unique() or 1 in input_dataset_row.is_file_available.unique():
            return True
        else:
            return False


def collect_qa_summary(src_path, output_path):
    """
    :param src_bucket: s3 bucket
    :param src_path: source folder path till the qa_summary/<batch_date>
    :return: summarized csv path
    """
    src_bucket, path = s3_path_to_bucket_key(src_path)
    object_ls = []
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=src_bucket, Prefix=path)
    for page in pages:
        for obj in page['Contents']:
            object_ls.append(bucket_key_to_s3_path(src_bucket, obj['Key']))
    print("Files to append : ", object_ls)
    df_res = pd.concat(map(pd.read_csv, object_ls), ignore_index=True)
    df_res.to_csv(output_path, mode='a', header=True, index=False)


def get_randomized_id(col_list, seed=0):
    return seed + dense_rank().over(Window.orderBy(col_list))


def get_grouped_tables_by_sources(map_df):
    src_li = [src.strip().lower() for src in pd.unique(map_df.src_name) if src.strip() != '']
    print(f"src_li :: {src_li}")
    grouped_tables_by_sources = {}
    for src_nm in src_li:
        grouped_tables_by_sources[src_nm] = [x for x in map_df[(map_df.src_name == src_nm)].src_table.unique().tolist()
                                             if x]
    print(f"grouped_tables_by_sources :: {grouped_tables_by_sources}")
    return grouped_tables_by_sources


def get_src_name(df):
    # get rule order and rule set
    ro_rs_li = get_ruleorder_ruleset(df)
    ro = list(ro_rs_li)[0]
    rs = ro_rs_li[ro][0]
    # get source list
    src_li = get_source_list(df)
    if len(src_li) > 1:
        raise Exception(f"get_src_name : rule order = {ro}, rule set = {rs} has more than one source")
    if len(src_li) < 1:
        raise Exception(
            f"get_src_name : atleast one source name should be defined for rule order = {ro}, rule set = {rs}")
    return src_li[0]


def get_src_sql(df, bucket, sql_script_path):
    # get sql file name
    sql_file_nm = get_sql_file_name(df)
    # if sql_file_nm is not defined build sql from mapping
    if sql_file_nm is not None:
        sql_script_file = add_slash(sql_script_path) + sql_file_nm
        if not s3_path_exists(bucket_key_to_s3_path(bucket, sql_script_file)):
            raise Exception(f"get_src_sql : {bucket_key_to_s3_path(bucket, sql_script_file)} does not exist")
        src_sql = get_s3_object(bucket, sql_script_file).decode('utf-8')
    else:
        src_mapping_df = df[(df.src_table != '') | (df.src_column != '') | (df.sql_tx != '')]
        src_sql = build_src_sql(src_mapping_df)
    return src_sql


def pattern_match(pattern, full_text):
    """
    function to perform pattern match with wild card characters
    """
    # If we reach at the end of both strings, we are done
    if len(pattern) == 0 and len(full_text) == 0:
        return True

    # Make sure that the characters after '*' are present
    # in second string. This function assumes that the first
    # string will not contain two consecutive '*'
    if len(pattern) > 1 and pattern[0] == '*' and len(full_text) == 0:
        return False

    # If the first string contains '?', or current characters
    # of both strings match
    if (len(pattern) > 1 and pattern[0] == '?') or (
            len(pattern) != 0 and len(full_text) != 0 and pattern[0] == full_text[0]):
        return pattern_match(pattern[1:], full_text[1:])

    # If there is *, then there are two possibilities
    # a) We consider current character of second string
    # b) We ignore current character of second string.
    if len(pattern) != 0 and pattern[0] == '*':
        return pattern_match(pattern[1:], full_text) or pattern_match(pattern, full_text[1:])

    return False


def getMonthNum(monthName):
    if (
            monthName.lower() == "january" or monthName.lower() == "jan" or monthName.lower() == "jan/feb" or monthName.lower() == "january/february"):
        print(monthName + " 01")
        return "01"
    elif (
            monthName.lower() == "february" or monthName.lower() == "feb" or monthName.lower() == "feb/mar" or monthName.lower() == "february/march"):
        print(monthName + " 02")
        return "02"
    elif (
            monthName.lower() == "march" or monthName.lower() == "mar" or monthName.lower() == "mar/apr" or monthName.lower() == "march/april"):
        print(monthName + " 03")
        return "03"
    elif (
            monthName.lower() == "april" or monthName.lower() == "apr" or monthName.lower() == "apr/may" or monthName.lower() == "april/may"):
        print(monthName + " 04")
        return "04"
    elif (
            monthName.lower() == "may" or monthName.lower() == "may" or monthName.lower() == "may/jun" or monthName.lower() == "may/june"):
        print(monthName + " 05")
        return "05"
    elif (
            monthName.lower() == "june" or monthName.lower() == "jun" or monthName.lower() == "jun/jul" or monthName.lower() == "june/july"):
        print(monthName + " 06")
        return "06"
    elif (
            monthName.lower() == "july" or monthName.lower() == "jul" or monthName.lower() == "jul/aug" or monthName.lower() == "july/august"):
        print(monthName + " 07")
        return "07"
    elif (
            monthName.lower() == "august" or monthName.lower() == "aug" or monthName.lower() == "aug/sep" or monthName.lower() == "august/september"):
        print(monthName + " 08")
        return "08"
    elif (
            monthName.lower() == "september" or monthName.lower() == "sep" or monthName.lower() == "sept/oct" or monthName.lower() == "september/october"):
        print(monthName + " 09")
        return "09"
    elif (
            monthName.lower() == "october" or monthName.lower() == "oct" or monthName.lower() == "oct/nov" or monthName.lower() == "october/november"):
        print(monthName + " 10")
        return "10"
    elif (
            monthName.lower() == "november" or monthName.lower() == "nov" or monthName.lower() == "nov/dec" or monthName.lower() == "november/december"):
        print(monthName + " 11")
        return "11"
    elif (
            monthName.lower() == "december" or monthName.lower() == "dec" or monthName.lower() == "dec/jan" or monthName.lower() == "december/january"):
        print(monthName + " 12")
        return "12"
    else:
        print("input: " + monthName)
        fo = re.sub('dec|december', '12', re.sub('nov|november', '11', re.sub('oct|october', '10',
                                                                              re.sub('sep|sept|september', '09',
                                                                                     re.sub('aug|august', '08',
                                                                                            re.sub('jul|july', '07',
                                                                                                   re.sub('jun|june',
                                                                                                          '06',
                                                                                                          re.sub(
                                                                                                              'may|may',
                                                                                                              '05',
                                                                                                              re.sub(
                                                                                                                  'apr|april',
                                                                                                                  '04',
                                                                                                                  re.sub(
                                                                                                                      'mar|march',
                                                                                                                      '03',
                                                                                                                      re.sub(
                                                                                                                          'feb|february',
                                                                                                                          '02',
                                                                                                                          re.sub(
                                                                                                                              'jan|january',
                                                                                                                              '01',
                                                                                                                              monthName,
                                                                                                                              flags=re.IGNORECASE),
                                                                                                                          flags=re.IGNORECASE),
                                                                                                                      flags=re.IGNORECASE),
                                                                                                                  flags=re.IGNORECASE),
                                                                                                              flags=re.IGNORECASE),
                                                                                                          flags=re.IGNORECASE),
                                                                                                   flags=re.IGNORECASE),
                                                                                            flags=re.IGNORECASE),
                                                                                     flags=re.IGNORECASE),
                                                                              flags=re.IGNORECASE),
                                                 flags=re.IGNORECASE), flags=re.IGNORECASE)
        print("formatted output: " + fo)
        return fo


def cleanseDate(s):
    if s is None:
        return None
    resStr = ""
    arr = re.split('[_,][_,]| ', s)
    for x in arr:
        resStr = resStr + getMonthNum(x) + " "
    return re.sub('[a-z]|[A-Z]| |\.', '', resStr)


""" Converting function to UDF """
cleanseDateUDF = udf(lambda z: cleanseDate(z))


def valStrList(strList):
    if strList is None:
        return False

    flag = False
    for x in strList:
        if (len(x) > 5):
            flag = True
            break
    return flag


""" Converting function to UDF """
valStrListUDF = udf(lambda z: valStrList(z))
