from cdm_utilities import *
from s3_utils import *
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.functions import lit, to_timestamp, col, to_date

# initialize spark context and sqlcontext
# sc = SparkContext(conf=SparkConf()
#                   # .set("spark.sql.crossJoin.enabled", True)
#                   .set("spark.driver.maxResultSize", "0")
#                   .set('spark.sql.autoBroadcastJoinThreshold', -1)
#                   .set('spark.sql.parquet.fs.optimized.committer.optimization-enabled', True)
#                   .set('spark.sql.shuffle.partitions', 300)
#                   ).getOrCreate()
# sqlContext = SQLContext(sc)
# sc.setLogLevel("Error")
#
# # spark.sql.crossJoin.enabled=true
#
# # Setting S3 max connection to 100
# hc = sc._jsc.hadoopConfiguration()
# hc.setInt("fs.s3a.connection.maximum", 100)
# hc.setInt("fs.s3.maxRetries", 20)
# hc.set("fs.s3a.multiobjectdelete.enable", "true")

# initialize script variables
format_length = 150


# function to get current date and time
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def overwrite_target(sqlContext, sc, src_path, tgt_path, inc_args, del_date, schema_overwrite='N'):
    src_df = read_parquet(sqlContext, src_path)
    if src_df is None:
        print(f"\toverwrite_target : could not load {src_path}")
        return {'status': "FAILURE", 'text': "No data in source path"}
    else:
        write_parquet(src_df, remove_slash(tgt_path) + "_tmp")
    return {'status': "SUCCESS", 'text': remove_slash(tgt_path) + "_tmp"}


def append_target(sqlContext, sc, src_path, tgt_path, inc_args, del_date, schema_overwrite='N'):
    src_df = read_parquet(sqlContext, src_path)
    if src_df is None:
        print(f"\tappend_target : could not load {src_path}")
        return {'status': "FAILURE", 'text': "No data in source path"}
    else:
        if s3_path_exists(tgt_path):
            print(f"\tappend_target : reading target data to append")
            tgt_df = read_parquet(sqlContext, tgt_path)
            src_col_li = get_all_columns_from_df_list([src_df])
            for c in src_col_li:
                if c not in tgt_df.columns:
                    tgt_df = tgt_df.withColumn(c, lit(None).cast(src_col_li[c]))
                else:
                    tgt_df = tgt_df.withColumn(c, col(c).cast(src_col_li[c]))

            sel_li = src_df.columns
            missing_cols = list(set(tgt_df.columns) - set(sel_li))
            if len(missing_cols) > 0 and schema_overwrite.upper() == 'N':
                raise Exception(f"columns {missing_cols} present in target but not present in source")

            print(f"\tAppend_target : Capturing unchanged records from target : ", tgt_df.count())
            if tgt_df is not None:
                tgt_df.select(sel_li).write.parquet(path=add_slash(src_path),
                                                    mode="Append",
                                                    compression="snappy")

        return {'status': "SUCCESS", 'text': src_path}


def deact_key_load(sqlContext, sc, src_path, tgt_path, deact_path, inc_args, del_date, schema_overwrite='N'):
    # read from ingestion temp path and load into source dataframe
    ing_tmp_path_full = src_path
    ing_path_full = tgt_path
    deact_path_full = deact_path

    src_df = read_parquet(sqlContext, ing_tmp_path_full)

    if src_df is None:
        print(f"\tdeact_key_load : could not load {ing_tmp_path_full}")
        return {'status': "FAILURE", 'text': "No data in source path"}

    # read from ingestion temp path and load into source deactive dataframe
    src_deactive_df = read_parquet(sqlContext, deact_path_full)
    if src_deactive_df is None:
        print(f"\tdeact_key_load : could not load {deact_path_full}; creating empty dataframe")
        src_deactive_df = sqlContext.createDataFrame(sc.emptyRDD(), src_df.schema)

    # read from ingestion path and load into target dataframe as lookup
    tgt_df = read_parquet(sqlContext, ing_path_full)

    # if temp path exist; clean-up
    if s3_path_exists(remove_slash(ing_path_full) + '_tmp'):
        buc1, key1 = s3_path_to_bucket_key(remove_slash(ing_path_full) + '_tmp')
        delete_s3_folder(buc1, key1)

    # if target df present apply incremental logic
    print(f"\tdeact_key_load : performing incremental load ")
    if tgt_df is not None:
        tgt_key_ls = inc_args["tgt_key"].split(",")
        src_key_ls = inc_args["src_key"].split(",")
        join_cnd = ''
        if len(tgt_key_ls) > 1:
            i = 1
            for key in tgt_key_ls:
                join_cnd = join_cnd + "upper(src.key" + str(i) + ") = upper(tgt." + key + ") and "
                i += 1
            # print(join_cnd)
            i = 1
            for key in src_key_ls:
                join_cnd = join_cnd.replace("key" + str(i), key)
                i += 1
            join_cnd = join_cnd[:-4]
            # print(join_cnd)
        else:
            join_cnd = join_cnd + " upper(src." + inc_args["src_key"] + ") = upper(tgt." + inc_args["tgt_key"] + ") "

        print(f"\tdeact_key_load : join condition : ", join_cnd)

        src_df.createOrReplaceTempView("src")
        src_deactive_df.createOrReplaceTempView("deact_src")
        tgt_df.createOrReplaceTempView("tgt")

        # distinct applied at the join as we are receiving duplicates in deactive file
        # Done by : shailesh Navendu 08/12/2021
        update_df1 = sqlContext.sql(
            "select distinct tgt.* from deact_src as src join tgt on " + join_cnd + " and tgt.is_active = 1 ")
        update_df1.createOrReplaceTempView("upd_vw")

        unchanged_df1 = subtract_dataframes(tgt_df, update_df1)
        unchanged_df1 = unchanged_df1.withColumn("is_incremental", lit(0))
        insert_df1 = sqlContext.sql("select src.* "
                                    "from (select * from src where is_active=1) src "
                                    "left join (select hashkey from tgt where is_active=1 except select hashkey from upd_vw) tgt "
                                    "on src.hashkey = tgt.hashkey "
                                    "where tgt.hashkey is null")

        src_col_li = get_all_columns_from_df_list([insert_df1])
        for c in src_col_li:
            if c not in unchanged_df1.columns:
                unchanged_df1 = unchanged_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df1 = unchanged_df1.withColumn(c, col(c).cast(src_col_li[c]))
            if c not in update_df1.columns:
                update_df1 = update_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                update_df1 = update_df1.withColumn(c, col(c).cast(src_col_li[c]))

        sel_li = insert_df1.columns
        missing_cols = list(set(tgt_df.columns) - set(sel_li))
        if len(missing_cols) > 0 and schema_overwrite.upper() == 'N':
            raise Exception(f"columns {missing_cols} present in target but not present in source")

        print(f"\tdeact_key_load : Capturing unchanged records b/w source and target : ", unchanged_df1.count())
        if unchanged_df1 is not None:
            unchanged_df1.select(sel_li).write.parquet(path=remove_slash(ing_path_full) + '_tmp',
                                                       mode="Append",
                                                       compression="snappy")

        update_df1 = update_df1.withColumn("delete_date", to_date(lit(del_date), "yyyyMMdd")) \
            .withColumn("last_modified_date", to_timestamp(lit(current_datetime()))) \
            .withColumn("is_active", lit(0))
        print(f"\tdeact_key_load : Capturing changed records b/w source and target : for update: ", update_df1.count())
        if update_df1 is not None:
            update_df1.select(sel_li).write.parquet(path=remove_slash(ing_path_full) + '_tmp',
                                                    mode="Append",
                                                    compression="snappy")

        print(f"\tdeact_key_load : Capturing new records from source: ", insert_df1.count())
        if insert_df1 is not None:
            insert_df1.select(sel_li).write.parquet(path=remove_slash(ing_path_full) + '_tmp',
                                                    mode="Append",
                                                    compression="snappy")

    else:
        src_df.write.parquet(path=remove_slash(ing_path_full) + '_tmp',
                             mode="Append",
                             compression="snappy")
    buc, key = s3_path_to_bucket_key(src_path)
    delete_s3_folder(buc, key)
    print(f"\tdeact_key_load : Process finished")
    return {'status': "SUCCESS", 'text': remove_slash(ing_path_full) + "_tmp"}


def conditional_load(sqlContext, sc, src_path, tgt_path, inc_args, del_date, schema_overwrite='N'):
    ing_tmp_path_full = src_path
    ing_path_full = tgt_path
    join_cond = inc_args["condn"]

    src_df = read_parquet(sqlContext, src_path)

    if src_df is None:
        print(f"\tconditional_load : could not load {src_path}")
        return {'status': "FAILURE", 'text': "No data in source path"}

    # read from ingestion path and load into target dataframe as lookup
    tgt_df = read_parquet(sqlContext, ing_path_full)

    if tgt_df is not None:
        print(f"\tconditional_load : performing incremental load ")
        tgt_df.createOrReplaceTempView("tgt")

        update_df1 = sqlContext.sql("select tgt.* from tgt where " + join_cond + " and is_active = 1")
        update_df1 = update_df1.withColumn("delete_date", to_date(lit(del_date), "yyyyMMdd")) \
            .withColumn("last_modified_date", to_timestamp(lit(current_datetime()))) \
            .withColumn("is_active", lit(0)) \
            .withColumn("is_incremental", lit(0))

        unchanged_df1 = sqlContext.sql("select tgt.* from tgt where !(" + join_cond + ") and is_active = 1")
        unchanged_df1 = unchanged_df1.withColumn("is_incremental", lit(0))

        unchanged_df2 = sqlContext.sql("select tgt.* from tgt where tgt.is_active = 0")
        unchanged_df2 = unchanged_df2.withColumn("is_incremental", lit(0))

        src_col_li = get_all_columns_from_df_list([src_df])
        sel_li = src_df.columns
        missing_cols = list(set(tgt_df.columns) - set(sel_li))
        if len(missing_cols) > 0 and schema_overwrite.upper() == 'N':
            raise Exception(f"columns {missing_cols} present in target but not present in source")

        for c in src_col_li:
            if c not in unchanged_df1.columns:
                unchanged_df1 = unchanged_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df1 = unchanged_df1.withColumn(c, col(c).cast(src_col_li[c]))
            if c not in unchanged_df2.columns:
                unchanged_df2 = unchanged_df2.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df2 = unchanged_df2.withColumn(c, col(c).cast(src_col_li[c]))
            if c not in update_df1.columns:
                update_df1 = update_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                update_df1 = update_df1.withColumn(c, col(c).cast(src_col_li[c]))

        unch_1_cnt = unchanged_df1.count()
        print(f"\tconditional_load : Capturing unchanged records b/w source and target : ", unch_1_cnt)
        unchanged_df1.select(sel_li).coalesce(1200).write.parquet(path=ing_tmp_path_full,
                                                                  mode="Append",
                                                                  compression="snappy")
        unch_2_cnt = unchanged_df2.count()
        print(f"\tconditional_load : Capturing inactive records from target: ", unch_2_cnt)
        unchanged_df2.select(sel_li).coalesce(600).write.parquet(path=ing_tmp_path_full,
                                                                 mode="Append",
                                                                 compression="snappy")
        upd_cnt = update_df1.count()
        print(f"\tconditional_load : Capturing changed records b/w source and target : for update: ", upd_cnt)
        update_df1.select(sel_li).coalesce(600).write.parquet(path=ing_tmp_path_full,
                                                              mode="Append",
                                                              compression="snappy")

    return {'status': "SUCCESS", 'text': remove_slash(ing_tmp_path_full)}


def all_column_checksum_load(sqlContext, sc, src_path, tgt_path, inc_args, del_date, schema_overwrite='N'):
    ing_tmp_path_full = src_path
    ing_path_full = tgt_path

    # read from ingestion temp path and load into source dataframe
    src_df = read_parquet(sqlContext, ing_tmp_path_full)

    if src_df is None:
        print(f"\tall_column_checksum_load : could not load {ing_tmp_path_full}")
        return {'status': "FAILURE", 'text': "No data in source path"}

    # read from ingestion path and load into target dataframe as lookup
    tgt_df = read_parquet(sqlContext, ing_path_full)
    src_df.createOrReplaceTempView("src")

    print(f"\tall_column_checksum_load : performing incremental load ")

    if s3_path_exists(remove_slash(ing_path_full) + '_tmp'):
        buc1, key1 = s3_path_to_bucket_key(remove_slash(ing_path_full) + '_tmp')
        delete_s3_folder(buc1, key1)

    if tgt_df is not None:
        tgt_df.createOrReplaceTempView("tgt")

        unchanged_df1 = sqlContext.sql(
            "select tgt.* from src join tgt on src.hashkey = tgt.hashkey and tgt.is_active = 1 ")
        unchanged_df1 = unchanged_df1.withColumn("is_incremental", lit(0))

        unchanged_df2 = sqlContext.sql("select tgt.* from tgt where tgt.is_active = 0")
        unchanged_df2 = unchanged_df2.withColumn("is_incremental", lit(0))

        update_df1 = sqlContext.sql(
            "select tgt.* from (select * from tgt where is_active=1) tgt left join src on src.hashkey = tgt.hashkey where src.hashkey is null ")
        update_df1 = update_df1.withColumn("delete_date", to_date(lit(del_date), "yyyyMMdd")) \
            .withColumn("last_modified_date", to_timestamp(lit(current_datetime()))) \
            .withColumn("is_incremental", lit(1)) \
            .withColumn("is_active", lit(0))

        insert_df1 = sqlContext.sql(
            "select src.* from src left join (select * from tgt where is_active=1) tgt on src.hashkey = tgt.hashkey where tgt.hashkey is null ")

        src_col_li = get_all_columns_from_df_list([insert_df1])
        sel_li = insert_df1.columns
        missing_cols = list(set(tgt_df.columns) - set(sel_li))
        if len(missing_cols) > 0 and schema_overwrite.upper() == 'N':
            raise Exception(f"columns {missing_cols} present in target but not present in source")

        for c in src_col_li:
            if c not in unchanged_df1.columns:
                unchanged_df1 = unchanged_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df1 = unchanged_df1.withColumn(c, col(c).cast(src_col_li[c]))
            if c not in update_df1.columns:
                update_df1 = update_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                update_df1 = update_df1.withColumn(c, col(c).cast(src_col_li[c]))
            if c not in unchanged_df2.columns:
                unchanged_df2 = unchanged_df2.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df2 = unchanged_df2.withColumn(c, col(c).cast(src_col_li[c]))

        print(f"\tall_column_checksum_load : Capturing changed records b/w source and target : for update: ",
              update_df1.count())
        update_df1.select(sel_li).write.parquet(path=remove_slash(ing_path_full) + '_tmp',
                                                mode="Append",
                                                compression="snappy")
        print(f"\tall_column_checksum_load : new records for insert: ", insert_df1.count())
        insert_df1.select(sel_li).write.parquet(path=remove_slash(ing_path_full) + '_tmp',
                                                mode="Append",
                                                compression="snappy")
        print(f"\tall_column_checksum_load : Capturing unchanged records b/w source and target : ",
              unchanged_df1.count())
        unchanged_df1.select(sel_li).write.parquet(path=remove_slash(ing_path_full) + '_tmp',
                                                   mode="Append",
                                                   compression="snappy")
        print(f"\tall_column_checksum_load : Capturing inactive records from target: ", unchanged_df2.count())
        unchanged_df2.select(sel_li).write.parquet(path=remove_slash(ing_path_full) + '_tmp',
                                                   mode="Append",
                                                   compression="snappy")
    else:
        src_df.coalesce(200).write.parquet(path=remove_slash(ing_path_full) + '_tmp',
                                           mode="Append",
                                           compression="snappy")

    buc, key = s3_path_to_bucket_key(src_path)
    delete_s3_folder(buc, key)
    return {'status': "SUCCESS", 'text': remove_slash(ing_path_full) + '_tmp'}


def based_on_key_load(sqlContext, sc, src_path, tgt_path, inc_args, del_date, schema_overwrite='N'):
    ing_tmp_path_full = remove_slash(tgt_path) + '_tmp'
    ing_path_full = tgt_path
    primary_key_column = inc_args["key_column"]

    # read from ingestion temp path and load into source dataframe
    src_df = read_parquet(sqlContext, src_path)

    if src_df is None:
        print(f"\tbased_on_key_load : could not load {ing_tmp_path_full}")
        return {'status': "FAILURE", 'text': "No data in source path"}

    # read from ingestion path and load into target dataframe as lookup
    tgt_df = read_parquet(sqlContext, ing_path_full)
    src_df.createOrReplaceTempView("src")

    spark_src_deactive_df = sqlContext.sql("select distinct " + primary_key_column + " from src ")

    # write deactivedkeys dataframe into ingestion temp path
    if spark_src_deactive_df is None:
        print(f"\tbased_on_key_load : No data for Source file hence creating empty dataframe")
        spark_src_deactive_df = sqlContext.createDataFrame(sc.emptyRDD(), src_df.schema)

    spark_src_deactive_df.createOrReplaceTempView("deact_src")
    if s3_path_exists(remove_slash(tgt_path) + '_tmp'):
        buc1, key1 = s3_path_to_bucket_key(remove_slash(tgt_path) + '_tmp')
        delete_s3_folder(buc1, key1)

    print(f"\tbased_on_key_load : performing incremental load ")
    if tgt_df is not None:
        key_args = inc_args["key_column"].split(",")
        # print(f"\tkey_args : {key_args}")
        join_cnd = ""
        if len(key_args) > 1:
            for key in key_args:
                join_cnd = join_cnd + " src." + key + " = tgt." + key + " and "
            join_cnd = join_cnd[:-4]
        else:
            join_cnd = join_cnd + " src." + inc_args["key_column"] + " = tgt." + inc_args["key_column"] + " "

        print(f"\tjbased_on_key_load : oin condition : ", join_cnd)
        tgt_df.createOrReplaceTempView("tgt")

        # distinct applied at the join as we are receiving duplicates in deactive file
        # Done by : shailesh Navendu 08/12/2021
        update_df1 = sqlContext.sql("select distinct tgt.* from deact_src as src join tgt "
                                    "on " + join_cnd + " and tgt.is_active = 1 ")

        unchanged_df1 = subtract_dataframes(tgt_df, update_df1)
        unchanged_df1 = unchanged_df1.withColumn("is_incremental", lit(0))

        update_df1 = update_df1.withColumn("delete_date", to_date(lit(del_date), "yyyyMMdd")) \
            .withColumn("last_modified_date", to_timestamp(lit(current_datetime()))) \
            .withColumn("is_active", lit(0)) \
            .withColumn("is_incremental", lit(1))

        insert_df1 = sqlContext.sql("select src.* from src where is_active = 1")

        print(f"\tbased_on_key_load : Combining all datasets")
        src_col_li = get_all_columns_from_df_list([insert_df1])
        sel_li = insert_df1.columns
        missing_cols = list(set(tgt_df.columns) - set(sel_li))
        if len(missing_cols) > 0 and schema_overwrite.upper() == 'N':
            raise Exception(f"columns {missing_cols} present in target but not present in source")

        for c in src_col_li:
            if c not in unchanged_df1.columns:
                unchanged_df1 = unchanged_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df1 = unchanged_df1.withColumn(c, col(c).cast(src_col_li[c]))
            if c not in update_df1.columns:
                update_df1 = update_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                update_df1 = update_df1.withColumn(c, col(c).cast(src_col_li[c]))

        if unchanged_df1 is not None:
            print(f"\tbased_on_key_load : Capturing unchanged records b/w source and target : ", unchanged_df1.count())
            unchanged_df1.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                       mode="Append",
                                                       compression="snappy")
        if update_df1 is not None:
            print(f"\tbased_on_key_load : Capturing changed records b/w source and target : for update: ",
                  update_df1.count())
            update_df1.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                    mode="Append",
                                                    compression="snappy")
        if insert_df1 is not None:
            print(f"\tbased_on_key_load : Capturing new records from source: ", insert_df1.count())
            insert_df1.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                    mode="Append",
                                                    compression="snappy")

        print(f"\tbased_on_key_load : Load completed successfully")

    else:
        if src_df is not None:
            src_df.write.parquet(path=ing_tmp_path_full,
                                 mode="Append",
                                 compression="snappy")

        print(f"\tbased_on_key_load : Load completed successfully")
    buc, key = s3_path_to_bucket_key(src_path)
    delete_s3_folder(buc, key)
    return {'status': "SUCCESS", 'text': ing_tmp_path_full}


def except_key_column_checksum_load(sqlContext, sc, src_path, tgt_path, inc_args, del_date, schema_overwrite='N'):
    ing_tmp_path_full = remove_slash(tgt_path) + '_tmp'
    src_df = read_parquet(sqlContext, src_path)

    if src_df is None:
        print(f"\texcept_key_column_checksum_load : could not load {src_path}")
        return {'status': "FAILURE", 'text': "No data in source path"}
    tgt_df = read_parquet(sqlContext, tgt_path)
    join_cnd = inc_args

    if s3_path_exists(ing_tmp_path_full):
        buc1, key1 = s3_path_to_bucket_key(ing_tmp_path_full)
        delete_s3_folder(buc1, key1)

    if tgt_df is not None:

        src_df.createOrReplaceTempView("src")
        tgt_df.createOrReplaceTempView("tgt")

        print(f"\texcept_key_column_checksum_load : join condition : ", join_cnd)

        unchanged_df1 = sqlContext.sql("select tgt.* from src join tgt "
                                       " on " + join_cnd + " and src.is_active = tgt.is_active "
                                                           "and src.hashkey = tgt.hashkey")

        unchanged_df2 = sqlContext.sql("select tgt.* from tgt where tgt.is_active = 0")

        insert_df1 = sqlContext.sql("select src.* from src join tgt "
                                    " on " + join_cnd + " and src.is_active = tgt.is_active "
                                                        "and src.hashkey != tgt.hashkey")

        insert_df2 = sqlContext.sql("select src.* from src left join (select * from tgt where is_active=1) tgt "
                                    " on " + join_cnd + " where tgt.hashkey is null")

        update_df1 = sqlContext.sql("select tgt.* from src join tgt "
                                    " on " + join_cnd + " and src.is_active = tgt.is_active "
                                                        "and src.hashkey != tgt.hashkey")
        update_df1 = update_df1.withColumn("delete_date", to_date(lit(del_date), "yyyyMMdd")) \
            .withColumn("last_modified_date", to_timestamp(lit(current_datetime()))) \
            .withColumn("is_active", lit(0))

        update_df2 = sqlContext.sql("select tgt.* from src right join (select * from tgt where is_active=1) tgt "
                                    " on " + join_cnd + " where src.hashkey is null")
        update_df2 = update_df2.withColumn("delete_date", to_date(lit(del_date), "yyyyMMdd")) \
            .withColumn("last_modified_date", to_timestamp(lit(current_datetime()))) \
            .withColumn("is_active", lit(0))

        src_col_li = get_all_columns_from_df_list([insert_df1])
        sel_li = insert_df1.columns
        missing_cols = list(set(tgt_df.columns) - set(sel_li))
        if len(missing_cols) > 0 and schema_overwrite.upper() == 'N':
            raise Exception(f"columns {missing_cols} present in target but not present in source")

        for c in src_col_li:
            if c not in unchanged_df1.columns:
                unchanged_df1 = unchanged_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df1 = unchanged_df1.withColumn(c, col(c).cast(src_col_li[c]))
            if c not in update_df1.columns:
                update_df1 = update_df1.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                update_df1 = update_df1.withColumn(c, col(c).cast(src_col_li[c]))
            if c not in unchanged_df2.columns:
                unchanged_df2 = unchanged_df2.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df2 = unchanged_df2.withColumn(c, col(c).cast(src_col_li[c]))
            if c not in update_df2.columns:
                update_df2 = update_df2.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                update_df2 = update_df2.withColumn(c, col(c).cast(src_col_li[c]))

        if unchanged_df1 is not None:
            print(f"\texcept_key_column_checksum_load : Capturing unchanged records b/w source and target : ",
                  unchanged_df1.count())
            unchanged_df1.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                       mode="Append",
                                                       compression="snappy")

        if unchanged_df2 is not None:
            print(f"\texcept_key_column_checksum_load : Capturing inactive records from target: ",
                  unchanged_df2.count())
            unchanged_df2.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                       mode="Append",
                                                       compression="snappy")

        if update_df1 is not None:
            print(f"\texcept_key_column_checksum_load : Capturing changed records b/w source and target : for update: ",
                  update_df1.count())
            update_df1.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                    mode="Append",
                                                    compression="snappy")

        if update_df2 is not None:
            print(f"\texcept_key_column_checksum_load : Capturing deleted records from target: ", update_df2.count())
            update_df2.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                    mode="Append",
                                                    compression="snappy")

        if insert_df1 is not None:
            print(f"\texcept_key_column_checksum_load : Capturing changed records b/w source and target : for insert: ",
                  insert_df1.count())
            insert_df1.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                    mode="Append",
                                                    compression="snappy")

        if insert_df2 is not None:
            print(f"\texcept_key_column_checksum_load : Capturing new records from source: ", insert_df2.count())
            insert_df2.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                    mode="Append",
                                                    compression="snappy")
    else:
        if src_df is not None:
            src_df.write.parquet(path=ing_tmp_path_full,
                                 mode="Append",
                                 compression="snappy")

    buc, key = s3_path_to_bucket_key(src_path)
    delete_s3_folder(buc, key)
    return {'status': "SUCCESS", 'text': ing_tmp_path_full}


def based_on_key_and_date_load(sqlContext, sc, src_path, tgt_path, inc_args, del_date, schema_overwrite='N'):
    ing_tmp_path_full = remove_slash(tgt_path) + '_tmp'
    ing_path_full = tgt_path
    key_column = inc_args["key_column"]
    date_column = inc_args["date_column"]
    src_df = read_parquet(sqlContext, src_path)

    if src_df is None:
        print(f"\tbased_on_key_load : could not load {ing_tmp_path_full}")
        return {'status': "FAILURE", 'text': "No data in source path"}

    tgt_df = read_parquet(sqlContext, ing_path_full)
    if tgt_df is not None:
        join_logic = ''
        count = 0
        for key in key_column.split(','):
            if count == 0:
                join_logic = f'src.{key} = tgt.{key}'
                count = 1
            else:
                join_logic += f' and src.{key} = tgt.{key}'
        src_df.createOrReplaceTempView("src")
        tgt_df.createOrReplaceTempView("tgt")

        insert_df1 = sqlContext.sql(
            f"select src.* from src left join (select distinct {key_column}, {date_column}, hashkey from tgt where is_active=1) tgt on {join_logic} where tgt.hashkey is null")

        update_df = sqlContext.sql(
            f"select tgt.* from (select * from tgt where is_active=1) tgt join (select distinct {key_column}, {date_column}, hashkey from src) as src on {join_logic} where tgt.{date_column} <= src.{date_column} and tgt.hashkey <> src.hashkey ")
        unchanged_df = subtract_dataframes(tgt_df, update_df)
        unchanged_df = unchanged_df.withColumn("is_incremental", lit(0))

        update_df = update_df.withColumn("is_incremental", lit(0)).withColumn("is_active", lit(0)).withColumn(
            "last_modified_date", to_timestamp(lit(current_datetime()))).withColumn("delete_date",
                                                                                    to_date(lit(del_date), "yyyyMMdd"))

        insert_df2 = sqlContext.sql(
            f"select src.* from (select distinct {key_column}, {date_column}, hashkey from tgt where is_active=1) tgt join src on {join_logic} where tgt.{date_column} <= src.{date_column} and tgt.hashkey <> src.hashkey ")

        src_col_li = get_all_columns_from_df_list([insert_df1])
        sel_li = insert_df1.columns
        missing_cols = list(set(tgt_df.columns) - set(sel_li))
        if len(missing_cols) > 0 and schema_overwrite.upper() == 'N':
            raise Exception(f"columns {missing_cols} present in target but not present in source")

        for c in src_col_li:
            if c not in unchanged_df.columns:
                unchanged_df = unchanged_df.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df = unchanged_df.withColumn(c, col(c).cast(src_col_li[c]))

            if c not in update_df.columns:
                update_df = update_df.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                update_df = update_df.withColumn(c, col(c).cast(src_col_li[c]))

        if unchanged_df is not None:
            print(f"\tbased_on_key_and_date_load : Capturing unchanged records b/w source and target : ",
                  unchanged_df.count())
            unchanged_df.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                      mode="Append",
                                                      compression="snappy")
        if update_df is not None:
            print(f"\tbased_on_key_and_date_load : Capturing changed records b/w source and target : for update: ",
                  update_df.count())
            update_df.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                   mode="Append",
                                                   compression="snappy")
        if insert_df1 is not None:
            print(f"\tbased_on_key_and_date_load : Capturing new records from target: ", insert_df1.count())
            insert_df1.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                    mode="Append",
                                                    compression="snappy")
        if insert_df2 is not None:
            print(f"\tbased_on_key_and_date_load : Capturing changed records b/w source and target : for insert: ",
                  insert_df2.count())
            insert_df2.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                    mode="Append",
                                                    compression="snappy")
        buc, key = s3_path_to_bucket_key(src_path)
        delete_s3_folder(buc, key)
    else:
        return {'status': "SUCCESS", 'text': src_path}
    return {'status': "SUCCESS", 'text': ing_tmp_path_full}


def based_on_multi_key_and_date_load(sqlContext, sc, src_path, tgt_path, inc_args, del_date, schema_overwrite='N'):
    ing_tmp_path_full = remove_slash(tgt_path) + '_tmp'
    ing_path_full = tgt_path
    key_column_raw = inc_args["key_column"]
    key_column = [k.strip() for k in inc_args["key_column"].split(",")]
    date_column = inc_args["date_column"]
    src_df = read_parquet(sqlContext, src_path)

    if src_df is None:
        print(f"\tbased_on_multi_key_and_date_load : could not load {ing_tmp_path_full}")
        return {'status': "FAILURE", 'text': "No data in source path"}

    tgt_df = read_parquet(sqlContext, ing_path_full)
    if tgt_df is not None:

        join_conds = {"insert": {"join": [], "where1": [], "where2": [], "where3": []},
                      "update": {"join": [], "where1": [], "where2": []}}

        src_df.createOrReplaceTempView("src")
        tgt_df.createOrReplaceTempView("tgt")

        for k in key_column:
            join_conds["insert"]["join"] += [
                f"left join(select distinct {key_column_raw}, {date_column}, hashkey from tgt where is_active=1) tgt_{k} on src.{k} = tgt_{k}.{k} "]
            join_conds["insert"]["where1"] += [f"tgt_{k}.hashkey is null "]
            join_conds["insert"]["where2"] += [f"tgt_{k}.hashkey"]
            join_conds["insert"]["where3"] += [f"tgt_{k}.{date_column} "]

            join_conds["update"]["join"] += [
                f"left join (select distinct {key_column_raw}, {date_column}, hashkey from src) as src_{k} on src_{k}.{k} = tgt.{k} "]
            join_conds["update"]["where1"] += [f"src_{k}.{date_column} "]
            join_conds["update"]["where2"] += [f"src_{k}.hashkey"]

        insert_sql = (f"select src.* "
                      f"from src "
                      f"{' '.join(join_conds['insert']['join'])} "
                      f"where (({' and '.join(join_conds['insert']['where1'])}) "
                      f"or (nvl({', '.join(join_conds['insert']['where2'])}) != src.hashkey "
                      f"and nvl({', '.join(join_conds['insert']['where3'])}) <= src.{date_column}))")
        print(insert_sql)
        insert_df1 = sqlContext.sql(insert_sql)

        update_sql = (f"select tgt.* "
                      f"from (select * from tgt where is_active=1) tgt "
                      f"{' '.join(join_conds['update']['join'])} "
                      f"where "
                      f"tgt.{date_column} <= nvl({', '.join(join_conds['update']['where1'])}) "
                      f"and tgt.hashkey <> nvl({', '.join(join_conds['update']['where2'])})")
        print(update_sql)
        update_df = sqlContext.sql(update_sql)
        unchanged_df = subtract_dataframes(tgt_df, update_df)
        unchanged_df = unchanged_df.withColumn("is_incremental", lit(0))

        update_df = update_df.withColumn("is_incremental", lit(0)).withColumn("is_active", lit(0)) \
            .withColumn("last_modified_date", to_timestamp(lit(current_datetime()))) \
            .withColumn("delete_date", to_date(lit(del_date), "yyyyMMdd"))

        src_col_li = get_all_columns_from_df_list([insert_df1])
        sel_li = insert_df1.columns
        missing_cols = list(set(tgt_df.columns) - set(sel_li))
        if len(missing_cols) > 0 and schema_overwrite.upper() == 'N':
            raise Exception(f"columns {missing_cols} present in target but not present in source")

        for c in src_col_li:
            if c not in unchanged_df.columns:
                unchanged_df = unchanged_df.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                unchanged_df = unchanged_df.withColumn(c, col(c).cast(src_col_li[c]))

            if c not in update_df.columns:
                update_df = update_df.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                update_df = update_df.withColumn(c, col(c).cast(src_col_li[c]))

        if unchanged_df is not None:
            print(f"\tbased_on_multi_key_and_date_load : Capturing unchanged records b/w source and target : ",
                  unchanged_df.count())
            unchanged_df.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                      mode="Append",
                                                      compression="snappy")
        if update_df is not None:
            print(
                f"\tbased_on_multi_key_and_date_load : Capturing changed records b/w source and target : for update: ",
                update_df.count())
            update_df.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                   mode="Append",
                                                   compression="snappy")
        if insert_df1 is not None:
            print(f"\tbased_on_multi_key_and_date_load : Capturing changed and new records from target: ",
                  insert_df1.count())
            insert_df1.select(sel_li).write.parquet(path=ing_tmp_path_full,
                                                    mode="Append",
                                                    compression="snappy")

        buc, key = s3_path_to_bucket_key(src_path)
        delete_s3_folder(buc, key)
    else:
        return {'status': "SUCCESS", 'text': src_path}
    return {'status': "SUCCESS", 'text': ing_tmp_path_full}


def append_based_on_key_load(sqlContext, sc, src_path, tgt_path, inc_args, del_date, schema_overwrite='N'):
    # read from ingestion temp path and load into source dataframe
    src_df = read_parquet(sqlContext, src_path)

    if src_df is None:
        print(f"\tappend_based_on_key_load : could not load {src_path}")
        return {'status': "FAILURE", 'text': "No data in source path"}

    # read from ingestion path and load into target dataframe as lookup
    tgt_df = read_parquet(sqlContext, tgt_path)
    src_df.createOrReplaceTempView("src")

    print(f"\tappend_based_on_key_load : performing incremental load ")

    if s3_path_exists(remove_slash(tgt_path) + '_tmp'):
        buc1, key1 = s3_path_to_bucket_key(remove_slash(tgt_path) + '_tmp')
        delete_s3_folder(buc1, key1)

    if tgt_df is not None:
        tgt_df.createOrReplaceTempView("tgt")

        tgt_key_ls = inc_args["key_column"].split(",")
        join_cnd = ''
        if len(tgt_key_ls) > 1:
            for key in tgt_key_ls:
                join_cnd = join_cnd + " src." + key + " = tgt." + key + " and "
            join_cnd = join_cnd[:-4]
        else:
            join_cnd = join_cnd + " src." + inc_args["key_column"] + " = tgt." + inc_args["key_column"] + " "

        insert_df1_sql = "select src.* from src left join (select * from tgt where is_active=1) tgt on " + join_cnd + \
                         " where tgt.is_active is null "
        print(f"insert_df1_sql :: {insert_df1_sql}")
        insert_df1 = sqlContext.sql(insert_df1_sql)

        src_col_li = get_all_columns_from_df_list([insert_df1])
        sel_li = insert_df1.columns
        missing_cols = list(set(tgt_df.columns) - set(sel_li))
        if len(missing_cols) > 0 and schema_overwrite.upper() == 'N':
            raise Exception(f"columns {missing_cols} present in target but not present in source")

        for c in src_col_li:
            if c not in tgt_df.columns:
                tgt_df = tgt_df.withColumn(c, lit(None).cast(src_col_li[c]))
            else:
                tgt_df = tgt_df.withColumn(c, col(c).cast(src_col_li[c]))

        print(f"\tappend_based_on_key_load : new records for insert: ", insert_df1.count())
        insert_df1.select(sel_li).write.parquet(path=remove_slash(tgt_path) + '_tmp',
                                                mode="Append",
                                                compression="snappy")
        print(f"\tappend_based_on_key_load : Capturing unchanged records b/w source and target : ",
              tgt_df.count())
        tgt_df.select(sel_li).write.parquet(path=remove_slash(tgt_path) + '_tmp',
                                            mode="Append",
                                            compression="snappy")

    else:
        src_df.coalesce(200).write.parquet(path=remove_slash(tgt_path) + '_tmp',
                                           mode="Append",
                                           compression="snappy")

    buc, key = s3_path_to_bucket_key(src_path)
    delete_s3_folder(buc, key)
    return {'status': "SUCCESS", 'text': remove_slash(tgt_path) + '_tmp'}
