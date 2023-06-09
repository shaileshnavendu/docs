# built-in libraries
import sys
import time

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import rand, ascii

# user defined libraries
from cdm_utilities import *

# initialize spark context and sqlcontext
sc = SparkContext.getOrCreate()
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


def get_credentials_from_sm(secret_name, region_name):
    if secret_name is not None and region_name is not None:
        secrets_client = boto3.client('secretsmanager', region_name=region_name)
        db_credentials = secrets_client.get_secret_value(SecretId=secret_name)
        return db_credentials
    else:
        raise Exception(f"invalid secret name '{secret_name}' or region '{region_name}'")


def transform(df):
    for k, v in oth_cols.items():
        print(f"{current_datetime()} :: starting for col {k}")
        function_name = v.split('#')[0]
        # getting max cai_patient_de_id of target data
        try:
            query = f"select max({k}) + 1 as max_id from {tgt_schema}.{table_name}"
            temp_df = read_redshift_query(sqlContext, redshift_driver, url, s3_temp_dir, redshift_role, query)
            seed = int(temp_df.first()[0]) if not temp_df.rdd.isEmpty() and temp_df.first()[0] is not None else None
        except Exception:
            seed = None

        if seed is None:
            if v.split('#')[1] != '':
                func_arg = json.loads(v.split('#')[1])
                print(func_arg)
                if 'seed' in func_arg:
                    seed = int(func_arg['seed'])
                else:
                    seed = 0
            else:
                seed = 0
        print(f"{current_datetime()} :: Starting de id for column ", k, " seed is ", seed)
        # df = df.withColumn('cai_patient_id_2', concat(col('cai_patient_id'), lit('_cai')))
        # pk_list.append('cai_patient_id_2')
        # df.show(5, False)
        print(f"{current_datetime()} :: pk_list:", pk_list)
        df = df.withColumn('sum', sum(ascii(df[col]) for col in pk_list))
        df = df.withColumn('key', rand(42) * col('sum'))
        key = 'key'
        df = df.withColumn(k, eval(f"{function_name}(key,seed)"))
        # df.show(5, False)
        df = df.drop('sum', 'key')
        print(f"{current_datetime()} :: transform :: {k} :: intermediate write - 2")
        df.write.parquet(remove_slash(temp_path) + f"_int_{k}/", mode="Overwrite")
        df = read_parquet(sqlContext, remove_slash(temp_path) + f"_int_{k}/")
    df = df.withColumn("insert_ts", lit(datetime.now()))
    return df


def add_increment_id_to_null_columns(unioned_df):
    for k, v in oth_cols.items():
        unioned_df_null = unioned_df.filter(f"{k} is null")
        if unioned_df_null.rdd.isEmpty():
            print(f"{current_datetime()} :: No nulls in column : {k}")
            continue
        else:
            print(f"{current_datetime()} :: null values present in column : {k}")
            unioned_df_not_null = unioned_df.filter(f"{k} is not null")
            seed = int(unioned_df_not_null.agg({k: "max"}).collect()[0][f"max({k})"])
            print(f"{current_datetime()} :: reloading seed for column ", k, " is ", seed)
            function_name = v.split('#')[0]
            unioned_df_null = unioned_df_null.withColumn(k, eval(f"{function_name}(pk_list,seed)"))
            unioned_df = unioned_df_null.union(unioned_df_not_null)
    return unioned_df


def read_and_load_from_s3_to_redshift():
    print(f"{current_datetime()} :: reading data from {temp_path}")
    final_df = read_parquet(sqlContext, temp_path)
    print(f"{current_datetime()} :: writing data to db")
    write_to_redshift(sqlContext, final_df, redshift_driver, url, tgt_schema, table_name, s3_temp_dir, redshift_role,
                      "Overwrite")
    time.sleep(10)


# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'TABLE_NAME', 'TGT_NAME', 'JOB_NAME'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    table_name = args['TABLE_NAME']
    tgt_name = args['TGT_NAME']
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
    print(f"{current_datetime()} :: main :: info - src_name         : {tgt_name}")
    print(f"{current_datetime()} :: main :: info - job_name         : {job_name}")
    print(f"{current_datetime()} :: main :: info - job_run_id       : {job_run_id}")
print("*" * format_length)

# parse the config file contents
print(f"\n{current_datetime()} :: main :: info - reading the config file {config_file} in bucket {bucket} ...\n")
try:
    param_data = get_s3_object(bucket, config_file)
    param_contents = json.loads(param_data)
    mapping_file = param_contents["ing_mapping"]

    root_path = param_contents["root_path"]
    cai_base_dir = param_contents["cai_base_dir"]
    data_folder_suffix = param_contents['data_folder_suffix']
    source_path_wo_prefix = root_path + add_slash(cai_base_dir) + add_slash(table_name)
    tmp_path_wo_prefix = root_path + add_slash(data_folder_suffix) + add_slash("lookup_temp")
    source_path = bucket_key_to_s3_path(bucket, source_path_wo_prefix)
    temp_path = bucket_key_to_s3_path(bucket, tmp_path_wo_prefix)

    tgt_schema = param_contents['extracts_lookup_schema']

    redshift_config = param_contents["s3_to_redshiftdb"]
    secret_manager = redshift_config["ingestion_lookup"]['secret_manager_db']
    # client = redshift_config['client']
    aws_region = redshift_config['aws_region']
    s3_temp_dir = redshift_config['s3_temp_dir']
    redshift_driver = redshift_config['redshift_driver']
    redshift_db = redshift_config["ingestion_lookup"]['redshift_db']
    redshift_role = redshift_config["ingestion_lookup"]['redshift_role']

except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(
        f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")
    print(f"{current_datetime()} :: main :: info - mapping_file         : {mapping_file}")
    print(f"{current_datetime()} :: main :: info - source_path          : {source_path}")
    print(f"{current_datetime()} :: main :: info - target schema        : {tgt_schema}")
print("*" * format_length)

print(f"\n{current_datetime()} :: main :: info - fetching secret manager credentials ...")
try:
    db_credentials = get_credentials_from_sm(secret_manager, aws_region)
    json_creds = json.loads(db_credentials["SecretString"])

    redshift_host = json_creds['host'].lstrip()
    redshift_port = json_creds['port']
    redshift_user = json_creds['user']
    redshift_password = json_creds['password']
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to fetch secret manager credentials")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully fetched secret manager credentials\n")
    print(f"{current_datetime()} :: main :: info - redshift_host	: '{redshift_host}'")
    print(f"{current_datetime()} :: main :: info - redshift_port	: '{redshift_port}'")
    print(f"{current_datetime()} :: main :: info - redshift_db	    : '{redshift_db}'")

url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}?user={redshift_user}&password={redshift_password}"

if s3_path_exists(temp_path):
    print(f"{current_datetime()} :: Files already present in s3 location {temp_path}. Loading to redshift")
    read_and_load_from_s3_to_redshift()
    print(f"{current_datetime()} :: Deleting temp location {temp_path}")
    delete_s3_folder_spark(sqlContext, temp_path)
else:
    # Read mapping file and filter for current table
    print(f"{current_datetime()} :: read mapping from {mapping_file}")
    mapping_df = read_mapping(mapping_file, table_name, tgt_name)
    # print(mapping_df)
    if mapping_df.empty:
        raise Exception(f"mapping not available for cdm_table={table_name} and tgt_name containing {tgt_name}")

    print(f"{current_datetime()} :: get source mapping")
    base_map_df = mapping_df[(mapping_df.src_table != '') |
                             (mapping_df.src_column != '') |
                             (mapping_df.sql_tx != '')]
    # print(base_map_df)
    base_sql = build_src_sql(base_map_df)

    print(f"{current_datetime()} :: base sql is - \n{base_sql}")
    # print("get function mapping")
    oth_map_df = mapping_df[~((mapping_df.src_table != '') |
                              (mapping_df.src_column != '') |
                              (mapping_df.sql_tx != '')) & (mapping_df.fn_tx != '')]
    print(oth_map_df)

    print(f"{current_datetime()} :: apply function mapping")
    oth_cols = dict(zip(oth_map_df.cdm_column, oth_map_df.fn_tx + "#" + oth_map_df.fn_arg))
    pk_list = read_primary_keys(mapping_df)
    print(f"{current_datetime()} :: oth_cols:", oth_cols)
    # identify partitions
    # part_li = list_s3_subfolders_wo_path(bucket, source_path_wo_prefix)
    part_li = []
    if part_li:
        print(f"{current_datetime()} :: partitions are - \n", '\n'.join(part_li))
    else:
        print(f"{current_datetime()} :: no partitions found under {source_path_wo_prefix}")
        part_li = ['']

    for part in part_li:
        print(f"{current_datetime()} :: start - {part}")
        if part == '':
            print(f"{current_datetime()} :: read source data into dataframe from - {source_path}")
            src_df = read_parquet(sqlContext, source_path)
        else:
            part_key = part.split("=")[0]
            part_val = part.split("=")[-1]
            full_src_path = add_slash(source_path) + add_slash(part)
            print(f"{current_datetime()} :: read source data into dataframe from - {full_src_path}")
            src_df = read_parquet(sqlContext, full_src_path)
            src_df = src_df.withColumn(part_key, lit(part_val))

        # read source data into dataframe
        print(f"{current_datetime()} :: apply source mapping")

        src_df.createOrReplaceTempView(table_name)
        derived_df = sqlContext.sql(base_sql)

        # read target data into dataframe
        print(f"{current_datetime()} :: read target data into dataframe")
        tgt_df = read_redshift_table(sqlContext, redshift_driver, url, s3_temp_dir, redshift_role, tgt_schema,
                                     table_name)
        tgt_df_col_list = tgt_df.columns
        print(f"{current_datetime()} :: tgt_df_col_list - ", tgt_df_col_list)
        # if table does not exist, create it; otherwise append new rows.
        if tgt_df is None:
            raise Exception(f"target table {tgt_schema}.{table_name} does not exist")
        else:
            print(
                f"{current_datetime()} :: target table {tgt_schema}.{table_name} exist; existing record count : {tgt_df.count()}")
            derived_df.createOrReplaceTempView("src")
            tgt_df.createOrReplaceTempView("tgt")
            keys = read_primary_keys(mapping_df)
            join_cnd = get_scd_join_condition(mapping_df)
            sql = f"select src.* " \
                  f"from " \
                  f"  src " \
                  f"where not exists " \
                  f"  (select 1 from tgt where {join_cnd}) "
            output_df = sqlContext.sql(sql)
            print(f"{current_datetime()} :: transforming data starting")
            output_df = transform(output_df)
            print(f"{current_datetime()} :: intermediate write - 1")
            output_df.write.parquet(path=remove_slash(temp_path) + "_int1/", mode="Overwrite", compression="snappy")
            output_df_1 = read_parquet(sqlContext, remove_slash(temp_path) + "_int1/")
            new_recod_count = output_df_1.count()
            print(f"{current_datetime()} :: new records count : {new_recod_count}")
            print(f"{current_datetime()} :: transforming data done")
            new_added_cols = 1
            if new_recod_count == 0:
                new_cols = list(set(tgt_df.columns) - set(output_df_1.columns)) + list(
                    set(output_df_1.columns) - set(tgt_df.columns))
                new_added_cols = len(new_cols)
                print(f"{current_datetime()} :: Newly added columns :: ", new_cols)

            process_data = True
            if new_recod_count == 0 and new_added_cols == 0:
                process_data = False

            if process_data:
                unioned_df = union_dataframes([tgt_df, output_df_1])
                unioned_df = add_increment_id_to_null_columns(unioned_df)
                print(f"{current_datetime()} :: writing data to s3 location :: {temp_path}")
                unioned_df.write.parquet(path=temp_path, mode="Overwrite", compression="snappy")
                time.sleep(10)
                read_and_load_from_s3_to_redshift()
                print(f"{current_datetime()} :: Deleting temp location {temp_path}")
                delete_s3_folder_spark(sqlContext, temp_path)
        print(f"{current_datetime()} :: end - {part}")

print("job completed successfully")
