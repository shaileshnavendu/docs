# built-in libraries

import sys
import traceback

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

# user defined libraries
from cdm_utilities import *

# initialize spark context and sqlcontext
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
sc.setLogLevel("Error")

# initialize script variables
format_length = 150


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def current_date():
    return datetime.now().strftime('%Y-%m-%d')


def generate_summary(curr_map_df, src_path, table_type, extract_type, delimiter, extract_tbl_name):
    print(f"{current_datetime()} :: main :: info - summary generation starting for src_path : {src_path}")

    data_dict_summary_path = generate_data_dictionary_summary(sqlContext, src_path, target_summary_temp_path,
                                                              table_type, file_type=extract_type, delimiter=delimiter,
                                                              extract_tbl_nm=extract_tbl_name)
    if data_dict_summary_path:
        print(f"{current_datetime()} :: main :: info - generated summary for data_dict_summary_path : "
              f"{data_dict_summary_path}")
    else:
        print(f"{current_datetime()} :: main :: error -  data_dict_summary_path was not created")

    table_level_summary_path = generate_table_level_summary(sqlContext, src_path, target_summary_temp_path,
                                                            generation_time=current_date(),
                                                            file_type=extract_type, delimiter=delimiter,
                                                            extract_tbl_nm=extract_tbl_name)
    if table_level_summary_path:
        print(f"{current_datetime()} :: main :: info - generated summary for table_level_summary_path : "
              f"{table_level_summary_path}")
    else:
        print(f"{current_datetime()} :: main :: error - table_level_summary_path was not created")

    file_level_summary_path = generate_file_level_summary(sqlContext, src_path, target_summary_temp_path,
                                                          generation_time=current_date(),
                                                          file_type=extract_type, delimiter=delimiter,
                                                          extract_tbl_nm=extract_tbl_name)
    if file_level_summary_path:
        print(f"{current_datetime()} :: main :: info - generated summary for file_level_summary_path :"
              f" {file_level_summary_path}")
    else:
        print(f"{current_datetime()} :: main :: error - file_level_summary_path was not created")

    if extract_name == "powerbi_extract":
        table_level_summary_path_v2 = generate_table_level_summary_v2(curr_map_df, load_type, sqlContext, src_path,
                                                                      target_summary_temp_path,
                                                                      generation_time=current_date(),
                                                                      file_type=extract_type, delimiter=delimiter,
                                                                      extract_tbl_nm=extract_tbl_name)
        if table_level_summary_path_v2:
            print(f"{current_datetime()} :: main :: info - generated summary for table_level_summary_path : "
                  f"{table_level_summary_path_v2}")
        else:
            print(f"{current_datetime()} :: main :: error - table_level_summary_path was not created")


# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv,
                              ['S3_BUCKET', 'CONFIG_FILE', 'TABLE_NAME', 'ARN', 'LAYER', 'NUM_PARTITIONS', 'LOAD_TYPE',
                               'TOUCHFILE_NAME', 'ADD_DATE'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    # table_name = args['TABLE_NAME']
    table_alias_name = args['TABLE_NAME']
    extract_name = args['LAYER']
    add_date = args['ADD_DATE']
    touchfile_name = args['TOUCHFILE_NAME']
    numPartition = int(args['NUM_PARTITIONS'])
    load_type = args['LOAD_TYPE'].strip().lower()
    arn = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")
    # print(f"{current_datetime()} :: main :: info - table_name       : {table_name}")
    print(f"{current_datetime()} :: main :: info - table_alias_name : {table_alias_name}")
    print(f"{current_datetime()} :: main :: info - extract_name     : {extract_name}")
    print(f"{current_datetime()} :: main :: info - numPartition     : {numPartition}")
    print(f"{current_datetime()} :: main :: info - load_type        : {load_type}")
    print(f"{current_datetime()} :: main :: info - arn              : {arn}")
    print(f"{current_datetime()} :: main :: info - touchfile_name   : {touchfile_name}")
    print(f"{current_datetime()} :: main :: info - add_date         : {add_date}")
print("*" * format_length)

if load_type not in ["full", "incremental"]:
    raise Exception(f"incorrect load type - {load_type}")

# parse the config file contents
print(f"\n{current_datetime()} :: main :: info - reading the config file {config_file} in bucket {bucket} ...\n")
try:
    param_data = get_s3_object(bucket, config_file, arn)
    param_contents = json.loads(param_data)
    tgt_bucket = bucket if arn is None else param_contents["cdm_account_details"]["bucket"]
    mapping_file = param_contents[extract_name]["extracts_mapping"]
    extract_version = param_contents[extract_name][
        "version"]  # changed on 19 Jul 2022 to read custom version for extracts
    parquet_conv_reqd = param_contents[extract_name]["parquet_conversion_required"] if "parquet_conversion_required" in \
                                                                                       param_contents[
                                                                                           extract_name] else False
    mapping_file_type = mapping_file["mapping_file_type"]
    file = mapping_file["file"]
    sheet1 = mapping_file["sheet1"]
    sheet2 = mapping_file["sheet2"]
    root_path = add_slash(param_contents["root_path"])
    outbound_base_path = remove_slash(param_contents[extract_name]["outbound_base_dir"])
    target_path_wo_prefix = root_path + add_slash(outbound_base_path)
    target_summary_temp_wo_prefix = root_path + f'{outbound_base_path}_temp/{extract_version}/'
    target_summary_temp_path = bucket_key_to_s3_path(tgt_bucket, target_summary_temp_wo_prefix, 's3')
    target_path = bucket_key_to_s3_path(tgt_bucket, target_path_wo_prefix, 's3')
    source_path_wo_prefix_icdm = param_contents["root_path"] + add_slash(param_contents["icdm_base_dir"])
    source_path_icdm = bucket_key_to_s3_path(tgt_bucket, source_path_wo_prefix_icdm, 's3')
    source_path_wo_prefix_deid = param_contents["root_path"] + add_slash(param_contents["masked_base_dir"])
    source_path_deid = bucket_key_to_s3_path(tgt_bucket, source_path_wo_prefix_deid, 's3')
    lookup_src_path = remove_slash(target_path) + "_temp/external_id/"
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(
        f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")
    print(f"{current_datetime()} :: main :: info - mapping_file         : {file}")
    print(f"{current_datetime()} :: main :: info - mapping_file_type    : {mapping_file_type}")
    print(f"{current_datetime()} :: main :: info - source_path - 1      : {source_path_icdm}")
    print(f"{current_datetime()} :: main :: info - source_path - 2      : {source_path_deid}")
    print(f"{current_datetime()} :: main :: info - target_path          : {target_path}")
    print(f"{current_datetime()} :: main :: info - lookup_src_path      : {lookup_src_path}")
print("*" * format_length)

try:
    # Read mapping file and filter for current table
    if mapping_file_type == 'excel':
        print(f"read mapping workbook - {file}; worksheet - {sheet1}")
        buc, key = s3_path_to_bucket_key(file)
        mapping_content = get_s3_object(buc, key, arn)
        tbl_df = pd.read_excel(BytesIO(mapping_content), sheet1, dtype=str).fillna("")
        # tbl_df = tbl_df[(tbl_df.table.str.lower() == table_name) & (tbl_df.Included.str.lower() == 'y')]
    elif mapping_file_type == 'csv':
        print(f"read csv mapping; file name - {sheet1}")
        buc, key = s3_path_to_bucket_key(sheet1)
        tbl_content = get_s3_object(buc, key, arn)
        tbl_df = pd.read_csv(BytesIO(tbl_content), dtype=str).fillna("")
    else:
        raise Exception(f"invalid mapping file type - {mapping_file_type}")

    # changed on 19 Jul 2022 - to filter by alias as well
    tbl_df = tbl_df[
        (tbl_df.Included.str.lower() == 'y') & (
                tbl_df.extract_table_name.str.lower() == table_alias_name.strip().lower())]
    # print(tbl_df)

    # if mapping not available for current table - exit
    # otherwise, get column list.
    #       If column list is empty, raise exception.
    #       otherwise, iterate through each row in mapping & generate extract

    if tbl_df.empty:
        print(f"extract_table_name={table_alias_name} is not identified to generate extracts")
        raise Exception(
            f"data extract config not found for extract_table_name={table_alias_name}, included=Y")
    else:
        if len(tbl_df.extract_table_name.str.strip().unique()) != tbl_df.shape[0]:
            raise Exception(
                f"Mapping Issue:: Duplicate extract_table_name given for table: extract_table_name={table_alias_name}")
        for tbl_ind in tbl_df.index:
            print(f"start table index - {tbl_ind}")
            # print(tbl_ind, tbl_df["alias"][tbl_ind])
            curr_tbl_df = tbl_df[tbl_df.index == tbl_ind].reset_index(drop=True)
            table_name = tbl_df["table"][tbl_ind]
            table_alias = tbl_df["alias"][tbl_ind]
            print("get extract_pattern, filter_expression, included_fields and generate query")
            # read query pattern from mapping file
            extract_pattern = tbl_df["extract_pattern"][tbl_ind]
            # getting filter conditions to apply where clause
            if load_type == "full":
                filter_expression = tbl_df["filter_expression_full_load"][tbl_ind].strip() if \
                    tbl_df["filter_expression_full_load"][
                        tbl_ind].strip() != '' else None
            elif load_type == "incremental":
                filter_expression = tbl_df["filter_expression_inc"][tbl_ind].strip() if \
                    tbl_df["filter_expression_inc"][
                        tbl_ind].strip() != '' else None
            else:
                raise Exception(f"incorrect load type - {load_type}")

            if " where " in extract_pattern:
                sql_template = f"{extract_pattern} {' and ' + filter_expression if filter_expression else ''}"
            else:
                sql_template = f"{extract_pattern} {' where ' + filter_expression if filter_expression else ''}"

            sql_template = sql_template.replace('<table:$1>', table_name).replace('<table:$2>', table_alias)

            # get column list
            if mapping_file_type == 'excel':
                print(f"read mapping workbook - {file}; worksheet - {sheet2}")
                ele_df = pd.read_excel(BytesIO(mapping_content), sheet2, dtype=str).fillna("")
            elif mapping_file_type == 'csv':
                print(f"read csv mapping; file name - {sheet2}")
                buc, key = s3_path_to_bucket_key(sheet2)
                ele_content = get_s3_object(buc, key, arn)
                ele_df = pd.read_csv(BytesIO(ele_content), dtype=str).fillna("")
            else:
                raise Exception(f"invalid mapping file type - {mapping_file_type}")
            ele_df = ele_df[(ele_df.table.str.lower() == table_alias) & (ele_df.included.str.lower() == 'y')]
            ele_df["field_order"] = ele_df["field_order"].replace('', '9999').astype(int)
            ele_df = ele_df.sort_values('field_order')
            # print(ele_df)

            if ele_df.empty:
                raise Exception(f"no elements are included for {table_alias} extract")

            # update column names in query
            included_fields = []
            for ind in ele_df.index:
                column = ele_df['field_name'][ind]
                col_alias = ele_df['new_field_name'][ind] if ele_df['new_field_name'][ind].strip() != '' else column
                dtype = ele_df['datatype'][ind].strip()
                dlength = ele_df['length'][ind].strip()
                dformat = ele_df['format'][ind].strip()
                sql_tx = ele_df['sql_tx'][ind].strip() if 'sql_tx' in ele_df.columns else ''
                if sql_tx != '':
                    included_fields.append(f"{sql_tx} as {col_alias}")
                elif dtype == 'time':
                    included_fields.append(f"date_format({table_alias}.{column}, '{dformat}') as {col_alias}")
                elif dlength != '' and dtype != 'varchar':
                    included_fields.append(f"cast({table_alias}.{column} as {dtype}{dlength}) as {col_alias}")
                else:
                    included_fields.append(f"cast({table_alias}.{column} as {dtype}) as {col_alias}")
            included_fields = ', '.join(included_fields)
            # print(included_fields)

            # load other tables into dataframe - added by DG : 31 Aug 2021
            # parsing sql to derive table names
            # this will help to make customizations; src_df is one of such example

            src_layer_li = "icdm"
            src_name_li = ""
            if "src_layer" in tbl_df.columns:
                if tbl_df["src_layer"][tbl_ind].strip() != "":
                    src_layer_li = tbl_df["src_layer"][tbl_ind]
            if "src_name" in tbl_df.columns:
                src_name_li = tbl_df["src_name"][tbl_ind]

            print(f"{current_datetime()} :: src_layer_li - {src_layer_li}")
            print(f"{current_datetime()} :: src_name_li  - {src_name_li}")

            layer_var_li = {}
            for _i, _l in enumerate(src_layer_li.split(",")):
                layer_var_li[f"<layer:${_i + 1}>"] = _l.strip()

            src_var_li = {}
            for _i, _s in enumerate(src_name_li.split(",")):
                src_var_li[f"<source:${_i + 1}>"] = _s.strip()

            print(f"{current_datetime()} :: layer_var_li  - {layer_var_li}")
            print(f"{current_datetime()} :: src_var_li    - {src_var_li}")

            all_tbl_li = [_ for _ in sql_template.split(" ") if '<extract_source_schema>.' in _]
            # all_tbl_li = list(set([_.replace('<extract_source_schema>.', "") for _ in all_tbl_li]))
            for tbl_pattern in all_tbl_li:
                layer_key = ''.join([_ for _ in tbl_pattern.split(".") if "<layer:$" in _])
                if layer_key == '':
                    if len(src_layer_li.split(",")) == 1:
                        src_layer = src_layer_li.split(",")[0]
                    elif len(src_name_li.split(",")) > 1:
                        raise Exception(f"variables for src layer not defined in the extract pattern")
                    else:
                        src_layer = "icdm"
                else:
                    src_layer = layer_var_li[layer_key]
                print(f"{current_datetime()} :: layer_key    - {layer_key}")
                print(f"{current_datetime()} :: src_layer    - {src_layer}")

                source_key = ''.join([_ for _ in tbl_pattern.split(".") if "<source:$" in _])
                if source_key == '':
                    if len(src_name_li.split(",")) == 1:
                        src_name = src_name_li.split(",")[0]
                    elif len(src_name_li.split(",")) > 1:
                        raise Exception(f"variables for source name not defined in the extract pattern")
                    else:
                        src_name = ''
                else:
                    src_name = src_var_li[source_key]

                print(f"{current_datetime()} :: source_key    - {source_key}")
                print(f"{current_datetime()} :: src_name      - {src_layer}")

                tbl_nm = tbl_pattern.replace('<extract_source_schema>.', "").replace(f'{source_key}.', "").replace(
                    f'{layer_key}.', "")

                print(f"{current_datetime()} :: tbl_pattern    - {tbl_pattern}")
                print(f"{current_datetime()} :: tbl_nm         - {tbl_nm}")

                picdm_table_path = source_path_icdm + add_slash("picdm_" + tbl_nm)
                if src_layer.strip().lower() == "icdm":
                    if s3_path_exists(picdm_table_path):
                        final_src_path = picdm_table_path
                    else:
                        final_src_path = source_path_icdm + add_slash(tbl_nm)
                elif src_layer.strip().lower() == "masked":
                    masked_version = param_contents["batch_date"]
                    if src_name in param_contents["src_dataset"]:
                        masked_version = param_contents["src_dataset"][src_name]["version"]
                        masked_version = param_contents["batch_date"] if masked_version == "" else masked_version
                    print(f"masked layer version is - {masked_version}")
                    final_src_path = source_path_deid + add_slash(src_name) + add_slash(masked_version) + add_slash(
                        tbl_nm)

                else:
                    raise Exception(f"invalid source layer {src_layer}")

                spark_tbl_df = read_parquet(sqlContext, final_src_path)
                if 'exclude_per_did' in [_c.lower() for _c in spark_tbl_df.columns]:
                    spark_tbl_df = spark_tbl_df.filter("nvl(exclude_per_did,0) != 1")
                print(f"load source data into dataframe - {tbl_nm} from path {final_src_path}")
                print(f"Total Record Count from {tbl_nm} - {spark_tbl_df.count()}")
                spark_tbl_df.createOrReplaceTempView(tbl_nm)

            # load lookup tables
            lkp_tbl_list = [_ for _ in sql_template.split(" ") if '<extract_lookup_schema>.' in _]
            lkp_tbl_list = list(set([_.replace('<extract_lookup_schema>.', "") for _ in lkp_tbl_list]))
            lkp_dfs = {}
            for tbl in lkp_tbl_list:
                print(f"load lookup data into dataframe - {tbl}")
                lkp_dfs[tbl] = read_parquet(sqlContext, lookup_src_path + add_slash(tbl))
                if 'exclude_per_did' in [_c.lower() for _c in lkp_dfs[tbl].columns]:
                    lkp_dfs[tbl] = lkp_dfs[tbl].filter("nvl(exclude_per_did,0) != 1")
                lkp_dfs[tbl] = lkp_dfs[tbl].persist()
                print(f"Total Record Count from {tbl} - {lkp_dfs[tbl].count()}")
                lkp_dfs[tbl].createOrReplaceTempView(tbl)

            # get output folder name
            extract_table_name = tbl_df["extract_table_name"][tbl_ind]
            out_folder = (extract_version + "/" + (
                table_name if extract_table_name.strip() == '' else extract_table_name.strip()))

            sql = sql_template. \
                replace('<included_fields>', included_fields). \
                replace('<extract_source_schema>.', ''). \
                replace('<extract_lookup_schema>.', ''). \
                replace('<add_date>', f"'{add_date}'"). \
                replace('varchar', 'string')

            for _l in layer_var_li:
                sql = sql.replace(f'{_l}.', "")

            for _s in src_var_li:
                sql = sql.replace(f'{_s}.', "")

            print("sql to execute - ", sql)

            out_df = sqlContext.sql(sql)
            print(f"write data into target path - {target_path + add_slash(out_folder)}")
            print(f"count - {out_df.count()}")

            if mapping_file['extract_type'] == 'parquet':
                out_df.coalesce(numPartition).write.parquet(target_path + add_slash(out_folder), "Overwrite",
                                                            compression='gzip')
            elif mapping_file['extract_type'] == 'delimited':
                out_df.coalesce(numPartition).write.csv(target_path + add_slash(out_folder), "Overwrite", header=True,
                                                        sep=mapping_file['delimiter'], compression='gzip')
            else:
                raise Exception(
                    f"invalid extract_type - {mapping_file['extract_type']}; it must be any of 'parquet', 'delimited'")
            generate_summary(curr_tbl_df, target_path + out_folder, tbl_df["type"][tbl_ind],
                             mapping_file['extract_type'],
                             mapping_file['delimiter'], extract_table_name)

            # added on 19 Jul 2022; as part of initial review by JBI
            if touchfile_name.lower() == "na":
                pass
            else:
                print(f"{current_datetime()} :: creating touch file - {touchfile_name}")
                put_s3_object(bucket, add_slash(target_path_wo_prefix) + add_slash(out_folder) + touchfile_name, "")

            if parquet_conv_reqd:
                if mapping_file['extract_type'] != 'delimited':
                    raise Exception("extract type is not delimited, parquet conversion is not possible")
                out_folder_parquet = (extract_version + "_parquet/" + (
                    table_name if extract_table_name.strip() == '' else extract_table_name.strip()))
                print(f"{current_datetime()} :: starting csv to parquet conversion")
                print(f"{current_datetime()} :: reading extract from {target_path + add_slash(out_folder)}")
                csv_df = read_csv(sqlContext,
                                  target_path + add_slash(out_folder),
                                  delimiter=mapping_file['delimiter'])
                print(f"{current_datetime()} :: write parquet extract to {target_path + add_slash(out_folder_parquet)}")
                csv_df.write.parquet(target_path + add_slash(out_folder_parquet), "Overwrite")
                print(f"{current_datetime()} :: csv to parquet conversion completed")

            print(f"end table index - {tbl_ind}")
except Exception as e:
    print("ERROR DETAILS - ", e)
    print(traceback.format_exc())
    raise e

print("job completed successfully")
