import pandas
from cdm_utilities import *
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, lit
import boto3

sc = SparkContext().getOrCreate()
sqlContext = SQLContext(sc)
s3_client = boto3.client('s3')


# function - to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def read_src_mapping(file_name, target_table=None, dataset=None, arn=None):
    buc, key = s3_path_to_bucket_key(file_name)
    mapping_content = get_s3_object(buc, key, arn)
    mapping_pdf = pd.read_csv(BytesIO(mapping_content), dtype=str).fillna("")
    mapping_pdf = mapping_pdf if target_table is None else mapping_pdf[
        mapping_pdf.src_table.str.lower().str.strip() == target_table.lower()]
    mapping_pdf = mapping_pdf if dataset is None else mapping_pdf[
        mapping_pdf.src_name.str.lower().str.strip() == dataset]
    return mapping_pdf


# def one_one_file_processing_df_ret(bucket, file_li, src_file_type, separator):
#     print(f"{current_datetime()} :: one_one_file_processing :: start")
#     # Process each file
#     for index, file_dic in enumerate(file_li):
#         # Source File name and path
#         print(f"{current_datetime()} :: one_one_file_processing :: start - {file_dic}")
#         input_file = file_dic
#         input_file_path = bucket_key_to_s3_path(bucket, input_file, "s3")
#
#         if src_file_type == 'delimited':
#             df = read_csv(sqlContext, input_file_path, delimiter=separator)
#
#         elif src_file_type == 'parquet':
#             df = read_parquet(sqlContext, input_file_path)
#             for c in df.columns:
#                 df = df.withColumn(c, col(c).cast("string"))
#
#         if index == 0:
#             df_ret = df
#         else:
#             df_ret = df_ret.union(df)
#         print(f"{current_datetime()} :: one_one_file_processing :: end - {file_dic}")
#     print(f"{current_datetime()} :: one_one_file_processing :: end")
#     return df_ret


def one_by_one_read_write_once(bucket, file_li, src_file_type, separator):
    print(f"{current_datetime()} :: one_by_one_read_write_once :: start")
    for index, file_path in enumerate(file_li):
        print(f"{current_datetime()} :: one_by_one_read_write_once :: start - reading {file_path}")
        single_input_file_path = bucket_key_to_s3_path(bucket, file_path, "s3")
        print(src_file_type)
        print(index)
        if src_file_type == 'delimited':
            if index == 0:
                print(f"creating df from file  list")
                df_intermediate = read_csv(sqlContext, single_input_file_path, delimiter=separator)
                df = df_intermediate
            else:
                df_intermediate = read_csv(sqlContext, single_input_file_path, delimiter=separator)
                df = df.union(df_intermediate)
        elif src_file_type == 'parquet':
            if index == 0:
                print(f"creating df from file  list")
                df_intermediate = read_parquet(sqlContext, single_input_file_path)
                for c in df_intermediate.columns:
                    df_intermediate = df_intermediate.withColumn(c, col(c).cast("string"))
                df = df_intermediate
            else:
                df_intermediate = read_parquet(sqlContext, single_input_file_path)
                for c in df_intermediate.columns:
                    df_intermediate = df_intermediate.withColumn(c, col(c).cast("string"))
                df = df.union(df_intermediate)
        print(f"{current_datetime()} :: one_by_one_read_write_once :: end - reading {file_path}")

    return df


def pattern_based_load(bucket, src_file_path, file_li, src_obj_type, src_file_type, separator):
    print(f"{current_datetime()} :: pattern_based_load :: start")
    output_file = file_li[0].split("/")[-1]
    file_pattern = str(''.join(i for i in output_file if not i.isdigit())).split(".")[0]
    search_pattern = file_pattern.strip('_')
    input_file_path = bucket_key_to_s3_path(bucket, add_slash(src_file_path) + search_pattern + '*')
    if src_obj_type == "directory":
        input_file_path = bucket_key_to_s3_path(bucket, file_li[0])
    print(f"{current_datetime()} :: pattern_based_load :: read {input_file_path}")
    if src_file_type == 'delimited':
        df = read_csv(sqlContext, input_file_path, delimiter=separator)
    elif src_file_type == 'parquet':
        df = read_parquet(sqlContext, input_file_path)
        for c in df.columns:
            df = df.withColumn(c, col(c).cast("string"))
    print(f"{current_datetime()} :: pattern_based_load :: end")
    return df


def read_file_list_into_df(bucket, file_li, src_obj_type, src_file_type, separator, headers_flg, quote=""):
    print(f"{current_datetime()} :: read_file_list_into_df :: start")
    print(f"{current_datetime()} :: read_file_list_into_df :: object type  is  - {src_obj_type}")
    print(f"{current_datetime()} :: read_file_list_into_df :: quote string is  - {quote}")
    input_file_path_li = bucket_keylist_to_s3_pathlist(bucket, file_li)
    if src_file_type == 'delimited':
        df = read_csv(sqlContext, input_file_path_li, delimiter=separator, headers_flg=headers_flg, quote=quote)
    elif src_file_type == 'parquet':
        df = read_parquet(sqlContext, input_file_path_li)
        for c in df.columns:
            df = df.withColumn(c, col(c).cast("string"))
    print(f"{current_datetime()} :: read_file_list_into_df :: end")
    return df


def get_input_dataset_for_table(bucket, src_name, table_name, market_basket, input_dataset):
    print(f"{current_datetime()} :: get_input_dataset_for_table :: start")
    # Read source dataset info from input_dataset file.
    print(f"Reading dataset info from {input_dataset}")
    _, filekey = s3_path_to_bucket_key(input_dataset)
    dataset_data = get_s3_object(bucket, filekey)
    dataset_df = pd.read_csv(BytesIO(dataset_data)).fillna('')
    print(f"market_to_filter :: {market_basket}")
    dataset_df = dataset_df[((dataset_df.src.str.lower().str.strip() == src_name) &
                             (dataset_df.dataset.str.lower().str.strip() == table_name) &
                             (dataset_df.data_source.str.lower().str.strip() == market_basket))]

    if dataset_df.empty:
        raise Exception(f"No source dataset info is available for src - {src_name}, dataset - {table_name}")

    print(f"{current_datetime()} :: get_input_dataset_for_table :: end")
    return dataset_df


def get_file_list_to_process(dataset_df, bucket, src_file_path, table_name, data_version, proc_flag):
    file_properties = dataset_df["file_properties"].tolist()[0].strip()
    file_extension = dataset_df["file_extension"].tolist()[0].strip().replace("<version>", data_version)
    if len(file_extension) > 0 and file_extension[0] != '.':
        file_extension = "." + file_extension
    if file_properties == "":
        raise Exception(f"get_file_list_to_process - file_properties is not defined")
    file_properties = ast.literal_eval(file_properties)

    if "object_type" not in file_properties:
        raise Exception(f"get_file_list_to_process - object_type is not defined")

    if "file_type" not in file_properties:
        raise Exception(f"get_file_list_to_process - file_type is not defined")

    src_obj_type = file_properties["object_type"]
    file_type_li = list(file_properties["file_type"])
    src_file_type = 'delimited'
    if 'parquet' in file_type_li:
        src_file_type = 'parquet'
    quote_string = ""
    if 'quote' in file_type_li:
        quote_string = file_properties["file_type"]['quote']
    if src_file_type not in ["delimited", "parquet"]:
        raise Exception(f"get_file_list_to_process - {src_file_type} is not handled in current version")
    separator = file_properties["file_type"][src_file_type]
    print(f"{current_datetime()} :: get_file_list_to_process :: info - src_file_type         : {src_file_type}")
    print(f"{current_datetime()} :: get_file_list_to_process :: info - separator             : {separator}")
    print(f"{current_datetime()} :: get_file_list_to_process :: info - quote_string          : {quote_string}")
    print(f"{current_datetime()} :: get_file_list_to_process :: info - src_obj_type          : {src_obj_type}")

    print(f"{current_datetime()} :: get_file_list_to_process :: start")
    print(f"{current_datetime()} :: get_file_list_to_process :: reading source table name for {table_name}")
    src_table_name = dataset_df['table_name'].tolist()[0]
    print(f"{current_datetime()} :: src_table_name : {src_table_name}")
    src_table_name_wo_digit = (''.join(i for i in src_table_name if not i.isdigit())).strip()
    src_table_name_wo_digit = ('_'.join(_ for _ in src_table_name_wo_digit.split("_") if _ != "")).strip()
    print(f"{current_datetime()} :: src_table_name_wo_digit : {src_table_name_wo_digit}")
    if src_table_name is None or src_table_name == "":
        raise Exception(f"Could not find source table name for alias {table_name}")

    file_list = list_s3_files_and_folders(bucket, add_slash(src_file_path))
    if file_list == []:
        src_file_path_lower = add_slash('/'.join(remove_slash(src_file_path).split("/")[:-1])) + add_slash(
            remove_slash(src_file_path).split("/")[-1].lower())
        file_list = list_s3_files_and_folders(bucket, add_slash(src_file_path_lower))
    print(file_list)
    file_list_filtered = []
    for file in file_list:
        # print(file)
        src_file_name_wo_ext = rreplace(str(file.lower().split("/")[-1]), file_extension.lower(), "")
        src_file_name = str(''.join(i for i in src_file_name_wo_ext if not i.isdigit())).strip()
        src_file_name = '_'.join(_ for _ in src_file_name.split("_") if _ != "")
        print(f"file - {file} >>> src_file_name : {src_file_name}")
        if src_obj_type == "file":
            if file[-1] == "/":
                continue
            if proc_flag == "fixed":
                if src_table_name.lower() == src_file_name or src_table_name_wo_digit.lower() == src_file_name:
                    file_list_filtered.append(file)
            elif proc_flag == "pattern":
                if src_table_name.lower() in src_file_name or src_table_name_wo_digit.lower() in src_file_name:
                    file_list_filtered.append(file)
            else:
                raise Exception(f"invalid proc_flag {proc_flag}")
        elif src_obj_type == "directory":
            if file.strip().lower().endswith(src_table_name.strip().lower() + "/"):
                file_list_filtered.append(file)
        else:
            raise Exception(
                f"get_file_list_to_process - object type = {src_obj_type} is not handled in current version")

    print(f"{current_datetime()} :: list of files to process : ", file_list_filtered)
    print(f"{current_datetime()} :: get_file_list_to_process :: end")
    return file_list_filtered, separator, src_file_type, src_obj_type, quote_string


def staging_load(bucket, file_li, sep, src_file_type, src_obj_type, headers_flg, quote=""):
    print(f"{current_datetime()} :: staging dataframe load :: start")

    list_of_paths_with_xlsx = []
    for li in file_li:
        if li.endswith(".xlsx"):
            list_of_paths_with_xlsx.append(li)
    print(f"{current_datetime()} :: staging_load :: list_of_paths_with_xlsx : {list_of_paths_with_xlsx}")

    list_of_paths_without_xlsx = [x for x in file_li if x not in list_of_paths_with_xlsx]
    print(f"{current_datetime()} :: staging_load :: list_of_paths_without_xlsx : {list_of_paths_without_xlsx}")

    if list_of_paths_with_xlsx:
        print(f"File pattern is xlsx, skipping the process")

    if list_of_paths_without_xlsx:
        df = read_file_list_into_df(bucket, list_of_paths_without_xlsx, src_obj_type, src_file_type, sep, headers_flg,
                                    quote)
    print(f"{current_datetime()} :: staging_load :: end")
    return df


def get_stg_validation(column_list_in_src_data, mapping_column_names, tbl_nm):
    new_coln_list = []
    for key in column_list_in_src_data:
        if key.lower() not in mapping_column_names:
            new_coln_list.append(key.lower())
    for key in mapping_column_names:
        if key.lower() not in column_list_in_src_data:
            if key.lower() not in new_coln_list:
                new_coln_list.append(key.lower())
    does_new_file_exists = 0
    if len(new_coln_list) > 0:
        does_new_file_exists = 1
        for key in new_coln_list:
            print(
                f"Column {key} is either not present in source_chema_definition file or in the source data itself, please check")
    else:
        print(f"No new column has been added in {tbl_nm}")
    return does_new_file_exists


def get_rowcount_filesize(bucket, file_ls, extract_file_ctl_tbl_path, separator):
    for item in file_ls:
        file_nm = item.split('/')[-1]
        response = s3_client.head_object(Bucket=bucket, Key=item)
        file_size = response['ContentLength']
        parquet_df = read_csv(sqlContext, bucket_key_to_s3_path(bucket, item), delimiter=separator)
        row_count = parquet_df.count()
        parquet_data = [[file_nm, file_size, current_datetime(), row_count]]
        parquet_df_count = pd.DataFrame(parquet_data, columns=['FILE_NM',
                                                               'SIZE_IN_BYTE', 'EXPORT_DATE',
                                                               'ROW_COUNT'])
        parquet_df_count.to_csv(extract_file_ctl_tbl_path, mode='a', header=False, index=False)


def getdiff_rowcount_filesize_from_path(stg_extract_summary_filepath, stg_data_summary_filepath):
    df = pandas.read_excel(stg_extract_summary_filepath, sheet_name='Over_all_counts')
    df1 = sc.createDataFrame(df)
    df1.show()
    df2 = read_csv(sqlContext, stg_data_summary_filepath)
    df2.show()
    df1.createOrReplaceTempView("extract_summary")
    df2.createOrReplaceTempView("data_summary")
    filter_query = 'Select * from extract_summary as src left join data_summary as tgt on `File Name` = FILE_NM where FILE_NM is not Null ' \
                   'and (`Compressed File size` !=  SIZE_IN_BYTE or `Row Count` != ROW_COUNT + 1 )'
    df3 = sqlContext.sql(filter_query)
    df3.show(1000)
    return df3.count()


def getdiff_rowcount_filesize_from_df(stg_extract_summary_df, stg_data_summary_filepath):
    print("Inside getdiff_rowcount_filesize_from_df")
    df1 = stg_extract_summary_df
    df2 = pandas.read_csv(stg_data_summary_filepath)
    df = pd.merge(df1, df2, how='left', left_on=['file_name'], right_on=['FILE_NM'])
    df = df.dropna(subset=['FILE_NM'])
    df['ROW_COUNT'] = df['ROW_COUNT'].astype(int)
    df['row_count'] = df['row_count'].astype(int)
    df['ROW_COUNT'] = df['ROW_COUNT'] + 1
    df = df[(df.file_size != df.SIZE_IN_BYTE) | (df.row_count != df.ROW_COUNT)]
    print("mismatch row", df)
    return df.shape[0]


def raise_error_condition(src_name, market_basket, deact_file_count, new_file_exist, mismatch_count, load_type,
                          is_deactive_file_avlbl, table_name, staging_validation_csv):
    deact_file_fail = 100
    if is_deactive_file_avlbl == '1':
        if load_type == 'active' or load_type == 'incremental':
            if deact_file_count > 1:
                deact_file_fail = 0
            else:
                deact_file_fail = 1
        else:
            if deact_file_count > 1:
                deact_file_fail = 1
            else:
                deact_file_fail = 0

    if not s3_path_exists(staging_validation_csv):
        data_extract = [['SRC_NAME', 'MARKET_BASKET', 'TABLE_NAME', 'DEAC_FILE_VALIDATION', 'SOURCE_COLUMN_VALIDATION',
                         'FILE_SIZE_ROWCOUNT_VALIDATION', 'EXPORT_DATE']]
        df_count = pd.DataFrame(data_extract,
                                columns=['SRC_NAME', 'MARKET_BASKET', 'TABLE_NAME', 'DEAC_FILE_VALIDATION',
                                         'SOURCE_COLUMN_VALIDATION', 'FILE_SIZE_ROWCOUNT_VALIDATION', 'EXPORT_DATE'])
        df_count.to_csv(staging_validation_csv, mode='a', header=False, index=False)

    print(f"is_deactive_file_avlbl, deact_file_fail :: {is_deactive_file_avlbl}, {deact_file_fail}")
    deact_validation_status = 'NA'
    if new_file_exist == 1:
        col_validation_status = 'FAILED'
    else:
        col_validation_status = 'SUCCESS'
    if mismatch_count > 0:
        rowcount_validation_status = 'FAILED'
    else:
        rowcount_validation_status = 'SUCCESS'
    if deact_file_fail != 100:
        if deact_file_fail == 1:
            deact_validation_status = 'FAILED'
        else:
            rowcount_validation_status = 'SUCCESS'

    data_extract = [
        [src_name.upper(), market_basket.upper(), table_name.upper(), deact_validation_status, col_validation_status,
         rowcount_validation_status, current_datetime()]]
    df_count = pd.DataFrame(data_extract, columns=['SRC_NAME', 'MARKET_BASKET', 'TABLE_NAME', 'DEAC_FILE_VALIDATION',
                                                   'SOURCE_COLUMN_VALIDATION', 'FILE_SIZE_ROWCOUNT_VALIDATION',
                                                   'EXPORT_DATE'])
    df_count.to_csv(staging_validation_csv, mode='a', header=False, index=False)

    if new_file_exist == 1:
        raise Exception(
            f"New columns are present in source data which are not present in mapping sheet for {table_name}, please check")
    if mismatch_count > 0:
        raise Exception(
            f"Few of the rowcounts and file sizes are not matching with the source extract summary for {table_name}, please check")
    if deact_file_fail != 100:
        if deact_file_fail == 1:
            raise Exception(f"Deactive file is either not expected or is not available for {table_name}, please check")


def source_column_validation(bucket, src_name, market_basket, is_file_available, li, deact_li,
                             file, src_table_name_wo_digit, file_tp, obj_tp, sep):
    if is_file_available == '1':
        print(f"{current_datetime()} :: main :: step 4 - perform staging load")
        df = staging_load(bucket, li, sep, file_tp, obj_tp, True)
        column_list_in_src_data_tmp = df.columns
        column_list_in_src_data = []
        for key in column_list_in_src_data_tmp:
            column_list_in_src_data.append(key.lower())
        print(f"all column list in source data :: {column_list_in_src_data}")

        print(f"read mapping workbook - {file}; worksheet - {src_name}")
        buc, key = s3_path_to_bucket_key(file)
        mapping_content = get_s3_object(buc, key)
        tbl_df = pd.read_excel(BytesIO(mapping_content), src_name, dtype=str).fillna("")

        tbl_df = tbl_df[(tbl_df.src_feed_name.str.lower() == src_table_name_wo_digit) & (
                tbl_df.src_name.str.lower() == src_name) & (tbl_df.data_source.str.lower() == market_basket)]
        print("Printing the tbl_df")
        print(tbl_df)
        mapping_column_names = tbl_df.column_name.unique().tolist()
        print(mapping_column_names)
        does_new_file_exists = get_stg_validation(column_list_in_src_data, mapping_column_names,
                                                  src_table_name_wo_digit)
        deact_file_count = 0
        does_deact_new_file_exists = 0

    else:
        print(f"{current_datetime()} :: Skipping the load process as file is not present")
    if does_new_file_exists == 1 or does_deact_new_file_exists == 1:
        new_file_exist = 1
    else:
        new_file_exist = 0
    return deact_file_count, new_file_exist


def read_pandas_df(filepath, headers, delimiter, skip_rows):
    file_name = filepath.split("~")[0]
    sheet = filepath.split("~")[-1]
    if delimiter != '':
        return pandas.read_csv(file_name, names=headers, sep=delimiter, skiprows=skip_rows)
    else:
        return pandas.read_excel(file_name, sheet_name=sheet, names=headers, skiprows=skip_rows)


def iqvia_file_rename(bucket, files_li, path):
    new_file_li = []
    fail_li = []
    for file in files_li:
        file_nm = file.split("/")[-1]
        if 'GZ.001' in file_nm:
            new_name = add_slash(path) + file_nm.replace('GZ.001', 'gz')
            multipart_copy_s3_object(bucket, file, new_name)
            new_file_li.append(new_name)
        elif 'GZ.002' in file_nm:
            new_name = add_slash(path) + file_nm.replace('GZ.002', 'gz')
            multipart_copy_s3_object(bucket, file, new_name)
            new_file_li.append(new_name)
        elif 'GZ.003' in file_nm:
            new_name = add_slash(path) + file_nm.replace('GZ.003', 'gz')
            multipart_copy_s3_object(bucket, file, new_name)
            new_file_li.append(new_name)
        else:
            fail_li.append(file)
    if fail_li:
        raise Exception(f"pre-processing {__name__} failed for {fail_li}")
    if not new_file_li:
        raise Exception(f"{__name__} pre-processed item list id empty")
    return new_file_li


def replace_space_in_columnname(df):
    for c in df.columns:
        new_c = '_'.join([_ for _ in c.split()])
        df = df.withColumnRenamed(c, new_c)

    return df
