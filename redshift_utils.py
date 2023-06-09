import psycopg2
import pandas as pd
from io import BytesIO
from datetime import datetime
from s3_utils import *

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 2000000)
pd.set_option('display.max_colwidth', 10000)
pd.options.mode.chained_assignment = None

format_length = 150


def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


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
    mapping_pdf = pd.read_csv(BytesIO(mapping_content)).replace(float("nan"), "")
    mapping_pdf = mapping_pdf if target_table is None else mapping_pdf[
        mapping_pdf.cdm_table.str.lower() == target_table.lower()]
    mapping_pdf = mapping_pdf if dataset is None else mapping_pdf[
        mapping_pdf.cdm_name.str.lower() == dataset]
    return mapping_pdf


def get_credentials_from_sm(secret_name, region_name):
    if secret_name is not None and region_name is not None:
        secrets_client = boto3.client('secretsmanager', region_name=region_name)
        db_credentials = secrets_client.get_secret_value(SecretId=secret_name)
        return db_credentials
    else:
        raise Exception("invalid secret name '{secret_name'} or region '{region_name}'")


# function - get redshift connection
def get_redshift_connection(redshift_host, redshift_port, redshift_user, redshift_password, redshift_db):
    # print(f"\n get_redshift_connection :: info - establishing redshift connection ...")
    try:
        redshift_connection = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            user=redshift_user,
            password=redshift_password,
            dbname=redshift_db
        )
    except Exception as err:
        print(f"get_redshift_connection :: error - failed to establish redshift connection")
        print("error details : ", err)
        raise err
    #        print(f"get_redshift_connection :: info - successfully established redshift connection")
    #        print(f"get_redshift_connection :: info - returning redshift connection object\n")
    return redshift_connection


def check_table_exists(host, port, user, password, db, schema, table):
    try:
        redshift_connection = get_redshift_connection(host, port, user, password, db)
        redshift_cursor = redshift_connection.cursor()
        query = """
           select count(1) from information_schema.tables where table_schema = '""" + schema + """' and table_name = '""" + table + """'
        """
        redshift_cursor.execute(query)
        data = redshift_cursor.fetchone()[0]
        redshift_connection.commit()
        redshift_cursor.close()
        redshift_connection.close()
        if data == 0:
            return False
        else:
            return True
    except Exception as e:
        print("Error Details : ", e)
        raise Exception(f"could not check whether table {schema}.{table} present in {db}")


def check_column_exists(host, port, user, password, db, schema, table, column):
    try:
        redshift_connection = get_redshift_connection(host, port, user, password, db)
        redshift_cursor = redshift_connection.cursor()
        query = "select count(1) from information_schema.columns where table_schema = '" + \
                schema + "' and table_name = '" + table + "' and column_name = '" + column + "';"
        print(f"Executing query : {query}")
        redshift_cursor.execute(query)
        data = redshift_cursor.fetchone()[0]
        redshift_connection.commit()
        redshift_cursor.close()
        redshift_connection.close()
        if data == 0:
            return False
        else:
            return True
    except Exception as e:
        print("Error Details : ", e)
        raise Exception(f"could not check whether column {column} in {schema}.{table} present in {db}")


def rename_table(host, port, user, password, db, schema, table, new_name):
    try:
        redshift_connection = get_redshift_connection(host, port, user, password, db)
        redshift_cursor = redshift_connection.cursor()
        query = f"alter table {schema}.{table} rename to {new_name}"
        redshift_cursor.execute(query)
        redshift_connection.commit()
        redshift_cursor.close()
        redshift_connection.close()
    except Exception as e:
        print("Error Details : ", e)
        return False
    else:
        return True


def run_sql(host, port, user, password, db, sql):
    try:
        redshift_connection = get_redshift_connection(host, port, user, password, db)
        redshift_cursor = redshift_connection.cursor()
        print("SQL to execute : \n", sql)
        redshift_cursor.execute(sql)
        redshift_connection.commit()
        redshift_cursor.close()
        redshift_connection.close()
    except Exception as e:
        print("Error Details : ", e)
        # return False
        raise e
    else:
        return True


def get_schema_suffix(dataset, market=None):
    """
    This reads dataset name and market name as input and
    derives schema suffix.
    :param dataset: sphub, claims etc
    :param market: ac, ai, pc etc
    :return: concatenated string
    """
    if market == "" or market is None:
        return "_" + dataset.lower()
    else:
        return "_" + dataset.lower() + "_" + market.lower()


def drop_table(host, port, user, password, db, schema, table):
    try:
        redshift_connection = get_redshift_connection(host, port, user, password, db)
        redshift_cursor = redshift_connection.cursor()
        query = f"drop table if exists {schema}.{table}"
        redshift_cursor.execute(query)
        redshift_connection.commit()
        redshift_cursor.close()
        redshift_connection.close()
    except Exception as e:
        print("Error Details : ", e)
        raise Exception(f"can not drop table {schema}.{table}")
    else:
        return True


def grant_privileges(host, port, user, password, db, schema, user_list):
    try:
        redshift_connection = get_redshift_connection(host, port, user, password, db)
        redshift_cursor = redshift_connection.cursor()
        query = f"grant all on all tables in schema {schema} to {user_list};\
            GRANT ALL ON SCHEMA {schema} TO {user_list} WITH GRANT OPTION;"
        redshift_cursor.execute(query)
        redshift_connection.commit()
        redshift_cursor.close()
        redshift_connection.close()
    except Exception as e:
        print("Error Details : ", e)
        return False
    else:
        return True


def get_columns(host, port, user, password, db, schema, table):
    try:
        redshift_connection = get_redshift_connection(host, port, user, password, db)
        redshift_cursor = redshift_connection.cursor()
        query = f"select column_name from information_schema.columns where table_schema = '{schema}' and table_name = '{table}'"
        redshift_cursor.execute(query)
        data = redshift_cursor.fetchall()
        data = [r[0] for r in data]
        redshift_connection.commit()
        redshift_cursor.close()
        redshift_connection.close()
        return data
    except Exception as e:
        print("Error Details : ", e)
        raise Exception(f"could not fetch columns from table {schema}.{table} present in {db}")


# Copy from S3 to RedShift
def copy_s3_parquet_to_redshift(redshift_schema, redshift_table, input_file, redshift_role):
    print("*" * format_length)
    print(f"\n{current_datetime()} :: copy_s3_parquet_to_redshift :: info - copying file from S3 to RedShift ...")

    try:
        redshift_table = redshift_schema + "." + redshift_table
        sql = "truncate table " + redshift_table + ";" + "copy " + redshift_table + " from '" + input_file + \
              "' iam_role '" + redshift_role + "' format as parquet COMPUPDATE OFF STATUPDATE OFF;"
        # copy_status = run_sql(redshift_host, redshift_port, redshift_user, redshift_password, redshift_db, sql)
    except Exception as err:
        print("error details : ", err)
        raise err
    else:
        return sql


def get_target_type(df):
    """
    This function reads mapping dataframe and
    returns target_type
    :param df: mapping dataframe (pandas df)
    :return: target_type
    """
    target_type = pd.unique(df.target_type).tolist()
    target_type = [t.strip().lower() for t in target_type if t.strip() != '']
    return target_type[0] if target_type != [] else ''


def get_column_datatype(lkp_column_attr, item_name, item_type):
    """
    """
    return_value = "255"
    if item_type.lower() == 'string':
        for key, value in lkp_column_attr.items():
            if value['cdm_column'] == item_name:
                if value['datatype'] == 'varchar':
                    return_value = value['length']
                break
        if return_value.strip() != '':
            return "varchar(" + return_value + ")  ENCODE zstd "
        else:
            return "varchar(255)" + "  ENCODE zstd "
    elif item_type.lower() == 'boolean':
        return item_type
    else:
        return item_type + " ENCODE az64 " 


# function - to get createtablestatement from glue crawler
# pass on the table , database name and region_name
def get_createtablestatement_from_gluecrawler(region_name, databasename, crawler_tbl_prefix, tablename,
                                              lkp_column_attr):
    lkp_column_attr = lkp_column_attr[["cdm_column", "length", "datatype"]]
    lkp_column_attr_dict = lkp_column_attr.to_dict('index')
    if region_name is not None and databasename is not None and tablename is not None:
        glueclient = boto3.client('glue', region_name=region_name)
        cai_tablename = crawler_tbl_prefix + "cai_" + tablename
        src_tablename = crawler_tbl_prefix + tablename
        try:
            tableDetails = glueclient.get_table(DatabaseName=databasename, Name=cai_tablename)
            tableDef = tableDetails['Table']['StorageDescriptor']['Columns']
            createTableStatement = 'CREATE TABLE IF NOT EXISTS schema_name.' + cai_tablename + '( '
            for item in tableDef:
                createTableStatement = createTableStatement + item['Name'] + " " + get_column_datatype(
                    lkp_column_attr_dict,
                    item['Name'], item['Type']) + ',\n'
            createTableStatement = createTableStatement[:-2] + "\n)\n" + "DISTSTYLE EVEN\n"
            print(f"sourced createtablestatement : {createTableStatement}")
        except Exception as err:
            tableDetails = glueclient.get_table(DatabaseName=databasename, Name=src_tablename)
            tableDef = tableDetails['Table']['StorageDescriptor']['Columns']
            createTableStatement = 'CREATE TABLE IF NOT EXISTS schema_name.' + src_tablename + '( '
            for item in tableDef:
                createTableStatement = createTableStatement + '"' + item['Name'] + '" ' + get_column_datatype(
                    lkp_column_attr_dict,
                    item['Name'], item['Type']) + ',\n'
            createTableStatement = createTableStatement[:-2] + "\n)\n" + "DISTSTYLE EVEN\n"
            print(f"sourced createtablestatement : {createTableStatement}")
            return createTableStatement.replace(src_tablename, tablename)
        else:
            return createTableStatement.replace(cai_tablename, tablename)
    else:
        raise Exception("Info : get_createtablestatement_from_gluecrawler() function - invalid region name :'{region_name'} or databasename :'{databasename}' or tablename : '{tablename}'")
