# built-in libraries
import sys
from awsglue.utils import getResolvedOptions
import traceback
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
import base64
from datetime import datetime
import pandas as pd
import json
from io import BytesIO

# user defined libraries
from s3_utils import *

# initialize script variables
print(f"info - initialize script variables...\n")
format_length = 150
secrets_client = boto3.client('secretsmanager')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


# function to get current date and time
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d')


def define_secret():
    global secret_name, secret_key
    if env.lower() in ['env1']:
        secret_name = param_contents['qc_integration']["env1"]['secret_name']
        secret_key = param_contents['qc_integration']["env1"]['secret_key']
    elif env.lower() in ['env2']:
        secret_name = param_contents['qc_integration']["env2"]['secret_name']
        secret_key = param_contents['qc_integration']["env2"]['secret_key']
    else:
        raise Exception(f"layer '{env}' not handled in current version")


def send_email(csv_path):
    try:
        sg_credentials = secrets_client.get_secret_value(SecretId=secret_name)
        json_creds = json.loads(sg_credentials["SecretString"])
        analytics_creds = json_creds[secret_key]
    except Exception as err:
        print("error details 1 : ", err)
        raise err

    from_email_id = param_contents['qc_integration']['from_email']
    # to_email_ids = param_contents["qc_integration"]["email_id_list"]
    to_email_ids = ["nchatra@concertai.com", "DGore@concertai.com"]
    email_subject = f"Predictive Patient | {bucket} | Summarized Validation Report "
    email_content = "<p>Hi Team,<br><br> Test Queries are executed successfully; " \
                    "Test Report is generated for the same.<br><br>Thank you,<br>ConcertAI Team</p>"

    message = Mail(
        from_email=from_email_id,
        to_emails=to_email_ids,
        subject=email_subject,
        html_content=email_content)

    buc, key = s3_path_to_bucket_key(csv_path)
    obj = s3_resource.Object(bucket, key)
    body = obj.get()['Body'].read()

    encoded = base64.b64encode(body).decode()
    _attachment = Attachment()
    _attachment.file_content = FileContent(encoded)
    _attachment.file_type = FileType('application/csv')
    _attachment.file_name = FileName(f"{batch_date}_qa_summary.csv")
    _attachment.disposition = Disposition('attachment')
    message.attachment = _attachment

    try:
        sendgrid_client = SendGridAPIClient(analytics_creds)
        response = sendgrid_client.send(message)
        print(response)
    except Exception as e:
        traceback.print_exc()
        print("error details 2 : ", e)


def get_data_from_df(df):
    string_data = ','.join(df.columns)
    df = df.astype(str)
    for index, row in df.iterrows():
        line = ''
        for cell in row.values:
            line += cell + ','
        string_data += '\n' + line[:-1]
    return string_data


def collect_qa_summary(src_path, output_path):
    """
    :param output_path: output path
    :param src_path: source folder path till the qa_summary/<batch_date>
    :return: summarized csv path
    """
    src_bucket, path = s3_path_to_bucket_key(src_path)
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=src_bucket, Prefix=path)
    df_list = []
    for page in pages:
        for obj in page['Contents']:
            content = get_s3_object(bucket, obj['Key'])
            df = pd.read_csv(BytesIO(content), dtype=str).fillna('NA')
            df_list.append(df)
    df_res = pd.concat(df_list, ignore_index=True)
    df_res = df_res[df_res.html_file.str.upper() != 'NA']
    string_data = get_data_from_df(df_res)
    buc, key = s3_path_to_bucket_key(output_path)
    put_s3_object(bucket, key, string_data, arn)


# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'ARN'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    arn = args['ARN'] if args['ARN'].strip().startswith("arn:aws:iam::") else None
    env = 'env1' if arn is None else 'env2'
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")
    print(f"{current_datetime()} :: main :: info - arn              : {arn}")
    print(f"{current_datetime()} :: main :: info - env              : {env}")
print("*" * format_length)

# parse the config file contents
print(f"\n{current_datetime()} :: main :: info - reading the config file {config_file} in bucket {bucket} ...\n")
try:

    param_data = get_s3_object(bucket, config_file, arn)
    param_contents = json.loads(param_data)
    tgt_bucket = bucket if arn is None else param_contents["cdm_account_details"]["bucket"]
    batch_date = param_contents["batch_date"]
    output_path = param_contents['qc_integration']['qc_summary_path'] + f"{batch_date}/{batch_date}_qa_summary.csv"
    input_path = param_contents['qc_integration']['qc_summary_path'] + f"{batch_date}/"
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(
        f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")
    print(f"{current_datetime()} :: main :: info - input_path           : {input_path}")
    print(f"{current_datetime()} :: main :: info - output_path          : {output_path}")

print("*" * format_length)
print(f"{current_datetime()} :: main :: info - starting summary aggregation")
collect_qa_summary(input_path, output_path)
print(f"{current_datetime()} :: main :: info - finished summary aggregation")
define_secret()
print(f"{current_datetime()} :: main :: info - sending email of summary report")
send_email(output_path)
print("job completed successfully")
