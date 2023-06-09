import sys
import boto3
import pandas as pd
from datetime import datetime
from awsglue.utils import getResolvedOptions
import time

# function - to get current date and time for logging
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# main function
if __name__ == "__main__":

    # variable declaration
    format_length = 150
    format_length_inner = 75
    region_name = "us-east-1"
    """
    bucket = "eurekapatient-j1-dev"
    glue_jobs_list_file = "config/glue delete_jobs_list.csv"
    """
    glue_jobs_list_file_separator = ","

    args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    workflow_name = args['WORKFLOW_NAME']
    workflow_run_id = args['WORKFLOW_RUN_ID']

    glue_client = boto3.client("glue", region_name='us-east-1', api_version=None)
    workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)[
        "RunProperties"]
    glue_jobs_list_path = workflow_params["glue_job_list"]

    s3_path = glue_jobs_list_path.replace("s3://", "").replace("s3a://", "")
    bucket, glue_jobs_list_file = s3_path.split("/", 1)

    print("bucket : ", bucket)
    print("glue_jobs_list_file : ", glue_jobs_list_file)

    # create a boto3 client
    print(f"\n{current_datetime()} :: main :: info - creating boto3 client ...")
    try:
        glue_client = boto3.client(service_name="glue", region_name=region_name, verify=True)
        s3_client = boto3.client(service_name="s3", region_name=region_name, verify=True)
    except Exception as err:
        print(f"{current_datetime()} :: main :: error - failed to create boto3 client")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: main :: info - successfully created boto3 client\n")
        print("*" * format_length)

    # read the feed file from s3 into a panda dataframe
    print(
        f"\n{current_datetime()} :: main :: info - read the feed file '{glue_jobs_list_file}' from s3 bucket '{bucket}' into a panda dataframe ...")
    try:
        # read the input feed file from s3
        s3_object = s3_client.get_object(Bucket=bucket, Key=glue_jobs_list_file)
        csv_input_file = s3_object['Body']
        csv_input_file_df = pd.read_csv(csv_input_file, sep=glue_jobs_list_file_separator, dtype=str,
                                        keep_default_na=False, na_filter=False)
    except Exception as err:
        print(
            f"{current_datetime()} :: main :: error - failed to read the feed file '{glue_jobs_list_file}' from s3 bucket '{bucket}' into a panda dataframe")
        print("error details : ", err)
        raise err
    else:
        print(
            f"{current_datetime()} :: main :: info - successfully read the feed file '{glue_jobs_list_file}' from s3 bucket '{bucket}' into a panda dataframe\n")
        print("*" * format_length)

    # delete glue workflow
    print(f"\n{current_datetime()} :: main :: info - deletion of glue workflow - started")
    glue_workflow_name_prior = ""
    for index in csv_input_file_df.index:
        try:
            glue_workflow_name = csv_input_file_df['glue_workflow_name'][index]
            if len(glue_workflow_name) > 0 and glue_workflow_name_prior != glue_workflow_name:
                glue_workflow_name_prior = glue_workflow_name
                print(f"\n{current_datetime()} :: main :: info - deleting glue workflow '{glue_workflow_name}' ...")

                delete_workflow_response = glue_client.delete_workflow(Name=glue_workflow_name)
                if delete_workflow_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print(
                        f"{current_datetime()} :: main :: info - successfully deleted glue workflow '{glue_workflow_name}'")
                else:
                    print(
                        f"{current_datetime()} :: main :: error - failed to delete glue workflow '{glue_workflow_name}'")

        except Exception as err:
            print(f"{current_datetime()} :: main :: error - failed to delete glue workflow '{glue_workflow_name}'")
            print("error details : ", err)
            # raise err
    else:
        print(f"\n{current_datetime()} :: main :: info - deletion of glue workflow - ended\n")
        print("*" * format_length)

    # delete glue jobs
    print(f"\n{current_datetime()} :: main :: info - deletion of glue jobs - started")
    for index in csv_input_file_df.index:
        try:
            glue_job_name = csv_input_file_df['glue_job_name'][index]
            print(f"{current_datetime()} :: main :: info - deleting glue job '{glue_job_name}'")

            delete_job_response = glue_client.delete_job(JobName=glue_job_name)
            if delete_job_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"{current_datetime()} :: main :: info - successfully deleted glue job '{glue_job_name}'\n")
            else:
                print(f"{current_datetime()} :: main :: error - failed to delete glue job '{glue_job_name}'")
            print("*" * format_length_inner)

        except Exception as err:
            print(f"{current_datetime()} :: main :: error - failed to delete glue job '{glue_job_name}'")
            print("error details : ", err)
            # raise err
    else:
        print(f"\n{current_datetime()} :: main :: info - deletion of glue jobs - ended\n")
        print("*" * format_length)

    # delete glue triggers
    print(f"\n{current_datetime()} :: main :: info - deletion of glue triggers - started")
    for index in csv_input_file_df.index:
        try:
            glue_trigger_name = csv_input_file_df['glue_trigger_name'][index]
            print(f"\n{current_datetime()} :: main :: info - deleting glue trigger '{glue_trigger_name}' ...")
            if glue_trigger_name.strip() != '':
                delete_trigger_response = glue_client.delete_trigger(Name=glue_trigger_name)
                if delete_trigger_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print(f"{current_datetime()} :: main :: info - successfully created glue trigger '{glue_trigger_name}'")
                else:
                    print(f"{current_datetime()} :: main :: error - failed to create glue trigger '{glue_trigger_name}'")

        except Exception as err:
            print(f"{current_datetime()} :: main :: error - failed to create glue trigger '{glue_trigger_name}'")
            print("error details : ", err)
            # raise err
    else:
        print(f"\n{current_datetime()} :: main :: info - creation of glue triggers - ended\n")
        print("*" * format_length)
    time.sleep(1)
