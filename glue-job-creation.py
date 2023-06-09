import sys
import boto3
import pandas as pd
from datetime import datetime
from awsglue.utils import getResolvedOptions
from collections import defaultdict
import time
import csv


# function - to get current date and time for logging
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def upd_wf_props(wf_name, props):
    response = glue_client.update_workflow(
        Name=wf_name,
        DefaultRunProperties=props,
    )
    return response


# main function
if __name__ == "__main__":

    """args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'GLUE_JOBS_LIST_FILE'])
    bucket = args['S3_BUCKET']
    glue_jobs_list_file = args['GLUE_JOBS_LIST_FILE']"""

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

    # variable declaration
    format_length = 150
    format_length_inner = 75
    region_name = "us-east-1"
    glue_jobs_list_file_separator = ","

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
        csv_input_file_df = pd.read_csv(
            csv_input_file,
            sep=glue_jobs_list_file_separator,
            dtype=str,
            keep_default_na=False,
            na_filter=False)
    except Exception as err:
        print(
            f"{current_datetime()} :: main :: error - failed to read the feed file '{glue_jobs_list_file}' from s3 bucket '{bucket}' into a panda dataframe")
        print("error details : ", err)
        raise err
    else:
        print(
            f"{current_datetime()} :: main :: info - successfully read the feed file '{glue_jobs_list_file}' from s3 bucket '{bucket}' into a panda dataframe\n")
        print("*" * format_length)

    # create glue workflow
    print(f"\n{current_datetime()} :: main :: info - creation of glue workflow - started")
    glue_workflow_name_prior = ""
    for index in csv_input_file_df.index:
        try:
            glue_workflow_name = csv_input_file_df['glue_workflow_name'][index]
            glue_job_tags_env = csv_input_file_df['glue_job_tags_env'][index]
            glue_wf_params = ""

            if len(glue_workflow_name) > 0 and glue_workflow_name_prior != glue_workflow_name:
                glue_workflow_name_prior = glue_workflow_name
                print(f"\n{current_datetime()} :: main :: info - creating glue workflow '{glue_workflow_name}' ...")

                delete_workflow_response = glue_client.delete_workflow(Name=glue_workflow_name)
                time.sleep(5)
                create_workflow_response = glue_client.create_workflow(
                    Name=glue_workflow_name,
                    Description=glue_workflow_name,
                    Tags={
                        'Cost Center': glue_job_tags_env
                    })

                if "glue_wf_params" in csv_input_file_df.columns:
                    glue_wf_params = csv_input_file_df['glue_wf_params'][index]

                if glue_wf_params != "":
                    upd_wf_props(glue_workflow_name, glue_wf_params)

                if create_workflow_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print(
                        f"{current_datetime()} :: main :: info - successfully created glue workflow '{glue_workflow_name}'")
                else:
                    print(
                        f"{current_datetime()} :: main :: error - failed to create glue workflow '{glue_workflow_name}'")

        except Exception as err:
            print(f"{current_datetime()} :: main :: error - failed to create glue workflow '{glue_workflow_name}'")
            print("error details : ", err)
            raise err
    else:
        print(f"\n{current_datetime()} :: main :: info - creation of glue workflow - ended\n")
        print("*" * format_length)

    # create glue jobs
    print(f"\n{current_datetime()} :: main :: info - creation of glue jobs - started")
    for index in csv_input_file_df.index:
        print(
            f"\n{current_datetime()} :: main :: info - creating glue job '{csv_input_file_df['glue_workflow_name'][index]}_{csv_input_file_df['glue_job_name'][index]}' ...\n")
        try:
            glue_job_name = csv_input_file_df['glue_job_name'][index]
            glue_job_description = csv_input_file_df['glue_job_description'][index]
            glue_job_loguri = csv_input_file_df['glue_job_loguri'][index]
            glue_job_role = csv_input_file_df['glue_job_role'][index]
            glue_job_executionproperty_maxconcurrentruns = int(
                csv_input_file_df['glue_job_executionproperty_maxconcurrentruns'][index])
            glue_job_command_name = csv_input_file_df['glue_job_command_name'][index]
            glue_job_command_scriptlocation = csv_input_file_df['glue_job_command_scriptlocation'][index]
            glue_job_command_pythonversion = csv_input_file_df['glue_job_command_pythonversion'][index]
            glue_job_defaultarguments_job_language = csv_input_file_df['glue_job_defaultarguments_job_language'][
                index]
            glue_job_defaultarguments_temp_dir = csv_input_file_df['glue_job_defaultarguments_temp_dir'][index]
            glue_job_defaultarguments_extra_py_files = \
                csv_input_file_df['glue_job_defaultarguments_extra_py_files'][index]
            # glue_job_defaultarguments_extra_jars = csv_input_file_df['glue_job_defaultarguments_extra_jars'][index]
            # glue_job_defaultarguments_extra_files = csv_input_file_df['glue_job_defaultarguments_extra_files'][index]
            glue_job_defaultarguments_job_bookmark_option = \
                csv_input_file_df['glue_job_defaultarguments_job_bookmark_option'][index]
            glue_job_connections = csv_input_file_df['glue_job_connections'][index].split(",")
            glue_job_maxretries = int(csv_input_file_df['glue_job_maxretries'][index])
            glue_job_timeout = int(csv_input_file_df['glue_job_timeout'][index])
            glue_job_tags_env = csv_input_file_df['glue_job_tags_env'][index]
            glue_job_glue_version = csv_input_file_df['glue_job_glue_version'][index]
            glue_job_workertype = csv_input_file_df['glue_job_workertype'][index]
            glue_job_numberofworkers = int(csv_input_file_df['glue_job_numberofworkers'][index])
            glue_job_params = csv_input_file_df['job_parameters'][index]

            print(f"{current_datetime()} :: main :: info - glue_job_name : '{glue_job_name}'")
            print(f"{current_datetime()} :: main :: info - glue_job_description : '{glue_job_description}'")
            print(f"{current_datetime()} :: main :: info - glue_job_loguri : '{glue_job_loguri}'")
            print(f"{current_datetime()} :: main :: info - glue_job_role : '{glue_job_role}'")
            print(
                f"{current_datetime()} :: main :: info - glue_job_executionproperty_maxconcurrentruns : '{glue_job_executionproperty_maxconcurrentruns}'")
            print(f"{current_datetime()} :: main :: info - glue_job_command_name : '{glue_job_command_name}'")
            print(
                f"{current_datetime()} :: main :: info - glue_job_command_scriptlocation : '{glue_job_command_scriptlocation}'")
            print(
                f"{current_datetime()} :: main :: info - glue_job_command_pythonversion : '{glue_job_command_pythonversion}'")
            print(
                f"{current_datetime()} :: main :: info - glue_job_defaultarguments_job_language : '{glue_job_defaultarguments_job_language}'")
            print(
                f"{current_datetime()} :: main :: info - glue_job_defaultarguments_temp_dir : '{glue_job_defaultarguments_temp_dir}'")
            print(
                f"{current_datetime()} :: main :: info - glue_job_defaultarguments_extra_py_files : '{glue_job_defaultarguments_extra_py_files}'")
            print(
                f"{current_datetime()} :: main :: info - glue_job_defaultarguments_job_bookmark_option : '{glue_job_defaultarguments_job_bookmark_option}'")
            print(f"{current_datetime()} :: main :: info - glue_job_connections : '{glue_job_connections}'")
            print(f"{current_datetime()} :: main :: info - glue_job_maxretries : '{glue_job_maxretries}'")
            print(f"{current_datetime()} :: main :: info - glue_job_timeout : '{glue_job_timeout}'")
            print(f"{current_datetime()} :: main :: info - glue_job_tags_env : '{glue_job_tags_env}'")
            print(f"{current_datetime()} :: main :: info - glue_job_glue_version : '{glue_job_glue_version}'")
            print(f"{current_datetime()} :: main :: info - glue_job_workertype : '{glue_job_workertype}'")
            print(
                f"{current_datetime()} :: main :: info - glue_job_numberofworkers : '{glue_job_numberofworkers}'\n")
            print(f"{current_datetime()} :: main :: info - glue_job_params : '{glue_job_params}'\n")

            delete_job_response = glue_client.delete_job(JobName=glue_job_name)
            defArgs = {
                '--job-language': glue_job_defaultarguments_job_language,
                '--TempDir': glue_job_defaultarguments_temp_dir,
                '--extra-py-files': glue_job_defaultarguments_extra_py_files,
                '--job-bookmark-option': glue_job_defaultarguments_job_bookmark_option
            }
            if glue_job_params.strip() != '':
                # param_list = (glue_job_params.split(","))
                param_list = list(
                    csv.reader([glue_job_params], delimiter=',', quotechar='"', skipinitialspace=True))[0]
                print(f"debug : {param_list}")
                for item in param_list:
                    key = item.split("=")[0].strip(" ")
                    val = '='.join(item.split("=")[1:]).strip(" ").strip('"')
                    defArgs[key] = val

            if glue_job_command_name.lower() == "glueetl":
                time.sleep(1)
                connection_string = "Connections={'Connections': glue_job_connections}," if len(
                    glue_job_connections[0]) != 0 else ''

                create_job_stmnt = """create_job_response = glue_client.create_job(Name=glue_job_name,
                    Description=glue_job_description,
                    # LogUri=glue_job_loguri,
                    Role=glue_job_role,
                    ExecutionProperty={
                        'MaxConcurrentRuns':
                            glue_job_executionproperty_maxconcurrentruns
                    },
                    Command={
                        'Name': glue_job_command_name,
                        'ScriptLocation': glue_job_command_scriptlocation,
                        'PythonVersion': glue_job_command_pythonversion
                    },
                    DefaultArguments=defArgs,
                    """ + connection_string + """
                    MaxRetries=glue_job_maxretries,
                    Timeout=glue_job_timeout,
                    Tags={'Cost Center': glue_job_tags_env},
                    GlueVersion=glue_job_glue_version,
                    WorkerType=glue_job_workertype,
                    NumberOfWorkers=glue_job_numberofworkers )"""
                print(f"create_job_stmnt ==> {create_job_stmnt}")
                time.sleep(5)
                exec(create_job_stmnt)
            elif glue_job_command_name.lower() == "pythonshell":
                connection_string = "Connections={'Connections': glue_job_connections}," if len(
                    glue_job_connections[0]) != 0 else ''
                time.sleep(1)
                create_job_stmnt = """create_job_response = glue_client.create_job(
                    Name=glue_job_name,
                    Description=glue_job_description,
                    # LogUri=glue_job_loguri,
                    Role=glue_job_role,
                    ExecutionProperty={
                        "MaxConcurrentRuns":
                            glue_job_executionproperty_maxconcurrentruns
                    },
                    Command={
                        "Name": glue_job_command_name,
                        "ScriptLocation": glue_job_command_scriptlocation,
                        "PythonVersion": glue_job_command_pythonversion,
                    },
                    DefaultArguments=defArgs,
                    """ + connection_string + """
                    MaxRetries=glue_job_maxretries,
                    Timeout=glue_job_timeout,
                    Tags={"Cost Center": glue_job_tags_env},
                    GlueVersion=glue_job_glue_version
                )"""
                print(f"create_job_stmnt ==> {create_job_stmnt}")
                time.sleep(5)
                exec(create_job_stmnt)
            else:
                raise Exception("Invalid glue_job_command_name : ", glue_job_command_name)

            if create_job_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"{current_datetime()} :: main :: info - successfully created glue job '{glue_job_name}'\n")
                print("*" * format_length_inner)
            else:
                print(f"{current_datetime()} :: main :: error - failed to create glue job '{glue_job_name}'")

        except Exception as err:
            print(f"{current_datetime()} :: main :: error - failed to create glue job '{glue_job_name}'")
            print("error details : ", err)
            raise err
    else:
        print(f"\n{current_datetime()} :: main :: info - creation of glue jobs - ended\n")
        print("*" * format_length)

    # create glue triggers
    print(f"\n{current_datetime()} :: main :: info - creation of glue triggers - started")

    tgr_details = csv_input_file_df[csv_input_file_df.glue_trigger_name.str.lower() != ''][
        ['glue_trigger_name', 'glue_job_name', 'glue_predecessor_job_name', 'glue_workflow_name']]
    tgr_seq = tgr_details.fillna('').values.tolist()
    dic = defaultdict(list)
    di_w = defaultdict(str)
    for k, v, n, w in tgr_seq:
        dic[k].append(v)
        di_w[k] = di_w[k] + n


    def get_wf_frm_seq(seq_num):
        for i in tgr_seq:
            if i[0] == seq_num:
                return i[3]


    for ts, wj in di_w.items():
        print('trigger seq :', ts)
        job_na = dic[ts]
        job_n_l = [i for i in job_na]
        job_name = ','.join(job_n_l)
        workflow_name = get_wf_frm_seq(ts)
        print('job  name :', job_name)
        print('workflow name :', workflow_name)

        trigger_name = ts
        if len(trigger_name) > 200:
            trigger_name = trigger_name[0:20]

        print("trigger_name: ", trigger_name)

        trigger_deleted = glue_client.delete_trigger(
            Name=trigger_name
        )

        print("trigger_deleted: ", trigger_deleted)

        # #Create first OnDemand trigger
        if wj == '':
            j_n = job_name.split(',')
            j_n_1 = [{'JobName': i} for i in j_n]
            time.sleep(5)
            trigger_created = glue_client.create_trigger(
                Name=trigger_name,
                WorkflowName=workflow_name,
                Type='ON_DEMAND',
                Actions=j_n_1
            )

            # watched_job_name = job_name
            print("trigger_created - OnDemandTrigger", trigger_created)

        else:
            # Create conditional triggers
            w_j_n = wj.split(',')
            wa_j_n = [i for i in w_j_n]
            con = []

            if len(wa_j_n) > 1:
                for i in wa_j_n:
                    di = {'LogicalOperator': 'EQUALS', 'JobName': i, 'State': 'SUCCEEDED', }
                    con.append(di)
            else:
                di = {'LogicalOperator': 'EQUALS', 'JobName': wa_j_n[0], 'State': 'SUCCEEDED', }
                con.append(di)
            time.sleep(5)
            trigger_created = glue_client.create_trigger(
                Name=trigger_name,
                WorkflowName=workflow_name,
                Type='CONDITIONAL',
                Predicate={
                    'Logical': 'AND',
                    'Conditions': con
                },
                Actions=[
                    {
                        'JobName': job_name,
                    }
                ],
                StartOnCreation=True
            )

            # watched_job_name = job_name
            print("trigger_created - ConditionalTrigger - ", trigger_created)

    print(f"\n{current_datetime()} :: main :: info - creation of glue triggers - ended\n")
    print("*" * format_length)
