import sys
from time import sleep

import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime

#########################################################################################################
# variable declaration
from awsglue.utils import getResolvedOptions

format_length = 150
format_length_inner = 75
environment = "P2_PROD"
region_name = "us-east-1"
delimiter = ","
s3_bucket = "eurekapatient-j1"
outbound_folder = "prod-jbi/config/rerun-params/"
outbound_filename = outbound_folder + "p2_glue_jobs_list.csv"
columns_list = ['glue_trigger_name', 'glue_trigger_type', 'glue_workflow_name', 'glue_predecessor_job_name',
                'glue_predecessor_job_state', 'glue_job_name', 'glue_job_description', 'glue_job_loguri',
                'glue_job_role', 'glue_job_executionproperty_maxconcurrentruns', 'glue_job_command_name',
                'glue_job_command_scriptlocation', 'glue_job_command_pythonversion',
                'glue_job_defaultarguments_job_language', 'glue_job_defaultarguments_temp_dir',
                'glue_job_defaultarguments_extra_py_files', 'glue_job_defaultarguments_extra_jars',
                'glue_job_defaultarguments_extra_files', 'glue_job_defaultarguments_job_bookmark_option',
                'glue_job_connections', 'glue_job_maxretries', 'glue_job_timeout', 'glue_job_tags_env',
                'glue_job_glue_version', 'glue_job_workertype', 'glue_job_numberofworkers', 'glue_job_maxcapacity',
                'job_parameters']
dev_tag = 'Janssen-Prod'
exclude_list = []


#########################################################################################################


def get_default_arguments(get_job_response):
    if 'workertype' in get_job_response.keys():
        workertype = get_job_response['WorkerType']
        numberofworkers = get_job_response['NumberOfWorkers']
    else:
        workertype = 'Standard'
        numberofworkers = 1
    if 'DefaultArguments' in get_job_response['Job'].keys():
        sample = get_job_response['Job']['DefaultArguments']
        # print(sample.keys())
        exclude_keys = ['--extra-py-files', '--enable-job-insights', '--job-bookmark-option', '--job-language', '--TempDir']
        job_args = {}
        for key in sample:
            if key not in exclude_keys:
                job_args[key] = sample[key]

        print(f"job_args :: {job_args}")
        job_args_str = ''
        for key in job_args:
            # print(f"{key}:: {job_args[key]}")
            job_args_str = job_args_str + key + "=" + (
                job_args[key] if key != '--conf' else ('"' + job_args[key] + '"')) + ","
            # print(f"job_args_str:: {job_args_str}")
        job_args_str = job_args_str[:-1]
        print(f"job_args_str :: {job_args_str}")
    else:
        job_args_str = ''
        print(f"'DefaultArguments' not present in get_job_response.keys()")
    return job_args_str, workertype, numberofworkers


def trg_wf_details(get_job_runs_response):
    predecessor_job_list = ''
    state = ''
    wf_nm = ''
    trigger_nm = ''
    glue_trigger_type = ''
    print(f"get_job_runs_response.keys() :: {get_job_runs_response.keys()}")
    if 'JobRuns' in get_job_runs_response.keys():
        print(f"get_job_runs_response['JobRuns'] :: {get_job_runs_response['JobRuns']}")
        if len(get_job_runs_response['JobRuns']) != 0:
            for i in range(len(get_job_runs_response['JobRuns'])):
                if 'TriggerName' in get_job_runs_response['JobRuns'][i].keys():
                    trigger_nm = get_job_runs_response['JobRuns'][i]['TriggerName']
                    print(trigger_nm)
                    response = glue_client.get_trigger(Name=trigger_nm)
                    sleep(2)
                    print(response)
                    if 'Predicate' in response['Trigger'].keys():
                        conditions = response['Trigger']['Predicate']['Conditions']
                        if len(conditions) > 0:
                            predecessor_job_list = ''
                            for key in conditions:
                                predecessor_job_list = predecessor_job_list + key['JobName'] + ","
                                state = key['State']
                            predecessor_job_list = predecessor_job_list[:-1]
                    else:
                        predecessor_job_list = ''
                        state = ''
                    print(f"predecessor_job_list:: {predecessor_job_list}")
                    print(f"state:: {state}")
                    glue_trigger_type = response['Trigger']['Type']
                    wf_nm = response['Trigger']['WorkflowName']
                    print(wf_nm)
                    print(f"wf_nm:: {wf_nm}")
                    print(f"glue_trigger_type:: {glue_trigger_type}")
                    break
    else:
        print(f"Trigger not available , hence not able to derive")


    return predecessor_job_list, state, wf_nm, trigger_nm, glue_trigger_type

#########################################################################################################
# function - to get current date and time for logging
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


#########################################################################################################

#########################################################################################################
# function - to get job details for a list of jobs
def get_job_details(job_list , outbound_filename , outbound_filename_excel):
    try:
        print(f"\n{current_datetime()} :: main :: info - get job details - STARTED\n")
        job_csv = []
        # job_list = ['tg2-batch-log-ingestion']
        for job in job_list:

            print(f"\n\n\n\n{current_datetime()} :: main :: info - processing job - {job}")
            # print(f"\n{current_datetime()} :: main :: info - fetching job details for {job} - STARTED")
            get_job_response = glue_client.get_job( JobName = job )
            print(f"\n\n\n\n\nget_job_response : {get_job_response}\n\n\n\n")
            job_dict = {}
            job_args_str, workertype, numberofworkers = get_default_arguments(get_job_response)
            get_job_runs_response = glue_client.get_job_runs(JobName = job)
            predecessor_job_list, state, wf_nm, trigger_nm, glue_trigger_type = trg_wf_details(get_job_runs_response)

            # fetching the parameters from response
            glue_trigger_name											=   trigger_nm
            glue_trigger_type											=	glue_trigger_type
            glue_workflow_name											=	wf_nm
            glue_predecessor_job_name									=	predecessor_job_list
            glue_predecessor_job_state									=	state
            glue_job_name												=	get_job_response.get('Job', {}).get('Name' ,'')
            glue_job_description										=	get_job_response.get('Job', {}).get('Name' ,'')
            glue_job_loguri												=	""
            glue_job_role												=	get_job_response.get('Job', {}).get('Role' ,'')
            glue_job_executionproperty_maxconcurrentruns				=	get_job_response.get('Job', {}).get('ExecutionProperty', {}).get \
                ('MaxConcurrentRuns' ,'')
            glue_job_command_name										=	get_job_response.get('Job', {}).get('Command', {}).get('Name' ,'')
            glue_job_command_scriptlocation								=	get_job_response.get('Job', {}).get('Command', {}).get('ScriptLocation'
                                                                                                     ,'').replace('dev'
                                                                                                                 ,'prod')
            glue_job_command_pythonversion								=	int(get_job_response.get('Job', {}).get('Command', {}).get('PythonVersion' ,''))
            glue_job_defaultarguments_job_language						=	get_job_response.get('Job', {}).get('DefaultArguments', {}).get \
                ('--job-language' ,'')
            glue_job_defaultarguments_temp_dir							=	get_job_response.get('Job', {}).get('DefaultArguments', {}).get \
                ('--TempDir' ,'').replace('dev' ,'prod')
            glue_job_defaultarguments_extra_py_files					=	get_job_response.get('Job', {}).get('DefaultArguments', {}).get \
                ('--extra-py-files' ,'').replace('dev' ,'prod')
            glue_job_defaultarguments_extra_jars						=	get_job_response.get('Job', {}).get('DefaultArguments', {}).get \
                ('--extra-jars' ,'').replace('dev' ,'prod')
            glue_job_defaultarguments_extra_files						=	""
            glue_job_defaultarguments_job_bookmark_option				=	get_job_response.get('Job', {}).get('DefaultArguments', {}).get \
                ('--job-bookmark-option' ,'')
            glue_job_connections_temp									=	[]
            for conn in get_job_response.get('Job', {}).get('Connections', {}).get('Connections' ,''):
                glue_job_connections_temp.append(conn.replace('dev' ,'prod'))
            else:
                glue_job_connections									=	",".join(glue_job_connections_temp)
            glue_job_maxretries											=	get_job_response.get('Job', {}).get('MaxRetries' ,'')
            glue_job_timeout											=	get_job_response.get('Job', {}).get('Timeout' ,'')
            glue_job_tags_env											=	dev_tag.replace('DEV' ,'PROD')
            glue_job_glue_version										=	get_job_response.get('Job', {}).get('GlueVersion' ,'') if get_job_response.get('Job', {}).get('GlueVersion' ,'') != '' else 1
            glue_job_workertype											=	workertype
            glue_job_numberofworkers									=	numberofworkers
            job_parameters											    =	job_args_str


            # adding required details to dictionary
            job_dict['glue_trigger_name']								=	glue_trigger_name
            job_dict['glue_trigger_type']								=	glue_trigger_type
            job_dict['glue_workflow_name']								=	glue_workflow_name
            job_dict['glue_predecessor_job_name']						=	glue_predecessor_job_name
            job_dict['glue_predecessor_job_state']						=	glue_predecessor_job_state
            job_dict['glue_job_name']									=	glue_job_name
            job_dict['glue_job_description']							=	glue_job_description
            job_dict['glue_job_loguri']									=	glue_job_loguri
            job_dict['glue_job_role']									=	glue_job_role
            job_dict['glue_job_executionproperty_maxconcurrentruns']	=	glue_job_executionproperty_maxconcurrentruns
            job_dict['glue_job_command_name']							=	glue_job_command_name
            job_dict['glue_job_command_scriptlocation']					=	glue_job_command_scriptlocation
            job_dict['glue_job_command_pythonversion']					=	glue_job_command_pythonversion
            job_dict['glue_job_defaultarguments_job_language']			=	glue_job_defaultarguments_job_language
            job_dict['glue_job_defaultarguments_temp_dir']				=	glue_job_defaultarguments_temp_dir
            job_dict['glue_job_defaultarguments_extra_py_files']		=	glue_job_defaultarguments_extra_py_files
            job_dict['glue_job_defaultarguments_extra_jars']			=	glue_job_defaultarguments_extra_jars
            job_dict['glue_job_defaultarguments_extra_files']			=	glue_job_defaultarguments_extra_files
            job_dict['glue_job_defaultarguments_job_bookmark_option']	=	glue_job_defaultarguments_job_bookmark_option
            job_dict['glue_job_connections']							=	glue_job_connections
            job_dict['glue_job_maxretries']								=	glue_job_maxretries
            job_dict['glue_job_timeout']								=	glue_job_timeout
            job_dict['glue_job_tags_env']								=	glue_job_tags_env
            job_dict['glue_job_glue_version']							=	glue_job_glue_version
            job_dict['glue_job_workertype']								=	glue_job_workertype
            job_dict['glue_job_numberofworkers']						=	glue_job_numberofworkers
            job_dict['job_parameters']								    =	job_parameters
            job_dict['Created_On']                                      =   get_job_response.get('Job', {}).get('CreatedOn' ,'').replace(tzinfo=None)


            print(f"glue_job_name                                       :	{glue_job_name}")
            print(f"glue_job_description                                :	{glue_job_description}")
            print(f"glue_job_role                                       :	{glue_job_role}")
            print \
                (f"glue_job_executionproperty_maxconcurrentruns        :	{glue_job_executionproperty_maxconcurrentruns}")
            print(f"glue_job_command_name                               :	{glue_job_command_name}")
            print(f"glue_job_command_scriptlocation                     :	{glue_job_command_scriptlocation}")
            print(f"glue_job_command_pythonversion                      :	{glue_job_command_pythonversion}")
            print(f"glue_job_defaultarguments_job_language              :	{glue_job_defaultarguments_job_language}")
            print(f"glue_job_defaultarguments_temp_dir                  :	{glue_job_defaultarguments_temp_dir}")
            print \
                (f"glue_job_defaultarguments_extra_py_files            :	{glue_job_defaultarguments_extra_py_files}")
            print(f"glue_job_defaultarguments_extra_jars                :	{glue_job_defaultarguments_extra_jars}")
            print \
                (f"glue_job_defaultarguments_job_bookmark_option       :	{glue_job_defaultarguments_job_bookmark_option}")
            print(f"glue_job_connections                                :	{glue_job_connections}")
            print(f"glue_job_maxretries                                 :	{glue_job_maxretries}")
            print(f"glue_job_timeout                                    :	{glue_job_timeout}")
            print(f"glue_job_tags_env                                   :	{glue_job_tags_env}")
            print(f"glue_job_glue_version                               :	{glue_job_glue_version}")
            print(f"glue_job_workertype                                 :	{glue_job_workertype}")
            print(f"glue_job_numberofworkers                            :	{glue_job_numberofworkers}")
            print(f"job_parameters                                 :	{job_parameters}")

            # print(f"\n\n\n\njob_dict : {job_dict}\n\n\n\n")

            job_csv.append(job_dict)

            print(f"{current_datetime()} :: main :: info - fetching job details for {job} - COMPLETED\n\n\n\n")
            print("*" * format_length_inner)


        else:

            job_details_df = pd.DataFrame.from_dict(job_csv)
            job_details_df = job_details_df.sort_values(by=['glue_workflow_name', 'Created_On'])

            ## writing to CSV file
            # print(f"\n{current_datetime()} :: main :: info - loading the job details to csv file buffer - STARTED")
            # csv_buffer = StringIO()
            # job_details_df = job_details_df.reindex( columns = columns_list )
            # job_details_df.to_csv( csv_buffer , sep = delimiter , index = False )
            # print(f"{current_datetime()} :: main :: info - loading the job details to csv file buffer - COMPLETED")
            # s3_client.put_object( Bucket = s3_bucket , Key = outbound_filename , Body = csv_buffer.getvalue() )
            # print(f"{current_datetime()} :: main :: info - pushed the csv file '{outbound_filename}' to S3")

            # writing to EXCEL file
            wr.s3.to_excel( job_details_df , outbound_filename_excel , sheet_name = 'Job Details' , index = False )
            print(f"{current_datetime()} :: main :: info - pushed the excel file '{outbound_filename_excel}' to S3")

    except Exception as err:
        print(f"\n{current_datetime()} :: main :: error - get job details - FAILED")
        print("error details : ", err)
        raise err
    else:
        print(f"\n{current_datetime()} :: main :: info - get job details - COMPLETED\n")
#########################################################################################################


#########################################################################################################
# create a boto3 client
print(f"\n{current_datetime()} :: main :: info - creating boto3 client - STARTED")
try:
    glue_client	= boto3.client( service_name = "glue" , region_name = region_name , verify = True )
    s3_client	= boto3.client( service_name = "s3"   , region_name = region_name , verify = True )
except Exception as err:
    print(f"{current_datetime()} :: main :: error - creating boto3 client - FAILED")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - creating boto3 client - COMPLETED\n")
    print("*" * format_length)
#########################################################################################################
print(':: main :: info - read the glue code parameters...')
try:
    args = getResolvedOptions(sys.argv, ['JOB_LIST'])
    job_list_arg = args['JOB_LIST'].split(",")
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - job_list_arg          : {job_list_arg}")

#########################################################################################################
# execute for each logo
print(f"\n{current_datetime()} :: main :: info - get job details for tag '{dev_tag}' - STARTED\n")
try:

    job_list = []
    outbound_filename_excel = "s3://" + s3_bucket + "/" + outbound_filename.replace('.csv' ,'.xlsx')
    print(f"\n{current_datetime()} :: main :: info - processing tag - '{dev_tag}'")

    print(f"\n{current_datetime()} :: main :: info - listing glue jobs for tag '{dev_tag}' - STARTED")
    list_jobs_response = glue_client.list_jobs( MaxResults = 1000, Tags = { 'Cost Center' : dev_tag } )
    if job_list_arg[0] == 'NA':
        job_list = list(set(list_jobs_response['JobNames']) - set(exclude_list))
    else:
        job_list = job_list_arg
    # job_list = ['prod-job-pp-acxiom_iqvia-sftp-pull-parallel']
    print(f"{current_datetime()} :: main :: info - listing glue jobs for tag '{dev_tag}' - COMPLETED")
    print(f"\n{current_datetime()} :: main :: info - '{dev_tag}' has '{len(job_list)}' jobs\n")

    print(f"{current_datetime()} :: main :: info - fetch job details for tag '{dev_tag}' - STARTED")
    get_job_details(job_list, outbound_filename, outbound_filename_excel)
    print(f"\n{current_datetime()} :: main :: info - fetch job details for tag '{dev_tag}' - COMPLETED\n\n")

    print("*" * format_length_inner)

except Exception as err:
    print(f"\n{current_datetime()} :: main :: error - get job details for tag '{dev_tag}' - FAILED")
    print("error details : ", err)
    raise err
else:
    print(f"\n{current_datetime()} :: main :: info - get job details for tag '{dev_tag}' - COMPLETED\n")
    print("*" * format_length)
#########################################################################################################