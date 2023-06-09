import sys
from awsglue.utils import getResolvedOptions
from control_framework_utils import *

glue_client = boto3.client("glue")


def wait_for_jobs_completion(job_nm, jr_dict):
    # wait for job completion
    tbl_li = list(jr_dict)              # fetch table list
    counter = len(tbl_li)               # create a counter with number of tables
    job_sta_dict = {}                   # initializing dictionary to capture job status
    fail_list = []                      # capturing failed job list
    aws_failures_dict = {}              #aws failures retry dict
    # Keep iterating till all the job executions completed
    while counter > 0:
        print(f"{current_datetime()} :: wait_for_jobs_completion : waiting for job completion for - {tbl_li}")
        time.sleep(60)
        # iterating through each source to get status of job
        for tbl in tbl_li:
            if jr_dict[tbl] is None:
                fail_list.append(tbl)
                tbl_li.remove(tbl)
                counter = counter - 1
                job_sta_dict[tbl] = "COULD NOT TRIGGER"
                continue

            response = glue_client.get_job_run(
                JobName=job_nm,
                RunId=jr_dict[tbl]
            )
            job_sta = response['JobRun']['JobRunState']
            job_sta_dict[tbl] = job_sta

            # if job run not completed, keep waiting & check for next table
            if job_sta in ['STARTING', 'RUNNING', 'STOPPING']:
                continue
            # If job run completed, remove completed job from iteration, update the counter and failed job list.
            else:
                print(
                    f"{current_datetime()} :: wait_for_jobs_completion : Job {job_nm} for {tbl} with job run id {jr_dict[tbl]} completed with status : {job_sta}")
                tbl_li.remove(tbl)
                counter = counter - 1
                if job_sta != 'SUCCEEDED':
                    fail_list.append(tbl)
                    if job_sta == 'FAILED' and 'ErrorMessage' in response['JobRun']: #script below is to handle aws failures and send to retry dict
                        job_error_msg = response['JobRun']['ErrorMessage']
                        dpu = 0
                        if 'NumberOfWorkers' in response['JobRun'] and response['JobRun']['NumberOfWorkers']>0:
                            dpu = response['JobRun']['NumberOfWorkers']
                        isAwsFailure = check_if_aws_failure(job_error_msg)
                        if isAwsFailure:
                            aws_failures_dict[tbl]= dpu
    return job_sta_dict, fail_list, aws_failures_dict


def get_directory_list_to_backup(file_name):
    buc, key = s3_path_to_bucket_key(file_name)
    content = get_s3_object(buc, key)
    df = pd.read_excel(BytesIO(content), "backup_config", dtype=str).fillna("NA")
    df = df[df.required.str.strip().str.lower() == 'y']
    return df


# config_file = "prod/config/prod_params.json"
# bucket = "eurekapatient-j1-prod"
# job_name = "prod-job-ep-data_backup"

# read the glue code arguments
def run_job(layer_to_tgt_df):
    jr_di = {}

    for index in layer_to_tgt_df.index:
        layer = layer_to_tgt_df["layer_directory"][index]
        src_name = layer_to_tgt_df["source_directory"][index]
        backup_suffix = layer_to_tgt_df["backup_directory_suffix"][index]
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--CONFIG_FILE': config_file,
                '--S3_BUCKET': bucket,
                '--TGT_NAME': src_name,
                '--LAYER': layer,
                "--ARN": arn,
                "--BACKUP_SUFFIX": backup_suffix
            }
        )
        print(response)
        job_run_id = response['JobRunId']
        jr_di[layer + " - " + src_name] = job_run_id

    job_sta_dict, fail_list, aws_failures_dict = wait_for_jobs_completion(job_name, jr_di)
    print(json.dumps(job_sta_dict, indent=4))
    print(fail_list)
    # if there are failed jobs, raise an exception
    if len(fail_list) > 0:
        raise Exception(f"{len(fail_list)} jobs failed; failed job list is {fail_list}")

print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'JOB_NAME', 'ARN'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    job_name = args['JOB_NAME']
    arn = args['ARN']
except Exception as e:
    print(f"{current_datetime()} :: main :: error - could not read glue code arguments\n")
    print("error details : ", e)
    raise e
else:
    print(f"{current_datetime()} :: main :: info - bucket           : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file      : {config_file}")
    print(f"{current_datetime()} :: main :: info - job_name         : {job_name}")
    print(f"{current_datetime()} :: main :: info - arn              : {arn}")

# parse the config file contents
print(f"\n{current_datetime()} :: main :: info - reading the config file {config_file} in bucket {bucket} ...\n")
try:
    filename = get_s3_object(bucket, config_file)
    param_contents = json.loads(filename)
    table_cfg = param_contents["tables_config"]
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the config file {config_file} in bucket {bucket}")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the config file {config_file} in bucket {bucket}\n")
    print(f"{current_datetime()} :: main :: info - table_cfg         : {table_cfg}")

# get directory list to backup
layer_to_tgt_df = get_directory_list_to_backup(table_cfg)
print(layer_to_tgt_df)

if 'load_order' in layer_to_tgt_df.columns:
    print(f"{current_datetime()} :: main :: info - found load_order column, running as per load order for required = 'Y'")
    for key, agg_df in layer_to_tgt_df.groupby(['load_order']):
        print(f"{current_datetime()} :: main :: info - starting for load order {key}")
        run_job(agg_df)
        print(f"{current_datetime()} :: main :: info - ending for load order {key}")
else:
    print(f"{current_datetime()} :: main :: info - load_order column not provided, running for required = 'Y'")
    run_job(layer_to_tgt_df)

print("Job completed successfully")