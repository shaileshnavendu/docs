import json
import sys
from file_download_utils import *
from awsglue.utils import getResolvedOptions
from smart_open import open

format_length = 150

CHUNK_SIZE = 6291456

# read the glue code arguments
# print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
print(':: main :: info - read the glue code parameters...')
try:
    args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'CONFIG_FILE', 'SNAPSHOT_DATE','FILE_NAME'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    snapshot_date = args['SNAPSHOT_DATE']
    file_name = args['FILE_NAME']

    if snapshot_date == "1900-01-01":
        snapshot_date = datetime.now().strftime('%Y-%m-%d')

except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket                   : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file              : {config_file}")
    print(f"{current_datetime()} :: main :: info - snapshot_date            : {snapshot_date}")
    print(f"{current_datetime()} :: main :: info - file_name                : {file_name}")

try:
    param_data = get_s3_object(bucket, config_file)
    param_contents = json.loads(param_data)
    secret_manager = 'acxiom_sdoh_sftp_push'
    region = 'us-east-1'
    db_credentials = get_credentials_from_sm(secret_manager, region)
    json_creds = json.loads(db_credentials["SecretString"].replace('“', '"'))
    SFTP_PORT = 22
    SFTP_HOST = json_creds['secret_sftp_server']
    SFTP_USERNAME = json_creds['secret_username']
    # SFTP_PASSWORD = json_creds['secret_password']
    SFTP_PASSWORD = 'TQ8hJ2vK3uB#vP7'
    src_file_path = f'{add_slash(param_contents["root_path"])}sdoh_outbound/{file_name}'
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the configuration details\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the config details\n")
    print(f"{current_datetime()} :: main :: info - hostname for FTP - {SFTP_HOST}\n")
    print(f"{current_datetime()} :: main :: info - port for FTP - {SFTP_PORT}\n")
    print(f"{current_datetime()} :: main :: info - username for FTP - {SFTP_USERNAME}\n")

ftp_connection = open_ftp_connection(SFTP_HOST, int(SFTP_PORT), SFTP_USERNAME, SFTP_PASSWORD)
if ftp_connection == "conn_error":
    print("Failed to connect FTP Server!")
elif ftp_connection == "auth_error":
    print("Incorrect username or password!")
else:
    print("Connected Successfully !!!")
    remote_file_list = ftp_connection.listdir()
    print(f"remote_file_list :: {remote_file_list}")
    obj_full_nm = bucket_key_to_s3_path(bucket, src_file_path)
    if not s3_path_exists(obj_full_nm):
        raise Exception(f"file {src_file_path} not found on {bucket}")

    remote_file_path = "/" + obj_full_nm.split("/")[-1]
    print(f"src_file_path ::> {obj_full_nm}")
    print(f"remote_file_path ::> {remote_file_path}")
    file_obj = open(obj_full_nm, 'r')
    ftp_connection.putfo(file_obj, remotepath=remote_file_path)
