import json
import sys
from file_download_utils import *
from awsglue.utils import getResolvedOptions
format_length = 150

CHUNK_SIZE = 6291456
s3_client = boto3.client('s3')

def initiate_params(bucket, config_file):
    global  SFTP_HOST, SFTP_PORT, SFTP_USERNAME, SFTP_PASSWORD, src_file_path
    try:
        # param_data = get_s3_object(bucket, config_file)
        # param_contents = json.loads(param_data)
        secret_manager = 'acxiom_sdoh_sftp_pull'
        region = 'us-east-1'
        db_credentials = get_credentials_from_sm(secret_manager, region)
        json_creds = json.loads(db_credentials["SecretString"].replace('“', '"'))
        SFTP_PORT = 22
        SFTP_HOST = json_creds['secret_sftp_server']
        SFTP_USERNAME = json_creds['secret_username']
        # SFTP_PASSWORD = json_creds['secret_password']
        SFTP_PASSWORD = 'TQ8hJ2vK3uB#vP7'
        src_file_path = 'inbound-feed/sdoh/'
    except Exception as err:
        print(f"{current_datetime()} :: main :: error - failed to read the configuration details\n")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: main :: info - successfully read the config details\n")
        print(f"{current_datetime()} :: main :: info - hostname for FTP - {SFTP_HOST}\n")
        print(f"{current_datetime()} :: main :: info - port for FTP - {SFTP_PORT}\n")
        # print(f"{current_datetime()} :: main :: info - username for FTP - {SFTP_USERNAME}\n")


print(':: main :: info - read the glue code parameters...')
try:
    args = getResolvedOptions(sys.argv,
                              ['S3_BUCKET', 'CONFIG_FILE'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']


except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket                   : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file              : {config_file}")

print("*" * format_length)
initiate_params(bucket, config_file)
ftp_connection = open_ftp_connection(SFTP_HOST, int(SFTP_PORT), SFTP_USERNAME, SFTP_PASSWORD)
if ftp_connection == "conn_error":
    print("Failed to connect FTP Server!")
elif ftp_connection == "auth_error":
    print("Incorrect username or password!")
else:
    print("Connected Successfully !!!")
    file_list = ftp_connection.listdir()
    print(f"file-list present at the sftp location :: {file_list}")
    if len(file_list) > 0:
        for file in file_list:
            ftp_file_path = "/" + file
            version = file.split(".")[0].split("_")[-1]
            s3_file_path = add_slash(src_file_path) + add_slash(version) + file
            try:
                transfer_file_from_ftp_to_s3(
                    bucket,
                    ftp_file_path,
                    s3_file_path,
                    SFTP_USERNAME,
                    SFTP_PASSWORD,
                    CHUNK_SIZE, SFTP_HOST, SFTP_PORT)
            except Exception as e:
                print(f"File {file} could not be copied due to following error :: {e}")
            else:
                print(f"File {file} copied successfully at {s3_file_path}")
                time.sleep(10)
