import json
import sys
import gzip

from file_download_utils import *
from awsglue.utils import getResolvedOptions

format_length = 150

CHUNK_SIZE = 6291456


def initiate_params(bucket, config_file):
    global src_file_path, cur_file_path, SFTP_HOST, SFTP_PORT, SFTP_USERNAME, SFTP_PASSWORD
    try:
        param_data = get_s3_object(bucket, config_file)
        param_contents = json.loads(param_data)
        decrypt_key = "tR7nR6wZHGjYMCuV"
        key = decrypt_key.encode('UTF8')
        src_file_path = 'inbound-feed/iqvia_acxiom_crosswalk/'
        SFTP_PORT = 22
        SFTP_HOST = 'sftp.msa.com'
        SFTP_USERNAME = 'cctaidxfer'
        SFTP_PASSWORD = 'IOG#!76RTCgrin-bcd'
    except Exception as err:
        print(f"{current_datetime()} :: main :: error - failed to read the configuration details\n")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: main :: info - successfully read the config details\n")
        print(f"{current_datetime()} :: main :: info - src file path - {src_file_path}\n")
        print(f"{current_datetime()} :: main :: info - hostname for FTP - {SFTP_HOST}\n")
        print(f"{current_datetime()} :: main :: info - port for FTP - {SFTP_PORT}\n")
        print(f"{current_datetime()} :: main :: info - username for FTP - {SFTP_USERNAME}\n")
    return (key)


# read the glue code arguments
# print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
print(':: main :: info - read the glue code parameters...')
try:
    args = getResolvedOptions(sys.argv,
                              ['S3_BUCKET', 'CONFIG_FILE', 'TABLE_NAME', 'VERSION'])
    bucket = args['S3_BUCKET']
    config_file = args['CONFIG_FILE']
    table_name = args['TABLE_NAME'].lower()
    version = args['VERSION']

    # bucket = 'eurekapatient-j1'
    # config_file = 'prod-jbi/config/params/prod_params.json'
    # table_name = 'sdoh.msapt'
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket                   : {bucket}")
    print(f"{current_datetime()} :: main :: info - config_file              : {config_file}")

print("*" * format_length)
key = initiate_params(bucket, config_file)
src_file_path = src_file_path + add_slash(version)
ftp_connection = open_ftp_connection(SFTP_HOST, int(SFTP_PORT), SFTP_USERNAME, SFTP_PASSWORD)
if ftp_connection == "conn_error":
    print("Failed to connect FTP Server!")
elif ftp_connection == "auth_error":
    print("Incorrect username or password!")
else:
    print("Connected Successfully !!!")
    files_tobe_copied = ftp_connection.listdir(path="/")
    print(f"file_list :: {files_tobe_copied}")
    # files_tobe_copied = []
    # for file in file_list:
    #     if file not in []
    #         if table_name in file.lower():
    #             files_tobe_copied.append(file)

    # file_nm = file.split('.')[0]
    # file_nm = ''.join(i for i in file_nm if not i.isdigit()).strip('_').lower()
    # print(f"file_nm :: {file_nm}")
    # if file_nm == table_name:
    #     files_tobe_copied.append(file)

    print(f"List of files to be copied :{files_tobe_copied}")
    # if len(files_tobe_copied) != int(file_count):
    #     raise Exception(
    #         f"Count of files present at the SFTP location does not match with specified count in uat_table_list.")
    if len(files_tobe_copied) > 0:
        for file in files_tobe_copied:
            ftp_file_path = "/" + file
            s3_file_path = add_slash(src_file_path) + file
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

                s3 = boto3.resource("s3")
                obj = s3.Object(bucket, s3_file_path)
                with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
                    content = gzipfile.read()
                put_s3_object(bucket, s3_file_path.replace(".gz", ""), content)

                # decrypt_and_place(bucket, s3_file_path.replace(".gz", ""), key, 1, "iqvia_mood_claims")
                time.sleep(5)
