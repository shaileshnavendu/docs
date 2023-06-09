import sys

from file_download_utils import *
from awsglue.utils import getResolvedOptions

format_length = 150

CHUNK_SIZE = 6291456


def initiate_params(bucket):
    global  SFTP_HOST, SFTP_PORT, SFTP_USERNAME, SFTP_PASSWORD
    try:
        SFTP_PORT = 22
        SFTP_HOST = 'ftp-phx.symphonyhealth.com'
        SFTP_USERNAME = 'cjan026'
        SFTP_PASSWORD = 'v5PKGHidIL'
    except Exception as err:
        print(f"{current_datetime()} :: main :: error - failed to read the configuration details\n")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: main :: info - successfully read the config details\n")
        print(f"{current_datetime()} :: main :: info - hostname for FTP - {SFTP_HOST}\n")
        print(f"{current_datetime()} :: main :: info - port for FTP - {SFTP_PORT}\n")


# read the glue code arguments
# print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
print(':: main :: info - read the glue code parameters...')
try:
    args = getResolvedOptions(sys.argv,
                              ['S3_BUCKET', 'FOLDER'])
    bucket = args['S3_BUCKET']
    folder_name = args['FOLDER']

    # bucket = 'eurekapatient-j1'
    # folder_name = 'PSA'


except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - bucket                   : {bucket}")
    print(f"{current_datetime()} :: main :: info - folder_name              : {folder_name}")


print("*" * format_length)
initiate_params(bucket)
ftp_connection = open_ftp_connection(SFTP_HOST, int(SFTP_PORT), SFTP_USERNAME, SFTP_PASSWORD)
if ftp_connection == "conn_error":
    print("Failed to connect FTP Server!")
elif ftp_connection == "auth_error":
    print("Incorrect username or password!")
else:
    print("Connected Successfully !!!")
    # file_list = sorted(ftp_connection.listdir(path="/" + f'{folder_name}'))
    file_list = ['jan_pc_mpd.txt.gz', 'jan_pc_rx_201612.txt.gz', 'jan_pc_rx_201708.txt.gz', 'jan_pc_rx_201711.txt.gz', 'jan_pc_rx_201807.txt.gz', 'jan_pc_rx_201810.txt.gz', 'jan_pc_rx_201903.txt.gz', 'jan_pc_rx_201905.txt.gz', 'jan_pc_rx_201906.txt.gz', 'jan_pc_rx_201907.txt.gz', 'jan_pc_rx_201910.txt.gz', 'jan_pc_rx_202001.txt.gz', 'jan_pc_rx_202002.txt.gz', 'jan_pc_rx_202003.txt.gz', 'jan_pc_rx_202006.txt.gz', 'jan_pc_rx_202009.txt.gz', 'jan_pc_rx_202010.txt.gz', 'jan_pc_rx_202011.txt.gz', 'jan_pc_rx_202102.txt.gz', 'jan_pc_rx_202105.txt.gz', 'jan_pc_rx_202108.txt.gz', 'jan_pc_rx_202109.txt.gz', 'jan_pc_rx_202110.txt.gz', 'jan_pc_rx_202111.txt.gz', 'jan_pc_rx_202201.txt.gz', 'jan_pc_rx_202202.txt.gz', 'jan_pc_rx_202203.txt.gz', 'jan_pc_rx_202204.txt.gz', 'jan_pc_rx_202205.txt.gz', 'jan_pc_rx_202206.txt.gz', 'jan_pc_rx_202207.txt.gz', 'jan_pc_rx_202208.txt.gz', 'jan_pc_rx_202209.txt.gz', 'jan_pc_rx_202210.txt.gz', 'jan_pc_rx_202211.txt.gz', 'jan_pc_rx_202212.txt.gz', 'jan_pc_rx_202301.txt.gz', 'jan_pc_rx_non_market_201001.txt.gz', 'jan_pc_rx_non_market_201002.txt.gz', 'jan_pc_rx_non_market_201003.txt.gz', 'jan_pc_rx_non_market_201004.txt.gz', 'jan_pc_rx_non_market_201005.txt.gz', 'jan_pc_rx_non_market_201006.txt.gz', 'jan_pc_rx_non_market_201007.txt.gz', 'jan_pc_rx_non_market_201008.txt.gz', 'jan_pc_rx_non_market_201009.txt.gz', 'jan_pc_rx_non_market_201010.txt.gz', 'jan_pc_rx_non_market_201011.txt.gz', 'jan_pc_rx_non_market_201012.txt.gz', 'jan_pc_rx_non_market_201101.txt.gz', 'jan_pc_rx_non_market_201102.txt.gz', 'jan_pc_rx_non_market_201103.txt.gz', 'jan_pc_rx_non_market_201104.txt.gz', 'jan_pc_rx_non_market_201105.txt.gz', 'jan_pc_rx_non_market_201106.txt.gz', 'jan_pc_rx_non_market_201107.txt.gz', 'jan_pc_rx_non_market_201108.txt.gz', 'jan_pc_rx_non_market_201109.txt.gz', 'jan_pc_rx_non_market_201110.txt.gz', 'jan_pc_rx_non_market_201111.txt.gz', 'jan_pc_rx_non_market_201112.txt.gz', 'jan_pc_rx_non_market_201201.txt.gz', 'jan_pc_rx_non_market_201202.txt.gz', 'jan_pc_rx_non_market_201203.txt.gz', 'jan_pc_rx_non_market_201204.txt.gz', 'jan_pc_rx_non_market_201205.txt.gz', 'jan_pc_rx_non_market_201206.txt.gz', 'jan_pc_rx_non_market_201207.txt.gz', 'jan_pc_rx_non_market_201208.txt.gz', 'jan_pc_rx_non_market_201209.txt.gz', 'jan_pc_rx_non_market_201210.txt.gz', 'jan_pc_rx_non_market_201211.txt.gz', 'jan_pc_rx_non_market_201212.txt.gz', 'jan_pc_rx_non_market_201301.txt.gz', 'jan_pc_rx_non_market_201302.txt.gz', 'jan_pc_rx_non_market_201303.txt.gz', 'jan_pc_rx_non_market_201304.txt.gz', 'jan_pc_rx_non_market_201305.txt.gz', 'jan_pc_rx_non_market_201306.txt.gz', 'jan_pc_rx_non_market_201307.txt.gz', 'jan_pc_rx_non_market_201308.txt.gz', 'jan_pc_rx_non_market_201309.txt.gz', 'jan_pc_rx_non_market_201310.txt.gz', 'jan_pc_rx_non_market_201311.txt.gz', 'jan_pc_rx_non_market_201312.txt.gz', 'jan_pc_rx_non_market_201401.txt.gz', 'jan_pc_rx_non_market_201402.txt.gz', 'jan_pc_rx_non_market_201403.txt.gz', 'jan_pc_rx_non_market_201404.txt.gz', 'jan_pc_rx_non_market_201405.txt.gz', 'jan_pc_rx_non_market_201406.txt.gz', 'jan_pc_rx_non_market_201407.txt.gz', 'jan_pc_rx_non_market_201408.txt.gz', 'jan_pc_rx_non_market_201409.txt.gz', 'jan_pc_rx_non_market_201410.txt.gz', 'jan_pc_rx_non_market_201411.txt.gz', 'jan_pc_rx_non_market_201412.txt.gz', 'jan_pc_rx_non_market_201501.txt.gz', 'jan_pc_rx_non_market_201502.txt.gz', 'jan_pc_rx_non_market_201503.txt.gz', 'jan_pc_rx_non_market_201504.txt.gz', 'jan_pc_rx_non_market_201505.txt.gz', 'jan_pc_rx_non_market_201506.txt.gz', 'jan_pc_rx_non_market_201507.txt.gz', 'jan_pc_rx_non_market_201508.txt.gz', 'jan_pc_rx_non_market_201509.txt.gz', 'jan_pc_rx_non_market_201510.txt.gz', 'jan_pc_rx_non_market_201511.txt.gz', 'jan_pc_rx_non_market_201512.txt.gz', 'jan_pc_rx_non_market_201601.txt.gz', 'jan_pc_rx_non_market_201602.txt.gz', 'jan_pc_rx_non_market_201603.txt.gz', 'jan_pc_rx_non_market_201604.txt.gz', 'jan_pc_rx_non_market_201605.txt.gz', 'jan_pc_rx_non_market_201606.txt.gz', 'jan_pc_rx_non_market_201607.txt.gz', 'jan_pc_rx_non_market_201608.txt.gz', 'jan_pc_rx_non_market_201609.txt.gz', 'jan_pc_rx_non_market_201610.txt.gz', 'jan_pc_rx_non_market_201611.txt.gz', 'jan_pc_rx_non_market_201612.txt.gz', 'jan_pc_rx_non_market_201701.txt.gz', 'jan_pc_rx_non_market_201702.txt.gz', 'jan_pc_rx_non_market_201703.txt.gz', 'jan_pc_rx_non_market_201704.txt.gz', 'jan_pc_rx_non_market_201705.txt.gz', 'jan_pc_rx_non_market_201706.txt.gz', 'jan_pc_rx_non_market_201707.txt.gz', 'jan_pc_rx_non_market_201708.txt.gz', 'jan_pc_rx_non_market_201709.txt.gz', 'jan_pc_rx_non_market_201710.txt.gz', 'jan_pc_rx_non_market_201711.txt.gz', 'jan_pc_rx_non_market_201712.txt.gz', 'jan_pc_rx_non_market_201801.txt.gz', 'jan_pc_rx_non_market_201802.txt.gz', 'jan_pc_rx_non_market_201803.txt.gz', 'jan_pc_rx_non_market_201804.txt.gz', 'jan_pc_rx_non_market_201805.txt.gz', 'jan_pc_rx_non_market_201806.txt.gz', 'jan_pc_rx_non_market_201807.txt.gz', 'jan_pc_rx_non_market_201808.txt.gz', 'jan_pc_rx_non_market_201809.txt.gz', 'jan_pc_rx_non_market_201810.txt.gz', 'jan_pc_rx_non_market_201811.txt.gz', 'jan_pc_rx_non_market_201812.txt.gz', 'jan_pc_rx_non_market_201901.txt.gz', 'jan_pc_rx_non_market_201902.txt.gz', 'jan_pc_rx_non_market_201903.txt.gz', 'jan_pc_rx_non_market_201904.txt.gz', 'jan_pc_rx_non_market_201905.txt.gz', 'jan_pc_rx_non_market_201906.txt.gz', 'jan_pc_rx_non_market_201907.txt.gz', 'jan_pc_rx_non_market_201908.txt.gz', 'jan_pc_rx_non_market_201909.txt.gz', 'jan_pc_rx_non_market_201910.txt.gz', 'jan_pc_rx_non_market_201911.txt.gz', 'jan_pc_rx_non_market_201912.txt.gz', 'jan_pc_rx_non_market_202001.txt.gz', 'jan_pc_rx_non_market_202002.txt.gz', 'jan_pc_rx_non_market_202003.txt.gz', 'jan_pc_rx_non_market_202004.txt.gz', 'jan_pc_rx_non_market_202005.txt.gz', 'jan_pc_rx_non_market_202006.txt.gz', 'jan_pc_rx_non_market_202007.txt.gz', 'jan_pc_rx_non_market_202008.txt.gz', 'jan_pc_rx_non_market_202009.txt.gz', 'jan_pc_rx_non_market_202010.txt.gz', 'jan_pc_rx_non_market_202011.txt.gz', 'jan_pc_rx_non_market_202012.txt.gz', 'jan_pc_rx_non_market_202101.txt.gz', 'jan_pc_rx_non_market_202102.txt.gz', 'jan_pc_rx_non_market_202103.txt.gz', 'jan_pc_rx_non_market_202104.txt.gz', 'jan_pc_rx_non_market_202105.txt.gz', 'jan_pc_rx_non_market_202106.txt.gz', 'jan_pc_rx_non_market_202107.txt.gz', 'jan_pc_rx_non_market_202108.txt.gz', 'jan_pc_rx_non_market_202109.txt.gz', 'jan_pc_rx_non_market_202110.txt.gz', 'jan_pc_rx_non_market_202111.txt.gz', 'jan_pc_rx_non_market_202112.txt.gz', 'jan_pc_rx_non_market_202201.txt.gz', 'jan_pc_rx_non_market_202202.txt.gz', 'jan_pc_rx_non_market_202203.txt.gz', 'jan_pc_rx_non_market_202204.txt.gz', 'jan_pc_rx_non_market_202205.txt.gz', 'jan_pc_rx_non_market_202206.txt.gz', 'jan_pc_rx_non_market_202207.txt.gz', 'jan_pc_rx_non_market_202208.txt.gz', 'jan_pc_rx_non_market_202209.txt.gz', 'jan_pc_rx_non_market_202210.txt.gz', 'jan_pc_rx_non_market_202211.txt.gz', 'jan_pc_rx_non_market_202212.txt.gz', 'jan_pc_rx_non_market_202301.txt.gz', 'jan_pc_surgical_codes.txt.gz', 'jan_pc_sx_201001.txt.gz', 'jan_pc_sx_201002.txt.gz', 'jan_pc_sx_201003.txt.gz', 'jan_pc_sx_201004.txt.gz', 'jan_pc_sx_201005.txt.gz', 'jan_pc_sx_201006.txt.gz', 'jan_pc_sx_201007.txt.gz', 'jan_pc_sx_201008.txt.gz', 'jan_pc_sx_201009.txt.gz', 'jan_pc_sx_201010.txt.gz', 'jan_pc_sx_201011.txt.gz', 'jan_pc_sx_201012.txt.gz', 'jan_pc_sx_201101.txt.gz', 'jan_pc_sx_201102.txt.gz', 'jan_pc_sx_201103.txt.gz', 'jan_pc_sx_201104.txt.gz', 'jan_pc_sx_201105.txt.gz', 'jan_pc_sx_201106.txt.gz', 'jan_pc_sx_201107.txt.gz', 'jan_pc_sx_201108.txt.gz', 'jan_pc_sx_201109.txt.gz', 'jan_pc_sx_201110.txt.gz', 'jan_pc_sx_201111.txt.gz', 'jan_pc_sx_201112.txt.gz', 'jan_pc_sx_201201.txt.gz', 'jan_pc_sx_201202.txt.gz', 'jan_pc_sx_201203.txt.gz', 'jan_pc_sx_201204.txt.gz', 'jan_pc_sx_201205.txt.gz', 'jan_pc_sx_201206.txt.gz', 'jan_pc_sx_201207.txt.gz', 'jan_pc_sx_201208.txt.gz', 'jan_pc_sx_201209.txt.gz', 'jan_pc_sx_201210.txt.gz', 'jan_pc_sx_201211.txt.gz', 'jan_pc_sx_201212.txt.gz', 'jan_pc_sx_201301.txt.gz', 'jan_pc_sx_201302.txt.gz', 'jan_pc_sx_201303.txt.gz', 'jan_pc_sx_201304.txt.gz', 'jan_pc_sx_201305.txt.gz', 'jan_pc_sx_201306.txt.gz', 'jan_pc_sx_201307.txt.gz', 'jan_pc_sx_201308.txt.gz', 'jan_pc_sx_201309.txt.gz', 'jan_pc_sx_201310.txt.gz', 'jan_pc_sx_201311.txt.gz', 'jan_pc_sx_201312.txt.gz', 'jan_pc_sx_201401.txt.gz', 'jan_pc_sx_201402.txt.gz', 'jan_pc_sx_201403.txt.gz', 'jan_pc_sx_201404.txt.gz', 'jan_pc_sx_201405.txt.gz', 'jan_pc_sx_201406.txt.gz', 'jan_pc_sx_201407.txt.gz', 'jan_pc_sx_201408.txt.gz', 'jan_pc_sx_201409.txt.gz', 'jan_pc_sx_201410.txt.gz', 'jan_pc_sx_201411.txt.gz', 'jan_pc_sx_201412.txt.gz', 'jan_pc_sx_201501.txt.gz', 'jan_pc_sx_201502.txt.gz', 'jan_pc_sx_201503.txt.gz', 'jan_pc_sx_201504.txt.gz', 'jan_pc_sx_201505.txt.gz', 'jan_pc_sx_201506.txt.gz', 'jan_pc_sx_201507.txt.gz', 'jan_pc_sx_201508.txt.gz', 'jan_pc_sx_201509.txt.gz', 'jan_pc_sx_201510.txt.gz', 'jan_pc_sx_201511.txt.gz', 'jan_pc_sx_201512.txt.gz', 'jan_pc_sx_201601.txt.gz', 'jan_pc_sx_201602.txt.gz', 'jan_pc_sx_201603.txt.gz', 'jan_pc_sx_201604.txt.gz', 'jan_pc_sx_201605.txt.gz', 'jan_pc_sx_201606.txt.gz', 'jan_pc_sx_201607.txt.gz', 'jan_pc_sx_201608.txt.gz', 'jan_pc_sx_201609.txt.gz', 'jan_pc_sx_201610.txt.gz', 'jan_pc_sx_201611.txt.gz', 'jan_pc_sx_201612.txt.gz', 'jan_pc_sx_201701.txt.gz', 'jan_pc_sx_201702.txt.gz', 'jan_pc_sx_201703.txt.gz', 'jan_pc_sx_201704.txt.gz', 'jan_pc_sx_201705.txt.gz', 'jan_pc_sx_201706.txt.gz', 'jan_pc_sx_201707.txt.gz', 'jan_pc_sx_201708.txt.gz', 'jan_pc_sx_201709.txt.gz', 'jan_pc_sx_201710.txt.gz', 'jan_pc_sx_201711.txt.gz', 'jan_pc_sx_201712.txt.gz', 'jan_pc_sx_201801.txt.gz', 'jan_pc_sx_201802.txt.gz', 'jan_pc_sx_201803.txt.gz', 'jan_pc_sx_201804.txt.gz', 'jan_pc_sx_201805.txt.gz', 'jan_pc_sx_201806.txt.gz', 'jan_pc_sx_201807.txt.gz', 'jan_pc_sx_201808.txt.gz', 'jan_pc_sx_201809.txt.gz', 'jan_pc_sx_201810.txt.gz', 'jan_pc_sx_201811.txt.gz', 'jan_pc_sx_201812.txt.gz', 'jan_pc_sx_201901.txt.gz', 'jan_pc_sx_201902.txt.gz', 'jan_pc_sx_201903.txt.gz', 'jan_pc_sx_201904.txt.gz', 'jan_pc_sx_201905.txt.gz', 'jan_pc_sx_201906.txt.gz', 'jan_pc_sx_201907.txt.gz', 'jan_pc_sx_201908.txt.gz', 'jan_pc_sx_201909.txt.gz', 'jan_pc_sx_201910.txt.gz', 'jan_pc_sx_201911.txt.gz', 'jan_pc_sx_201912.txt.gz', 'jan_pc_sx_202001.txt.gz', 'jan_pc_sx_202002.txt.gz', 'jan_pc_sx_202003.txt.gz', 'jan_pc_sx_202004.txt.gz', 'jan_pc_sx_202005.txt.gz', 'jan_pc_sx_202006.txt.gz', 'jan_pc_sx_202007.txt.gz', 'jan_pc_sx_202008.txt.gz', 'jan_pc_sx_202009.txt.gz', 'jan_pc_sx_202010.txt.gz', 'jan_pc_sx_202011.txt.gz', 'jan_pc_sx_202012.txt.gz', 'jan_pc_sx_202101.txt.gz', 'jan_pc_sx_202102.txt.gz', 'jan_pc_sx_202103.txt.gz', 'jan_pc_sx_202104.txt.gz', 'jan_pc_sx_202105.txt.gz', 'jan_pc_sx_202106.txt.gz', 'jan_pc_sx_202107.txt.gz', 'jan_pc_sx_202108.txt.gz', 'jan_pc_sx_202109.txt.gz', 'jan_pc_sx_202110.txt.gz', 'jan_pc_sx_202111.txt.gz', 'jan_pc_sx_202112.txt.gz', 'jan_pc_sx_202201.txt.gz', 'jan_pc_sx_202202.txt.gz', 'jan_pc_sx_202203.txt.gz', 'jan_pc_sx_202204.txt.gz', 'jan_pc_sx_202205.txt.gz', 'jan_pc_sx_202206.txt.gz', 'jan_pc_sx_202207.txt.gz', 'jan_pc_sx_202208.txt.gz', 'jan_pc_sx_202209.txt.gz', 'jan_pc_sx_202210.txt.gz', 'jan_pc_sx_202211.txt.gz', 'jan_pc_sx_202212.txt.gz', 'jan_pc_sx_202301.txt.gz']

    print(f"List of files to be copied :{file_list}")
    print(f"No. of files to be copied :{len(file_list)}")
    if len(file_list) > 0:
        for file in file_list:
            ftp_file_path = "/" + f'{folder_name}/' + file
            s3_file_path = add_slash('inbound-feed/claims/' + f'{folder_name}') + file
            try:
                print(f"ftp_file_path :: {ftp_file_path}")
                print(f"s3_file_path :: {s3_file_path}")
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
