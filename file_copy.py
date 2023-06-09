from datetime import datetime
import sys
import time

from awsglue.utils import getResolvedOptions
from s3_utils import multipart_copy_s3_object_env1_to_env2, delete_s3_object


def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# read the glue code arguments
print(f"{current_datetime()} :: main :: info - read the glue code parameters...\n")
try:
    args = getResolvedOptions(sys.argv, ['SRC_BUCKET', 'TGT_BUCKET','SRC_PATH','TGT_PATH'])
    src_bucket = args['SRC_BUCKET']
    tgt_bucket = args['TGT_BUCKET']
    src_path = args['SRC_PATH']
    tgt_path = args['TGT_PATH']
except Exception as err:
    print(f"{current_datetime()} :: main :: error - failed to read the glue code parameters\n")
    print("error details : ", err)
    raise err
else:
    print(f"{current_datetime()} :: main :: info - successfully read the glue code parameters\n")
    print(f"{current_datetime()} :: main :: info - src_bucket           : {src_bucket}")
    print(f"{current_datetime()} :: main :: info - tgt_bucket           : {tgt_bucket}")
    print(f"{current_datetime()} :: main :: info - src_path             : {src_path}")
    print(f"{current_datetime()} :: main :: info - tgt_path             : {tgt_path}")

print(f"Started moving file from s3://{src_bucket}/{src_path} to s3://{tgt_bucket}/{tgt_path}")
multipart_copy_s3_object_env1_to_env2(src_bucket, tgt_bucket, src_path, tgt_path, arn=None)
# delete_s3_object(src_bucket, src_path, arn=None)
print(f"Completed moving file from s3://{src_bucket}/{src_path} to s3://{tgt_bucket}/{tgt_path}")
