import base64
import math
import io
import paramiko
import time
from Crypto.Cipher import DES3
from Crypto.Util.Padding import unpad
from s3_utils import *
from datetime import datetime

# Decryption Function
def decrypt(ciphertext, key):
    cipher = DES3.new(key, DES3.MODE_ECB)
    plaintext = cipher.decrypt(ciphertext)
    return plaintext.decode()


def decrypt_and_place(bucket, file_nm, key, decryption_flag, tgt_name):
    tgt_path = file_nm.replace(tgt_name + '_raw', tgt_name + '_cur')
    archive_path = file_nm.replace(tgt_name + '_raw', add_slash(tgt_name) + 'raw').replace('inbound-feed', 'archive')
    if decryption_flag == 1:
        print(f"file {file_nm} going for decryption")
        data = get_s3_object(bucket, file_nm)
        lines = data.split()
        data_content = ''
        for idx, line in enumerate(lines):
            line = line.decode('UTF8')
            if idx == 0:
                data_content = line
            else:
                encrypted_data = line.split("|")[0].encode('UTF8')
                base64_decoded_data = base64.b64decode(encrypted_data)
                decrypt_data = unpad(decrypt(base64_decoded_data, key).encode('UTF8'), DES3.block_size, 'pkcs7').decode(
                    'UTF8')
                data_content = data_content + '\n' + decrypt_data + "|" + line.split("|")[1]
        put_s3_object(bucket, tgt_path, data_content)
        multipart_copy_s3_object(bucket, file_nm, archive_path)
        delete_s3_object(bucket, file_nm)
        print(f"Successfully decrypted the synomacrosswalk file and kept at {tgt_path}")
    else:
        print(f"As files are flagged not to be decrypted, they are simply moved to {tgt_path} without decryption")
        multipart_copy_s3_object(bucket, file_nm, tgt_path)
        delete_s3_object(bucket, file_nm)


def get_credentials_from_sm(secret_name, region_name):
    if secret_name is not None and region_name is not None:
        secrets_client = boto3.client('secretsmanager', region_name=region_name)
        credentials = secrets_client.get_secret_value(SecretId=secret_name)
        return credentials
    else:
        raise Exception(f" Error : {current_datetime()} invalid secret name {secret_name} or region {region_name}")


# function to get current date and time
def current_datetime():
    time.sleep(0.01)
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def open_ftp_connection(ftp_host, ftp_port, ftp_username, ftp_password):
    """
    Opens ftp connection and returns connection object

    """
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    try:
        transport = paramiko.Transport(ftp_host, ftp_port)
    except Exception as e:
        return "conn_error"
    try:
        transport.connect(username=ftp_username, password=ftp_password)
    except Exception as identifier:
        return "auth_error"
    ftp_connection = paramiko.SFTPClient.from_transport(transport)
    return ftp_connection


def transfer_chunk_from_ftp_to_s3(
        ftp_file,
        s3_connection,
        multipart_upload,
        bucket_name,
        ftp_file_path,
        s3_file_path,
        part_number,
        chunk_size,
):
    start_time = time.time()
    chunk = ftp_file.read(int(chunk_size))
    part = s3_connection.upload_part(
        Bucket=bucket_name,
        Key=s3_file_path,
        PartNumber=part_number,
        UploadId=multipart_upload["UploadId"],
        Body=chunk
    )
    end_time = time.time()
    total_seconds = end_time - start_time
    print(
        "speed is {} kb/s total seconds taken {}".format(
            math.ceil((int(chunk_size) / 1024) / total_seconds), total_seconds
        )
    )
    part_output = {"PartNumber": part_number, "ETag": part["ETag"]}
    return part_output


def transfer_file_from_ftp_to_s3(
        bucket_name, ftp_file_path, s3_file_path, ftp_username, ftp_password, chunk_size, SFTP_HOST, SFTP_PORT
):
    ftp_connection = open_ftp_connection(
        SFTP_HOST, int(SFTP_PORT), ftp_username, ftp_password
    )
    ftp_file = ftp_connection.file(ftp_file_path, "r")
    s3_connection = boto3.client("s3")
    ftp_file_size = ftp_file._get_size()
    try:
        s3_file = s3_connection.head_object(Bucket=bucket_name, Key=s3_file_path)
        if s3_file["ContentLength"] == ftp_file_size:
            print("File Already Exists in S3 bucket")
            ftp_file.close()
            return
    except Exception as e:
        pass
    if ftp_file_size <= int(chunk_size):
        # upload file in one go
        print("Transferring complete File from FTP to S3...")
        ftp_file_data = ftp_file.read()
        ftp_file_data_bytes = io.BytesIO(ftp_file_data)
        s3_connection.upload_fileobj(ftp_file_data_bytes, bucket_name, s3_file_path)
        print("Successfully Transferred file from FTP to S3!")
        ftp_file.close()

    else:
        print("Transferring File from FTP to S3 in chunks...")
        # upload file in chunks
        chunk_count = int(math.ceil(ftp_file_size / float(chunk_size)))
        multipart_upload = s3_connection.create_multipart_upload(
            Bucket=bucket_name, Key=s3_file_path
        )
        parts = []
        for i in range(chunk_count):
            print("Transferring chunk {}...".format(i + 1))
            part = transfer_chunk_from_ftp_to_s3(
                ftp_file,
                s3_connection,
                multipart_upload,
                bucket_name,
                ftp_file_path,
                s3_file_path,
                i + 1,
                chunk_size,
            )
            parts.append(part)
            print("Chunk {} Transferred Successfully!".format(i + 1))

        part_info = {"Parts": parts}
        s3_connection.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_file_path,
            UploadId=multipart_upload["UploadId"],
            MultipartUpload=part_info,
        )
        print("All chunks Transferred to S3 bucket! File Transfer successful!")
        ftp_file.close()

