import boto3
import random
import string

"""
This module has functions for 
commonly used s3 operations
"""


def create_s3_resource(arn=None):
    if arn is None:
        s3_resource = boto3.resource('s3')
    else:
        sts_client = boto3.client('sts')
        token = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        # Call the assume_role method of the STSConnection object and pass the role
        # ARN and a role session name.
        assumed_role_object = sts_client.assume_role(
            RoleArn=arn,
            RoleSessionName=f"AssumeRoleSession_{token}"
        )
        # From the response that contains the assumed role, get the temporary
        # credentials that can be used to make subsequent API calls
        credentials = assumed_role_object['Credentials']
        # Use the temporary credentials that AssumeRole returns to make a
        # connection to Amazon S3
        s3_resource = boto3.resource(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
        )
    return s3_resource


def create_s3_client(arn=None):
    if arn is None:
        s3_client = boto3.client('s3')
    else:
        sts_client = boto3.client('sts')
        token = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        # Call the assume_role method of the STSConnection object and pass the role
        # ARN and a role session name.
        assumed_role_object = sts_client.assume_role(
            RoleArn=arn,
            RoleSessionName=f"AssumeRoleSession_{token}"
        )
        # From the response that contains the assumed role, get the temporary
        # credentials that can be used to make subsequent API calls
        credentials = assumed_role_object['Credentials']
        # Use the temporary credentials that AssumeRole returns to make a
        # connection to Amazon S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
        )
    return s3_client


def get_s3_object(bucket_name, object_name, arn=None):
    """
    Reads data from a file in S3 bucket.
    """
    try:
        s3_resource = create_s3_resource(arn)
        data_object = s3_resource.Object(bucket_name, object_name)
        data = data_object.get()['Body'].read()
    except Exception as err:
        print(err)
        raise Exception(f"could not read {object_name}")
    else:
        return data


def put_s3_object(bucket_name, object_name, data, arn=None):
    """
    Writes data into a file in S3 bucket.
    """
    try:
        s3_resource = create_s3_resource(arn)
        bucket_object = s3_resource.Bucket(bucket_name)
        response = bucket_object.put_object(
            Body=data,
            Key=object_name
        )
    except Exception as err:
        print(err)
        raise err
    else:
        return response


def copy_s3_object(bucket_name, source_path, target_path, arn=None):
    """
    Copies a file in S3 bucket from one location to another.
    """
    try:
        s3_resource = create_s3_resource(arn)
        copy_source = {"Bucket": bucket_name, "Key": source_path}
        s3_resource.Object(bucket_name, target_path).copy_from(CopySource=copy_source)
    except Exception as err:
        print(err)
        raise err


def multipart_copy_s3_object(bucket_name, source_path, target_path, arn=None):
    """
    Performs mutipart file copy >5GB also in S3 bucket from one location to another.
    """
    try:
        s3_resource = create_s3_resource(arn)
        copy_source = {"Bucket": bucket_name, "Key": source_path}
        s3_resource.meta.client.copy(copy_source, bucket_name, target_path)

    except Exception as err:
        print(err)
        raise err


def copy_s3_folder(bucket_name, source_path, target_path, arn=None):
    """
    Copies a folder content in S3 bucket from one location to another.
    """
    source_list = list_s3_objects(bucket_name, add_slash(source_path))
    if source_list != [] and source_list is not None:
        try:
            s3_resource = create_s3_resource(arn)
            for file in source_list:
                srckey = file
                tgtkey = file.replace(add_slash(source_path), add_slash(target_path))
                copy_source = {"Bucket": bucket_name, "Key": srckey}
                s3_resource.Object(bucket_name, tgtkey).copy_from(CopySource=copy_source)

        except Exception as err:
            print(err)
            raise err
    return source_list


def copy_multipart_s3_folder(bucket_name, source_path, target_path, arn=None):
    """
    Copies a folder content in S3 bucket from one location to another.
    """
    source_list = list_s3_objects(bucket_name, add_slash(source_path))
    if source_list != [] and source_list is not None:
        try:
            for file in source_list:
                srckey = file
                tgtkey = file.replace(add_slash(source_path), add_slash(target_path))
                multipart_copy_s3_object(bucket_name, srckey, tgtkey, arn)

        except Exception as err:
            print(err)
            raise err
    return source_list


def move_s3_folder(bucket_name, source_path, target_path):
    """
    Moves a folder contentin S3 bucket from one location to another.
    """
    obj_list = list_s3_objects(bucket_name, add_slash(source_path))
    if obj_list is None or obj_list == []:
        print("nothing to move")
    else:
        copy_s3_folder(bucket_name, source_path, target_path)
        delete_s3_folder(bucket_name, source_path)

    return obj_list


def move_multipart_s3_folder(bucket_name, source_path, target_path):
    """
    Moves a folder contentin S3 bucket from one location to another.
    """
    obj_list = list_s3_objects(bucket_name, add_slash(source_path))
    if obj_list is None or obj_list == []:
        print("nothing to move")
    else:
        copy_multipart_s3_folder(bucket_name, source_path, target_path)
        delete_s3_folder(bucket_name, source_path)

    return obj_list


def delete_s3_object(bucket_name, object_name, arn=None):
    """
    Deletes object in S3 bucket.
    """
    try:
        s3_resource = create_s3_resource(arn)
        response = s3_resource.Object(bucket_name, object_name).delete()
    except Exception as err:
        print(err)
        raise err
    else:
        return response


def delete_s3_folder(bucket_name, file_path, arn=None):
    """
    Deletes a folder and underlying objects in S3 bucket along with the version.
    """
    while s3_path_exists(bucket_key_to_s3_path(bucket_name, add_slash(file_path))):
        s3_resource = create_s3_resource(arn)
        bucket = s3_resource.Bucket(bucket_name)
        bucket.object_versions.filter(Prefix=add_slash(file_path)).delete()
        print(f"deletion completed for path {add_slash(file_path)}")
    else:
        print(f"Deletion path {add_slash(file_path)} doesn't exist")


def list_s3_files_and_folders_old(bucket_name, file_path, arn=None):
    try:
        s3_client = create_s3_client(arn)
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=file_path)
        file_list = get_nested_sub_folders(bucket_name, file_path)
        for item in response["Contents"]:
            file = item["Key"]
            file_list.append(file)
    except KeyError:
        print(f"{file_path} not found on {bucket_name}")
        return []
    except Exception as err:
        print(err)
        raise err
    else:
        return list(set(file_list))


def list_s3_files_and_folders(bucket_name, file_path, arn=None):
    """
    Lists all the underlying objects of a folder in S3 bucket.
    """
    try:
        s3_client = create_s3_client(arn)
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=file_path)
        li = get_nested_sub_folders(bucket_name, file_path)
        for page in pages:
            for obj in page['Contents']:
                # print(obj['Key'])
                li.append(obj['Key'])
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=file_path)
    except KeyError:
        print(f"{file_path} not found on {bucket_name}")
        return []
    except Exception as err:
        print(err)
        raise err
    else:
        return list(set(li))


def list_s3_objects(bucket_name, file_path, arn=None):
    """
    Lists all the underlying objects of a folder in S3 bucket.
    """
    try:
        s3_client = create_s3_client(arn)
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=file_path)
        file_list = []
        for item in response["Contents"]:
            file = item["Key"]
            if file[-1] != "/":
                file_list.append(file)
    except KeyError:
        print(f"{file_path} not found on {bucket_name}")
        return []
    except Exception as err:
        print(err)
        raise err
    else:
        return file_list


def list_s3_subfolders(bucket_name, file_path, arn=None):
    """
    Lists all the underlying folders of a folder in S3 bucket.
    """
    try:
        s3_client = create_s3_client(arn)
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=file_path, Delimiter="/")
        # print("list_s3_folders : response : ", response)
        folder_list = []
        if "CommonPrefixes" not in response:
            return []
        for item in response["CommonPrefixes"]:
            folder = item["Prefix"]
            folder_list.append(folder)
    except KeyError:
        print(f"{file_path} not found on {bucket_name}")
        return []
    except Exception as err:
        print(err)
        raise err
    else:
        return folder_list


def list_s3_subfolders_wo_path(bucket_name, file_path):
    """
    Lists all the underlying folders of a folder in S3 bucket by trimming source path.
    """
    folder_list = list_s3_subfolders(bucket_name, file_path)
    folder_list = [remove_slash(folder).split("/")[-1] for folder in folder_list]
    return folder_list


def bucket_key_to_s3_path(bucket, key, s3_prefix="s3"):
    """
    Takes an S3 bucket and key combination and returns the full S3 path to that location.
    """
    s3_prefix = s3_prefix.split(":")[0]
    return "{}://{}/{}".format(s3_prefix, bucket, key)


def bucket_keylist_to_s3_pathlist(bucket, keylist, s3_prefix="s3"):
    """
    Takes an S3 bucket and key combination and returns the full S3 path to that location.
    """
    pathlist = []
    for key in keylist:
        s3_prefix = s3_prefix.split(":")[0]
        pathlist.append("{}://{}/{}".format(s3_prefix, bucket, key))
    return pathlist


def s3_path_to_bucket_key(s3_path):
    """
    Splits out s3 file path to bucket key combination
    """
    s3_path = s3_path.replace("s3://", "").replace("s3a://", "")
    bucket, key = s3_path.split("/", 1)
    return bucket, key


def add_slash(path):
    return path if path[-1] == "/" else path + "/"


def remove_slash(path):
    return path[0:-1] if path[-1] == "/" else path


def s3_path_exists(path, arn=None):
    s3_client = create_s3_client(arn)
    bucket, key = s3_path_to_bucket_key(path)
    result = s3_client.list_objects(Bucket=bucket, Prefix=key)
    exists = False
    if "Contents" in result:
        exists = True
    return exists


def get_size(full_path, arn=None):
    bucket, path = s3_path_to_bucket_key(full_path)
    s3_resource = create_s3_resource(arn)
    my_bucket = s3_resource.Bucket(bucket)
    total_size = 0

    for obj in my_bucket.objects.filter(Prefix=path):
        # print(f"{obj.key} =============> {obj.size}")
        total_size = total_size + obj.size
        total_size_mb = total_size / 1024 / 1024

        if total_size > 1024 * 1024 * 1024:
            siz_str = str(round(total_size / (1024 * 1024 * 1024), 1)) + " GB"
        elif total_size > 1024 * 1024:
            siz_str = str(round(total_size / (1024 * 1024), 1)) + " MB"
        elif total_size > 1024:
            siz_str = str(round(total_size / 1024, 1)) + " KB"
        else:
            siz_str = str(total_size) + " Bytes"

    return siz_str


def get_nested_sub_folders(bucket, base_path, sub_folder_li=[]):
    tmp_li = list_s3_subfolders(bucket, base_path)
    sub_folder_li += tmp_li
    for item in tmp_li:
        get_nested_sub_folders(bucket, item, sub_folder_li)
    return sub_folder_li



def multipart_copy_s3_object_env1_to_env2(src_bucket, tgt_bucket, source_path, target_path, arn=None):
    """
    Performs mutipart file copy >5GB also in S3 bucket from one location to another.
    """
    try:
        s3_resource = create_s3_resource(arn)
        copy_source = {"Bucket": src_bucket, "Key": source_path}
        s3_resource.meta.client.copy(copy_source, tgt_bucket, target_path)

    except Exception as err:
        print(err)
        raise err
