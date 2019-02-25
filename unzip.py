import os
import zipfile
import boto3
import io


def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    dest_folder = 'python/'

    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=key)

        with io.BytesIO(response["Body"].read()) as s3c:
            with zipfile.ZipFile(s3c, mode='r') as rzipf:
                for infile_info in rzipf.infolist():
                    with rzipf.open(infile_info, 'r') as infile_obj:
                        upload_to_s3(client=s3_client, obj=infile_obj,
                                     info=infile_info, bucket=bucket_name,
                                     folder=dest_folder)

        del response, s3c, rzipf

        s3_client.delete_object(Bucket=bucket_name, Key=key)

    except Exception as e:
        print(f'Error trying unzip object {key} from bucket {bucket_name}.')
        print(e)
        raise e


def upload_to_s3(client, obj, info, bucket, folder):
    upload_chunk_size = 1024 * 1000 * 250  # 250MB
    upload_parts = []
    key = os.path.join(folder, info.filename)

    if info.file_size <= upload_chunk_size:
        print(f'Uploading {info.filename}')
        client.upload_fileobj(Fileobj=obj, Bucket=bucket, Key=key)
    else:
        upload = client.create_multipart_upload(Bucket=bucket, Key=key)

        for i, obj_part in enumerate(read_in_chunks(obj, upload_chunk_size)):
            infile_part_num = i + 1

            print(f'Uploading {info.filename}: part {infile_part_num}')

            part = client.upload_part(Bucket=bucket, Key=key, Body=obj_part,
                                      PartNumber=infile_part_num,
                                      UploadId=upload['UploadId']
                                      )
            upload_parts.append(part)

        part_info = {
            'Parts': [
                {
                    'PartNumber': (i + 1),
                    'ETag': part['ETag']
                } for i, part in enumerate(upload_parts)
            ]
        }

        client.complete_multipart_upload(Bucket=bucket, Key=key,
                                         UploadId=upload['UploadId'],
                                         MultipartUpload=part_info)


def read_in_chunks(file_object, chunk_size):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data
