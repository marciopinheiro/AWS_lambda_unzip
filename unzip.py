import os
import zipfile
import boto3
import io
import tempfile


def handler(event, context):
    s3 = boto3.client('s3')
    # bucket_name = str(event['Records'][0]['s3']['bucket']['name']).encode('utf8')
    # key = str(event['Records'][0]['s3']['object']['key']).encode('utf8')

    bucket_name = 'sls-mdtr-bucket'
    key = 'teste/MDTR_201805.zip'

    try:
        #obj = s3.get_object(Bucket=bucket_name, Key=key)
        obj_body = open('data/MDTR_201805.zip').read()
        temp_filename = os.path.join(tempfile.gettempdir(), key)

        with open(temp_filename, 'wb') as l_file:
            l_file.write(obj_body.read())

        del obj_body

        put_objects = []

        with open(temp_filename).read() as tf:
            # rewind the file
            tf.seek(0)

            # Read the file as a zipfile and process the members
            with zipfile.ZipFile(tf, mode='r') as zipf:

                for file in zipf.infolist():
                    file_name = file.filename
                    print(file_name)
                    put_file = s3.put_object(Bucket=bucket_name, Key='python/' + file_name,
                                             Body=zipf.read(file))
                    print(put_file)
                    put_objects.append(put_file)
                    print(put_objects)

        # Delete zip file after unzip
        if len(put_objects) > 0:
            # deleted_obj = s3.delete_object(Bucket=bucket_name, Key=key)
            print('deleted file:')
            # print(deleted_obj)

    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket_name}. '
              f'Make sure they exist and your bucket is in the same region as this function.')
        raise e
