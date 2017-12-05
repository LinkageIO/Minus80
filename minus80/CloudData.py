from .Tools import get_files
import os

class CloudData(object):

    bucket = 'minus80'

    def __init__(self):
        try:
            from .Config import cf
            import boto3
            from botocore.client import Config
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError('boto3 must be installed to use this feature')
   
        if cf.cloud.access_key == 'None' or cf.cloud.secret_key == 'None':
            raise ValueError('Fill in your Amazon Credentials in ~/.minus80.conf')

        self.s3 = boto3.client(
            service_name='s3',
            endpoint_url= cf.cloud.endpoint,
            aws_access_key_id= cf.cloud.access_key,
            aws_secret_access_key= cf.cloud.secret_key,
            config=Config(s3={'addressing_style': 'path'})
        )

        # make sure the minus80 bucket exists
        if self.bucket not in [x['Name'] for x in self.s3.list_buckets()['Buckets']]:
            self.s3.create_bucket(Bucket='minus80')
   

    def put(self,name,dtype,raw=False):
        if raw == True:
            # The name is a FILENAME
            filename = name
            self.s3.upload_file(filename,self.bucket,f'Raw/{dtype}/{filename}')
        else:
            files = get_files(dtype,name,fullpath=True)
            for filename in files:
                key = os.path.basename(filename)
                self.s3.upload_file(filename,self.bucket,f'databases/{dtype}/{key}')


