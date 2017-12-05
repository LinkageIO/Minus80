from .Tools import get_files
from .Config import cf
import os

class CloudData(object):

    bucket = 'minus80'

    def __init__(self):
        try:
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
        key = os.path.basename(name)
        if raw == True:
            # The name is a FILENAME
            filename = name
            self.s3.upload_file(filename,self.bucket,f'Raw/{dtype}/{key}')
        else:
            files = get_files(dtype,name,fullpath=True)
            for filename in files:
                self.s3.upload_file(filename,self.bucket,f'databases/{dtype}/{key}')


    def get(self,name,dtype,raw=False):
        key = os.path.basename(name)
        if raw == True:
            filename = name
            bdir = os.path.expanduser(cf.options.basedir)
            self.s3.download_file(self.bucket,f'Raw/{dtype}/{key}',f'{bdir}/Raw/{key}')
