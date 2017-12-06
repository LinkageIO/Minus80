from .Tools import get_files
from .Config import cf
from collections import defaultdict
import os
import lzma

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
   

    def put(self,name,dtype,raw=False,lzma=False):
        key = os.path.basename(name)
        if raw == True:
            # The name is a FILENAME
            filename = name
            if lzma:
                with open(filename,'rb') as OUT:
                    self.s3.upload_fileobj(lzma.compress(OUT.read()),self.bucket,f'Raw/{dtype}/{key}.xz')
            else:
                self.s3.upload_file(filename,self.bucket,f'Raw/{dtype}/{key}.xz')
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

    def list(self,name=None,dtype=None,raw=False):
        items = defaultdict(list)
        for item in self.s3.list_objects(Bucket=self.bucket)['Contents']:
            key = item['Key']
            if key.startswith('Raw') and raw == False:
                pass
            elif not key.startswith('Raw') and raw == True:
                pass
            else:
                _,key_dtype,key_name = key.split('/')
                if dtype != None and key_dtype != dtype:
                    pass
                elif name != None and not key_name.startswith(name):
                    pass
                else:
                    items[key_dtype].append(key_name)
        if len(items) == 0:
            print('Nothing here yet!')
        else:
            for key,vals in items.items():
                print(f'-----{key}------')
                print('\n'.join(vals))
