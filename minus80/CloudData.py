from .Tools import get_files
from .Config import cf
from collections import defaultdict
import os
import lzma
import sys

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
        # Define a helper
        def get_percent_done(current,total):
            write,flush = sys.stdout.write, sys.stdout.flush
            percent = int((current/total)*100)
            write('\x08'*17)
            write(f'Percent done: {percent}%')
            flush()

        key = os.path.basename(name)
        if raw == True:
            filename = name
            bdir = os.path.expanduser(cf.options.basedir)
            # get the number of bytes in the object
            num_bytes = self.s3.list_objects(Bucket=self.bucket,Prefix=f'Raw/{dtype}/{key}')['Contents'][0]['Size']
            # download
            os.makedirs(f'{bdir}/Raw/{dtype}')
            with open(f'{bdir}/Raw/{dtype}/{key}','wb') as OUT:
                self.s3.download_fileobj(
                    self.bucket,
                    f'Raw/{dtype}/{key}',
                    OUT,
                    Callback = lambda x: get_percent_done(x,num_bytes)
                )

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
            if raw:
                print('######   Raw Data:   ######')
            for key,vals in items.items():
                print(f'-----{key}------')
                print('\n'.join(vals))
