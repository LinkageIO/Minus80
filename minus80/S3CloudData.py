import os
import lzma
import sys
import threading
import tarfile
import requests

from collections import defaultdict

from .Tools import get_files
from .Config import cf
from .CloudData import BaseCloudData

class ProgressPercentage(object):
    '''
    Borrowed from: https://boto3.readthedocs.io/en/latest/_modules/boto3/s3/transfer.html
    '''
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

class ProgressDownloadPercentage(object):
    def __init__(self,filename,total_bytes):
        self._filename = filename
        self._total_bytes = total_bytes
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._total_bytes) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._total_bytes,
                    percentage)
            )
            sys.stdout.flush()


class S3CloudData(BaseCloudData):

    '''
    CloudData objects allow minus80 to interact with the cloud to store both
    prepared as well as raw datasets.
    '''

    def __init__(self):
        '''
        Create a CloudData object. Once proper S3 credentials are stored in the
        config file (~/.minus80.conf) initialization takes no arguments.
        '''
        import boto3
        from botocore.client import Config

        # Fetch creds from config file
        aws_endpoint   = cf.cloud.endpoint
        aws_bucket     = cf.cloud.bucket
        aws_access_key = cf.cloud.access_key
        aws_secret_key = cf.cloud.secret_key

        # override config with ENV variables 
        if 'CLOUD_ENDPOINT' in os.environ:
            aws_endpoint = os.environ['CLOUD_ENDPOINT'] 
        if 'CLOUD_BUCKET' in os.environ:
            aws_bucket = os.environ['CLOUD_BUCKET'] 
        if 'CLOUD_ACCESS_KEY' in os.environ:
            aws_access_key = os.environ['CLOUD_ACCESS_KEY']
        if 'CLOUD_SECRET_KEY' in os.environ:
            aws_secret_key = os.environ['CLOUD_SECRET_KEY']

        try:
            self.s3 = boto3.client(
                service_name='s3',
                endpoint_url=aws_endpoint,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                config=Config(s3={'addressing_style': 'path'})
            )
            #self.bucket = f'minus80-{aws_access_key.lower()}'
            self.bucket = aws_bucket
    
            # make sure the minus80 bucket exists
            #if self.bucket not in [x['Name'] for x in self.s3.list_buckets()['Buckets']]:
                # Append access key to bucket name so multiple users can use host
            try:
                self.s3.create_bucket(Bucket=self.bucket)
            except self.s3.exceptions.BucketAlreadyOwnedByYou as e:
                pass
        except Exception as e:
            raise ValueError(
                'Accessing the cloud requires either setting up AWS credentials in ~/.minus80.conf '
                'contact help@linkage.io for assistance',e
            ) 


    def push(self, dtype, name, raw=False):
        '''
        Store a minus80 dataset in the cloud using its name and dtype (e.g. Cohort).
        the dtype is the name of the Freezable class or object. See :ref:`freezable`.
        Assume we are storing ``x = Cohort('experiment1')``

        dtype : str
            The type of freezable object (i.e. 'Cohort')
        name : str
            The name of the dataset (i.e. 'experiment1')
        raw : bool, default=False
            If True, raw files can be stored in the cloud. In this case, name changes
            to the file name and dtype changes to a string representing the future dtype
            or anything that describes the type of data that is being stored.
        '''
        from boto3.s3.transfer import S3Transfer
        transfer = S3Transfer(self.s3)
        if raw == True:
            # The name is a FILENAME
            filename = name
            key = os.path.basename(filename)
            transfer.upload_file(filename, self.bucket, f'Raw/{dtype}.{key}',
                callback=ProgressPercentage(filename)
            )
        else:
            key = f'{dtype}.{name}' 
            data_path = os.path.join(
                cf.options.basedir,
                'databases', key
            )
            if not os.path.exists(data_path):
                raise ValueError('There were no datasets with that name')
            # Tar it up
            tarpath = os.path.join(cf.options.basedir,'tmp',key+'.tar')
            tar = tarfile.open(tarpath,'w',dereference=True)
            tar.add(data_path,recursive=True,arcname=key)
            tar.close()
            transfer.upload_file(tarpath, self.bucket, f'databases/{key}',
                callback=ProgressPercentage(tarpath)        
            )
            os.unlink(tarpath)

    def pull(self, dtype, name, raw=False, output=None):
        '''
        Retrive a minus80 dataset in the cloud using its name and dtype (e.g. Cohort).
        the dtype is the name of the Freezable class or object. See :ref:`freezable`.
        Assume we are storing ``x = Cohort('experiment1')``

        dtype : str
            The type of freezable object (i.e. 'Cohort')
        name : str
            The name of the dataset (i.e. 'experiment1')
        raw : bool, default=False
            If True, raw files can be stored in the cloud. In this case, name changes
            to the file name and dtype changes to a string representing the future dtype
            or anything that describes the type of data that is being stored.
        '''
        from boto3.s3.transfer import S3Transfer
        transfer = S3Transfer(self.s3)
        if raw == True:
            prefix_dir = 'Raw'
            name = os.path.basename(name)
            if output is None:
                output = name
        else:
            prefix_dir = 'databases'
            output = os.path.join(cf.options.basedir,'tmp',f'{dtype}.{name}.tar')
        # get the number of bytes in the object
        num_bytes = self.s3.list_objects(
                Bucket=self.bucket, 
                Prefix=f'{prefix_dir}/{dtype}.{name}'
        )['Contents'][0]['Size']
        # download the object
        transfer.download_file(
            self.bucket,
            f'{prefix_dir}/{dtype}.{name}',
            output,
            callback = ProgressDownloadPercentage(output,num_bytes)
        )
        # Extract if its a tar file
        if output.endswith('.tar'):
            tar = tarfile.open(output,'r')
            tar.extractall(path=os.path.join(cf.options.basedir,'databases'))
            

    def list(self, name=None, dtype=None, raw=False):
        '''
            List datasets that are in the cloud

            Parameters
            ----------
            dtype : str
                The type of freezable object (i.e. 'Cohort')
            name : str
                The name of the dataset (i.e. 'experiment1')
            raw : bool, default=False
                If true, list raw datasets instead of frozen ones.

        '''
        items = defaultdict(set)
        try:
            for item in self.s3.list_objects(Bucket=self.bucket)['Contents']:
                key = item['Key']
                if key.startswith('Raw') and raw == False:
                    pass
                elif not key.startswith('Raw') and raw == True:
                    pass
                else:
                    bucket, key = key.split('/')
                    key_dtype, key_name = key.split('.',maxsplit=1)
                    if dtype != None and key_dtype != dtype:
                        pass
                    elif name != None and not key_name.startswith(name):
                        pass
                    else:
                        items[key_dtype].add(key_name)
            if len(items) == 0:
                print('Nothing here yet!')
            else:
                if raw:
                    print('######   Raw Data:   ######')
                for key, vals in items.items():
                    print(f'-----{key}------')
                    for i,name in enumerate(vals,1):
                        print(f'{i}. {name}')
        except KeyError:
            if len(items) == 0:
                print('Nothing here yet!')

    def remove(self, dtype, name, raw=False):
        '''
            Remove a dataset from the cloud
        '''
        if raw:
            key = f'Raw/{dtype}.{name}'
        else:
            key = f'databases/{dtype}.{name}'
        self.s3.delete_object(Bucket=self.bucket,Key=key)

    def nuke(self):
        '''
            Nuke all the datasets in the cloud 
        '''
        try:
            for obj in self.s3.list_objects(Bucket=self.bucket)['Contents']:
                self.s3.delete_object(Bucket=self.bucket,Key=obj['Key'])
        except KeyError as e:
            return
