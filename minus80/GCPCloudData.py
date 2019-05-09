import os
import tarfile

from collections import defaultdict
from google.cloud import storage
from minus80 import __version__ as m80_version

from .Config import cf
from .CloudData import BaseCloudData


class GCPCloudData(BaseCloudData):

    '''
    Google Cloud Platform CloudData objects allow minus80 to interact with 
    the cloud to store both prepared (freezable) data as well as raw data.
    '''

    def __init__(self):
        # GCP uses a JSON and defaults to ENV variables, if not
        # set, pull a path from the config file
        if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
            credential_path = os.path.expanduser(
                os.path.join(
                    cf.gcp.credentials
                )
            )
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

        try:
            self.client = storage.Client()
            self.bucket_name = cf.gcp.bucket
            if self.client.lookup_bucket(self.bucket_name) is None:
                # Create the bucket
                self.bucket = self.client.create_bucket(self.bucket_name)
            else:
                self.bucket = self.client.get_bucket(self.bucket_name)

        except Exception as e:
            raise ValueError(
                'Cloud access requires setting up GCP credentials in ~/.minus80.conf '
                'contact help@linkage.io for assistance', e
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
        if raw == True:
            # the name is a FILENAME
            filename = name
            key = os.path.basename(filename)
            blob = self.bucket.blob(f'Raw/{dtype}.{key}') 
            blob.upload_from_filename(filename)
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
            # create a blob in the bucket
            blob = self.bucket.blob(f'databases/{key}')
            blob.upload_from_filename(tarpath)
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
        if raw == True:
            prefix_dir = 'Raw'
            name = os.path.basename(name)
            if output is None:
                output = name
            else:
                # create any directories for the outpu
                os.makedirs(os.path.dirname(output),exist_ok=True)
        else:
            prefix_dir = 'databases'
            output = os.path.join(cf.options.basedir,'tmp',f'{dtype}.{name}.tar')
        # get the blob and download
        blob = self.bucket.get_blob(f'{prefix_dir}/{dtype}.{name}')
        if blob is None:
            raise NameError(f'{name} does not exist as a {dtype}')
        # check to see if output requires creating a directory
        blob.download_to_filename(output) 
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
            for blob in self.bucket.list_blobs():
                key = blob.name
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

            Parameters
            ----------
            dtype : str
                The type of dataset (e.g. 'Cohort'). Types can be seen 
                using the list command.
            name : str
                The name of the dataset to delete.
            raw : bool, default=False
                If True, specifies a raw dataset. (see list function)
        '''
        if raw:
            name = os.path.basename(name)
            key = f'Raw/{dtype}.{name}'
        else:
            key = f'databases/{dtype}.{name}'
        self.bucket.delete_blob(key)

    def nuke(self):
        '''
            Nuke all the datasets in the cloud.

            Warning
            -------
            This WILL delete all your data
        '''
        self.bucket.delete_blobs(self.bucket.list_blobs())
