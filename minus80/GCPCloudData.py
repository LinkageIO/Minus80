import os
import tarfile

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
        
        credential_path = os.path.join(cf.options.basedir,'gcp_creds.json')
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
            blob = self.bucket.blob(f'Raw/{dtype}/{key}') 
            blob.upload_from_filename(filename)
        else:
            key = f'{dtype}/{name}'
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
        pass

    def list(self, name=None, dtype=None, raw=False):
        pass

    def remove(self, dtype, name, raw=False):
        pass

    def nuke(self):
        pass
