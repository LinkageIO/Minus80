
import os
from google.cloud import storage
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
            self.bucket = cf.gcp.bucket
            
            if self.client.lookup_bucket(self.bucket) is None:
                # Create the bucket
                self.client.create_bucket(self.bucket)

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

        pass
    
    def pull(self, dtype, name, raw=False, output=None):
        pass

    def list(self, name=None, dtype=None, raw=False):
        pass

    def remove(self, dtype, name, raw=False):
        pass

    def nuke(self):
        pass
