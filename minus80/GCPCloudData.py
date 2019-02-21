
from google.cloud import storage

class GCPCloudData(BaseCloudData):

    '''
    Google Cloud Platform CloudData objects allow minus80 to interact with 
    the cloud to store both prepared (freezable) data as well as raw data.
    '''

    def __init__(self):
        
        gcp_endpoint   = cf.gcp.endpoint
        gcp_bucket     = cf.gcp.bucket
        gcp_access_key = cf.gcp.access_key
        gcp_secret_key = cf.gcp.secret_key

        try:
            self.client = storage.Client()
        except Exception as e:
            raise ValueError(
                'Cloud access requires setting up GCP credentials in ~/.minus80.conf '
                'contact help@linkage.io for assistance', e
            )

    def push(self, dtype, name, raw=False):
        pass
    
    def pull(self, dtype, name, raw=False, output=None):
        pass

    def list(self, name=None, dtype=None, raw=False):
        pass

    def remove(self, dtype, name, raw=False):
        pass

    def nuke(self):
        pass
