

class GCPCloudData(BaseCloudData):

    '''
    Google Cloud Platform CloudData objects allow minus80 to interact with 
    the cloud to store both prepared (freezable) data as well as raw data.
    '''

    def __init__(self):
        pass

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
