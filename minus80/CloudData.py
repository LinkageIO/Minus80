
def CloudData(engine='gcp'):
    if engine == 'gcp':
        from .GCPCloudData import GCPCloudData
        return GCPCloudData()
    else:
        raise ValueError(f'Cannot use {engine} as a cloud engine.')

class BaseCloudData(object): #pragma: no cover
    def __init__(self):
        pass

    def push(self, name, dtype, raw=False, compress=False):
        raise NotImplementedError('This engine does not support pushing')

    def pull(self, name, dtype, raw=False):
        raise NotImplementedError('This engine does not support pulling')

    def list(self, name=None, dtype=None, raw=None):
        raise NotImplementedError('This engine does not support listing')
