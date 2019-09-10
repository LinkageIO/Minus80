def CloudData(engine="firebase"):
    if engine == 'firebase':
        from .FireBaseCloudData import FireBaseCloudData
        return FireBaseCloudData()
    else:
        raise ValueError(f"Cannot use {engine} as a cloud engine.")


class BaseCloudData(object):  # pragma: no cover
    def __init__(self):
        pass

    def push(self, dtype, name, tag):
        raise NotImplementedError("This engine does not support pushing")

    def pull(self, dtype, name, tag):
        raise NotImplementedError("This engine does not support pulling")

    def list(self, dtype=None, name=None):
        raise NotImplementedError("This engine does not support listing")
