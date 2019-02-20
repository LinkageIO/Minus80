
from .S3CloudData import S3CloudData

__all__ = ['CloudData']


def CloudData(engine='s3'):
    if engine == 's3':
        return S3CloudData()
    else:
        raise ValueError(f'Cannot use {engine} as a cloud engine.')



