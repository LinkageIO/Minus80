import os
import h5py
import numpy
import pandas

import pandas as pd

from pathlib import Path

__all__ = ["columnar_db"]


def columnar_db(rootdir, engine="parquet"):  # pragma: no cover
    if engine == "parquet":
        return parquet_db(rootdir)
    else:
        raise ValueError("Engine must be one of: ['parquet']")


class ColumnarDB(object):  # pragma: no cover
    "Abstract Base Class for Colmnar DBs engines"

    def __init__(self, rootdir):
        raise NotImplementedError()

    def remove(self, name):
        raise NotImplementedError()

    def list(self):
        raise NotImplementedError()

    def __contains__(self, key):
        raise NotImplementedError()

    def __setitem__(self, name, val):
        raise NotImplementedError()

    def __getitem__(self, name):
        raise NotImplementedError()


class parquet_db(ColumnarDB):
    """
    A Parquet engine for storing columnar data in Minus80
    """

    _MAGICKEY = "__MINUS80ARRAY__"
    
    def __init__(self, rootdir):
        self._pqdir = Path(os.path.expanduser(os.path.join(rootdir, "pq")))
        os.makedirs(self._pqdir, exist_ok=True)

    def remove(self, name):
        os.remove(self._pqdir / f"{name}.pq") 

    def list(self):
        return [pq.removesuffix(".pq") for pq in os.listdir(self._pqdir)]

    def __contains__(self, key):
        return f"{key}.pq" in os.listdir(self._pqdir)

    def __setitem__(self, name, val):
        if isinstance(val, numpy.ndarray):
            val = pd.DataFrame({self._MAGICKEY: val})
            val.to_parquet(self._pqdir / f"{name}.pq")
        # Handle data frames
        elif isinstance(val, pandas.DataFrame):
            val.to_parquet(self._pqdir / f"{name}.pq")
        else:
            raise ValueError("Datatype must be either a numpy array or a dataframe")


    def __getitem__(self, name):
        val = pd.read_parquet(self._pqdir / f"{name}.pq") 
        if '__MINUS80ARRAY__' in val.columns and val.columns[0] == '__MINUS80ARRAY__':
            return val['__MINUS80ARRAY__'].to_numpy()
        return val
