import os
import h5py
import numpy
import pandas

__all__ = ["columnar_db"]


def columnar_db(basedir, engine="hdf5"):
    if engine == "hdf5":
        return hdf5_db(basedir)
    else:
        raise ValueError("Engine must be one of ['hdf5']")


class ColumnarDB(object):
    "Abstract Base Class for Colmnar DBs engines"

    def __init__(self, basedir, engine=None):
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


class bcolz_db(ColumnarDB):
    def __init__(self, basedir, engine=None):
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


class hdf5_db(ColumnarDB):
    """
        An HDF5 engine for storing columnar data in Minus80
    """

    def __init__(self, basedir):
        self.filename = os.path.expanduser(os.path.join(basedir, "db.hdf5"))
        # This creates an empty db if it doesnt exist
        with h5py.File(self.filename, "a") as hdf:
            pass

    def remove(self, name):
        """
            Remove a  dataframe/array from disk
        """
        with h5py.File(self.filename, "a") as hdf:
            del hdf[name]

    def list(self):
        """
            List the available bcolz datasets
        """
        with h5py.File(self.filename, "r") as hdf:
            keys = list(hdf.keys())
        return keys

    def __contains__(self, name):
        with h5py.File(self.filename, "r") as hdf:
            return name in hdf

    def __setitem__(self, name, val):
        if isinstance(val, numpy.ndarray):
            # check if exists and if so, delete
            if name in self:
                self.remove(name)
            with h5py.File(self.filename, "a") as hdf:
                hdf[name] = val
        elif isinstance(val, pandas.DataFrame):
            val.to_hdf(self.filename, key=name, mode="a")
        else:
            raise ValueError("Datatype must be either a numpy array or a dataframe")

    def __getitem__(self, name):
        with h5py.File(self.filename, "r") as hdf:
            val = hdf[name]
            # dataset -> np.array
            if isinstance(val, h5py.Dataset):
                return val[:]
            elif isinstance(val, h5py.Group):
                return pandas.read_hdf(self.filename, key=name, mode="r")
