__version__ = "2.0.0"

API_VERSION = "v2"

import logging

__all__ = ["Freezable", "Accession", "CloudData", "Project", "Cohort", "tools", "FreezableAPI"]

from .Freezable import Freezable, FreezableAPI
from .Accession import Accession
from .CloudData import CloudData
from .Project import Project
from .Cohort import Cohort


log = logging.getLogger("minus80")
# One of DEBUG, INFO, WARNING, ERROR, CRITICAL
log.setLevel(logging.INFO)
# Set up the console handler
ch = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s | %(name)s | %(levelname)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S"
)
ch.setFormatter(formatter)
ch.setLevel(logging.INFO)
log.addHandler(ch)

# Initialize the Minus80 Freezer
def _init():  # pragma: no cover
    import os
    from .Config import cf

    rootdir = os.path.realpath(os.path.expanduser(cf.options.rootdir))

    # Create the rootdir if not exists
    try:
        os.makedirs(rootdir, exist_ok=True)
        os.makedirs(os.path.join(rootdir, "datasets"), exist_ok=True)
        os.makedirs(os.path.join(rootdir, "tmp"), exist_ok=True)
        os.makedirs(os.path.join(rootdir, "Raw"), exist_ok=True)
    except Exception:
        raise


_init()



def exists(dtype, name, rootdir=None):
    """
    Reports the available datasets **Frozen** in the minus80
    database.

    Parameters
    ----------
    dtype : str
        The data type of the dataset. E.g.: Cohort.
    name : str
        The name of the dataset.

    Returns
    -------
    bool, None
        If both dtype and name are specified, a bool is returned
        indiciating if the dataset is available. Otherise a formatted
        table is printed and None is returned.
    """
    return FreezableAPI.exists(dtype, name, rootdir=None)


def delete(dtype, name, rootdir=None):
    """
    Deletes files associated with Minus80 datasets.

    Parameters
    ----------
    name : str
        The name of the dataset you want to delete
    dtype : str
        Each dataset has a datatype associated with it. E.g.:
        `Cohort`. If no dtype is specified, all available dtypes
        will be returned.

    Returns
    -------
    int
        Returns the number of datasets deleted

    .. warning:: This is damaging. Deleted datasets cannot be recovered.
    """
    return FreezableAPI.delete(dtype, name, rootdir=None)


def datasets(dtype="*", name="*", rootdir=None, fullpath=False):
    """
    List datasets in the specified rootdir.


    Parameters
    ----------
    dtype: str, default=None
        The data type of the dataset. E.g.: Cohort.
        Note: accepts glob arguments.
    name: str, required
        The name of the dataset. Note: accepts glob arguments.
    rootdir : str
        The root directory to look for Minus80 datasets
    fullpath: bool, default=False
        If true, full paths to files will be returned
        if false, only filenames will be returned.


    .. note:: This will only return top level files which sometimes
              will be directories.
    """
    return FreezableAPI.datasets(dtype, name, rootdir=None)
