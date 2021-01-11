__version__ = "2.0.0"

API_VERSION = "v2"

import logging

__all__ = ["Freezable", "Accession", "CloudData", "Project", "Cohort", "tools"]

from .Freezable import Freezable
from .Accession import Accession
from .CloudData import CloudData
from .Project import Project
from .Cohort import Cohort

import minus80.Tools as tools

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
