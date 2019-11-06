__version__ = "1.0.0"

API_VERSION = 'v1'

import logging

from .Freezable import Freezable
from .Accession import Accession
from .CloudData import CloudData
from .Project import Project
from .Cohort import Cohort

import minus80.Tools as tools

log = logging.getLogger('minus80')
log.setLevel(logging.DEBUG)
# Set up the console handler
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
ch.setFormatter(formatter)
ch.setLevel(logging.INFO)
log.addHandler(ch)

# Initialize the Minus80 Freezer
def _init():  # pragma: no cover
    import os
    from .Config import cf

    basedir = os.path.realpath(os.path.expanduser(cf.options.basedir))

    # Create the basedir if not exists
    try:
        os.makedirs(basedir, exist_ok=True)
        os.makedirs(os.path.join(basedir, "datasets"), exist_ok=True)
        os.makedirs(os.path.join(basedir, "tmp"), exist_ok=True)
        os.makedirs(os.path.join(basedir, "Raw"), exist_ok=True)
    except Exception:
        raise


_init()
