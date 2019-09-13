__version__ = "1.0.0-dev"

SLUG_VERSION = 'v1'

from .Freezable import Freezable
from .Accession import Accession
from .CloudData import CloudData
from .Project import Project
from .Cohort import Cohort

import minus80.Tools as tools

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
