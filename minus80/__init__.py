__version__ = '0.3.3'


from .Freezable import Freezable
from .Accession import Accession
from .Cohort    import Cohort
from .CloudData import CloudData
import minus80.Tools as tools

# Initialize the Minus80 Freezer
def _init(): # pragma: no cover
    import os
    from .Config import cf
 
    basedir = os.path.realpath(
        os.path.expanduser(cf.options.basedir)
    )
    
    # Create the basedir if not exists
    try:
        os.makedirs(basedir, exist_ok=True)
        os.makedirs(os.path.join(basedir, "databases"), exist_ok=True)
        os.makedirs(os.path.join(basedir, "tmp"), exist_ok=True)
        os.makedirs(os.path.join(basedir, "Raw"), exist_ok=True)
    except Exception:
        raise

_init()
