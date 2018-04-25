__version__ = '0.1.1'


from .Freezable import Freezable
from .Accession import Accession
from .Cohort    import Cohort
from .CloudData import CloudData


# Initialize the Minus80 Freezer
def _init():
    import os
    import apsw as lite
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
    # Create the base database
#    lite.Connection(
#        os.path.join(basedir, 'databases', 'Minus80.Freezer.db')
#    ).cursor().execute('''
#        CREATE TABLE IF NOT EXISTS datasets (
#            name TEXT NOT NULL,
#            description TEXT,
#            type TEXT,
#            added datetime DEFAULT CURRENT_TIMESTAMP,
#            PRIMARY KEY(name, type)
#        );
#        INSERT OR IGNORE INTO datasets (name, description, type)
#        VALUES (?, ?, ?)''', ('Freezer', "Freezer Database", 'Minus80')
#    )   

_init()
