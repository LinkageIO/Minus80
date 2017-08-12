__version__ = '0.0.1'


from .Freezable import Freezable
from .Sample import Sample


# Initialize the Minus80 Freezer
def _init():
    import os
    import apsw as lite
    from .Log import log
    from .Config import cf
    
    basedir = os.path.realpath(
        os.path.expanduser(cf.options.basedir)
    )
    
    # Create the basedir if not exists
    try:
        os.makedirs(basedir, exist_ok=True)
        os.makedirs(os.path.join(basedir, "databases"), exist_ok=True)
        os.makedirs(os.path.join(basedir, "tmp"), exist_ok=True)
    except Exception:
        log('Could not create files in {}', basedir)
        raise
    # Create the base camoco database
    lite.Connection(
        os.path.join(basedir, 'databases', 'Minus80.Freezer.db')
    ).cursor().execute('''
        CREATE TABLE IF NOT EXISTS datasets (
            name TEXT NOT NULL,
            description TEXT,
            type TEXT,
            added datetime DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(name, type)
        );
        INSERT OR IGNORE INTO datasets (name, description, type)
        VALUES (?, ?, ?)''', ('Freezer', "Freezer Database", 'Minus80')
    )   

_init()
