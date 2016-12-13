import pandas as pd
import apsw as lite
import os as os


class M80(object):

    def __init__(self):
        basedir = os.path.expanduser('~/.M80') 

        os.makedirs(os.path.join(basedir,'databases'),exist_ok=True)
        self._db = lite.Connection(os.path.join(basedir,'databases','M80.sqlite')) 

        self._db.cursor().execute(''' 
            CREATE TABLE IF NOT EXISTS Samples (
                ID NOT NULL,
                PRIMARY KEY(ID)
            );        
            CREATE TABLE IF NOT EXISTS Files (
                ID NOT NULL,
                system TEXT,
                path TEXT,
                PRIMARY KEY(ID, system, path)
            );
            CREATE TABLE IF NOT EXISTS Metadata (
                ID NOT NULL,
                key NOL NULL,
                val NOT NULL,
                PRIMARY KEY(ID,key,val)
            );
        ''')

    @classmethod
    def from_googlesheet(cls):
        pass
