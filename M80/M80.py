#!/usr/bin/env python3
import apsw as lite
import os as os
import tempfile
import numpy as np
import pandas as pd
import bcolz as bcz

# Suppress the warning until the next wersion
import warnings
from flask.exthook import ExtDeprecationWarning
warnings.simplefilter('ignore',ExtDeprecationWarning)
import blaze as blz

from .Tools import log
from .Config import cf
from .Exceptions import CamocoExistsError
from apsw import ConstraintError


class M80(object):

    '''
    An M80 instance is a persistant object that lives in a known directory
    aimed to make expensive to build objects and databases loadable from 
    new runtimes.
    
    The three main things that an M80 object supplies are:
    - access to a sqlite database (relational records)
    - access to a bcolz databsase (columnar data)
    - access to a key/val store
    - access to named temp files

    This class is meant to be used as an abstract class meaning that
    other classes inherit its methods and will have access to all the
    underlying methods.

    '''

    def __init__(self, name, type='M80'):
        # Set up our base directory
        self.log = log()
        self.type = type
        # A dataset already exists, return it
        self.db = self._database(name)
        try:
            (self.ID, self.name, self.description, self.type, self.added) = \
                self._database('M80', type='M80') \
                .cursor().execute(
                "SELECT rowid, * FROM datasets WHERE name = ? AND type = ?",
                (name, type)
            ).fetchone()
            cur = self.db.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS globals (
                    key TEXT,
                    val TEXT,
                    type TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uniqkey ON globals(key)
                ''')
        except TypeError:
            raise TypeError('{}.{} does not exist'.format(type, name))

    def _database(self, dbname, type=None):
        '''
        This is the access point to the sqlite database
        '''
        # This lets us grab databases for other types
        if type is None:
            type = self.type
        # return a connection if exists
        return lite.Connection(
            os.path.expanduser(
                os.path.join(
                    cf.options.basedir,
                    'databases',
                    "{}.{}.db".format(type, dbname)
                )
            )
        )

    def _bcolz(self, tblname, dbname=None, type=None, df=None, blaze=False):
        '''
        This is the access point to the bcolz database
        '''
        if type is None:
            type = self.type
        if dbname is None:
            dbname = self.name
        if df is None:
            # return the dataframe if it exists 
            try:
                df = bcz.open(
                    os.path.expanduser(
                        os.path.join(
                            cf.options.basedir,
                            'databases',
                            "{}.{}.{}".format(type, dbname, tblname)
                        )
                    )
                )
            except IOError:
                return None
            else:
                if len(df) == 0:
                    df = pd.DataFrame()
                    if blaze:
                        df = blz.data(df)
                else:
                    if blaze:
                        df = blz.data(df)
                    else:
                        df = df.todataframe()
                if not blaze and 'idx' in df.columns.values:
                    df.set_index('idx', drop=True, inplace=True)
                    df.index.name = None
                return df
        
        else:
            if not(df.index.dtype_str == 'int64') and not(df.empty):
                df = df.copy()
                df['idx'] = df.index
            if isinstance(df,pd.DataFrame):
                path = os.path.expanduser(
                        os.path.join(
                            cf.options.basedir,
                            'databases',
                            "{}.{}.{}".format(type, dbname, tblname)
                        )
                    )
                if df.empty:
                    bcz.fromiter((),dtype=np.int32,mode='w',count=0,rootdir=path)
                else:
                    bcz.ctable.fromdataframe(df,mode='w',rootdir=path)
                
            if 'idx' in df.columns.values:
                del df
            return
    
    def _tmpfile(self):
        # returns a handle to a tmp file
        return tempfile.NamedTemporaryFile(
            dir=os.path.expanduser(
                os.path.join(
                    cf.options.basedir,
                    "tmp"
                )   
            )
        )

    def _global(self, key, val=None):
        # set the global for the dataset
        try:
            if val is not None:
                self.db.cursor().execute('''
                    INSERT OR REPLACE INTO globals
                    (key, val)VALUES (?, ?)''', (key, val)
                )
            else:
                return self.db.cursor().execute(
                    '''SELECT val FROM globals WHERE key = ?''', (key, )
                ).fetchone()[0]
        except TypeError:
            # It pains me to do, but but return none if key isn't in global
            # TODO: replace returning None with an exception
            return None

    def __getattr__(self, name):
        return self._global(name)

    def __del__(self):
        '''
            Destroy a Camoco object and associated files.
        '''
        pass

    @classmethod
    def create(cls, name, description, type='Camoco'):
        '''
            This is a class method to create a new M80 type object.
            It initializes base directory hierarchy
        '''
        basedir = os.path.realpath(
            os.path.expanduser(cf.options.basedir)
        )

        # Create the basedir if not exists
        try:
            os.makedirs(basedir, exist_ok=True)
            os.makedirs(os.path.join(basedir, "logs"), exist_ok=True)
            os.makedirs(os.path.join(basedir, "databases"), exist_ok=True)
            os.makedirs(os.path.join(basedir, "analyses"), exist_ok=True)
            os.makedirs(os.path.join(basedir, "tmp"), exist_ok=True)
        except Exception:
            log('Could not create files in {}', basedir)
            raise
        try:
            # Create the base camoco database
            lite.Connection(
                os.path.join(basedir, 'databases', 'Camoco.Camoco.db')
            ).cursor().execute('''
                CREATE TABLE IF NOT EXISTS datasets (
                    name TEXT NOT NULL,
                    description TEXT,
                    type TEXT,
                    added datetime DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(name, type)
                );
                INSERT OR IGNORE INTO datasets (name, description, type)
                    VALUES ('Camoco', 'Camoco base', 'Camoco');
                INSERT OR IGNORE INTO datasets (name, description, type)
                    VALUES (?, ?, ?)''', (name, description, type)
            )
        except ConstraintError:
            raise CamocoExistsError(
                'CAUTION! {}.{} Database already exists.', name, type
            )
        self = cls(name)
        return self
