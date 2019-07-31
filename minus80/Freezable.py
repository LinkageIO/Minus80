#!/usr/bin/env python3
import re
import tempfile

import bcolz as bcz
import os as os
import numpy as np
import pandas as pd

from contextlib import contextmanager
from shutil import rmtree as rmdir

from .Config import cf
from .SQLiteDict import sqlite_dict
from .Tools import guess_type

try:
    import apsw as lite
except ModuleNotFoundError as e:
    from .Tools import install_apsw
    install_apsw()
    import apsw as lite


__all__ = ['Freezable']


class relational_db(object):
    def __init__(self, basedir):
        self.filename = os.path.expanduser(
            os.path.join(basedir,'db.sqlite')        
        )
        self.db = lite.Connection(self.filename)

    def cursor(self):
        return self.db.cursor()

    @contextmanager
    def bulk_transaction(self):
        '''
            This is a context manager that handles bulk transaction.
            i.e. this context will handle the BEGIN, END and appropriate
            ROLLBACKS.

            Usage:
            >>> with x._bulk_transaction() as cur:
                     cur.execute('INSERT INTO table XXX VALUES YYY')
        '''
        cur = self._db.cursor()
        cur.execute('PRAGMA synchronous = off')
        cur.execute('PRAGMA journal_mode = memory')
        cur.execute('SAVEPOINT m80_bulk_transaction')
        try:
            yield cur
        except Exception as e:
            cur.execute('ROLLBACK TO SAVEPOINT m80_bulk_transaction')
            raise e
        finally:
            cur.execute('RELEASE SAVEPOINT m80_bulk_transaction')

    def query(self,q):
        cur = self._db.cursor().execute(q)
        names = [x[0] for x in cur.description]
        rows = cur.fetchall()
        result = pd.DataFrame(rows,columns=names)
        return result



class columnar_db(object):
    def __init__(self,basedir):
        self.dirname = os.path.expanduser(
            os.path.join(basedir,'bcz')        
        )


    def remove(self,name):
        '''
            Remove a bcolz array from disk
        '''
        path = os.path.join(self.dirname,name)
        if not os.path.exists(path):
            raise ValueError(f'{name} does not exist')
        else:
            rmdir(path)

    def list(self):
        '''
            List the available bcolz datasets
        '''
        return os.listdir(self.dirname)
       
    def __getitem__(self,name):
        arr = bcz.open(os.path.join(self.dirname, name))
        return arr

    def __setitem__(self,val):
        if isinstance(val,np.array):
            bcz.carray(
                val, mode='w', rootdir=os.path.join(self.dirname, name)
            )
        elif isinstance(val,

    def _bcolz(self, tblname, df=None):

        # function is a getter if df is provided
        if df is None:
            # return the dataframe if it exists
            try:
                df = bcz.open(os.path.join(path, tblname))
            except IOError:
                raise IOError(
                    f'could not open database for {m80type}:{m80name} '
                )
            else:
                if len(df) == 0:
                    df = pd.DataFrame()
                else:
                    df = df.todataframe()
                if not blaze and f'{tblname}_index' in self._dict:
                    df.set_index(
                        self._dict[f'{tblname}_index'], 
                        inplace=True)
                return df
        # If df is set, then store the table
        else:
            df = df.copy()
            if df.index.name is not None:
                # We need to remember to index
                self._dict[tblname+'_index'] = df.index.name
                df.reset_index(inplace=True)
            path = os.path.join(path, tblname)
            if df.empty:
                bcz.fromiter(
                    (), dtype=np.int32, mode='w',
                    count=0, rootdir=path
                )
            else:
                bcz.ctable.fromdataframe(df, mode='w', rootdir=path)
            return


class Freezable(object):

    '''
    Freezable is an base class. Things that inherit from Freezable can
    be loaded and unloaded from the Minus80.

    A freezable object is a persistant object that lives in a known directory
    aimed to make expensive to build objects and databases loadable from
    new runtimes.

    The three main things that a Freezable object supplies are:
    * access to a sqlite database (relational records)
    * access to a bcolz databsase (columnar/table data)
    * access to a persistant key/val store
    * access to named temp files

    '''

    def __init__(self, name, parent=None, basedir=None):
        '''
        Initialize the Freezable Object.

        Parameters
        ----------
        name : str
            The name of the frozen object.
        parent: Freezable object or None
            The parent object
        '''
        # Set the m80 name
        self._m80_name = name
        # Set the m80 dtype
        self._m80_dtype = guess_type(self)
      
        # default to the basedir in the config file
        if basedir is None:
            basedir = cf.options.basedir
        # Set up our base directory
        if parent is None:
            # set as the top level basedir as specified in the config file
            self._m80_basedir = os.path.join(
                basedir,
                'databases',
                f'{self._m80_dtype}.{self._m80_name}'
            )
            self._m80_parent = None
        else:
            # set up the basedir to be within the parent basedir
            self._m80_basedir = os.path.join(
                parent._m80_basedir,
                f'{self._m80_dtype}.{self._m80_name}'
            )
            self._m80_parent = parent
            self._m80_parent._m80_add_child(self)
        # Create the base dir
        os.makedirs(self._m80_basedir,exist_ok=True)

        # Get a handle to the sql database
        self._m80db = relational_db(self.basedir)
        # Set up a table
        self._m80_dict = sqlite_dict(self._db) 

    @staticmethod
    def _tmpfile(*args, **kwargs):
        # returns a handle to a tmp file
        return tempfile.NamedTemporaryFile(
            'w',
            dir=os.path.expanduser(
                os.path.join(
                    # use the top level basedir
                    cf.options.basedir,
                    "tmp"
                )
            ),
            **kwargs
        )

    def _get_dbpath(self, extension, create=False):
        '''
        Get the path to database files

        Parameters
        ----------
        '''
        path = os.path.expanduser(
            os.path.join(
                self._basedir,
                f'{extension}'
            )
        )
        if create:
            os.makedirs(path,exist_ok=True)
        return path







