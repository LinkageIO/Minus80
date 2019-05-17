#!/usr/bin/env python3
import tempfile
import re
# Suppress the warning until the next wersion
import bcolz as bcz

import os as os
import numpy as np
import pandas as pd

from .Config import cf
from contextlib import contextmanager
from shutil import rmtree as rmdir

try:
    import apsw as lite
except ModuleNotFoundError as e:
    from .Tools import install_apsw
    install_apsw()
    import apsw as lite


__all__ = ['Freezable']

class sqlite_dict(object):
    def __init__(self,con):
        self._con = con
        con.cursor().execute('''
            CREATE TABLE IF NOT EXISTS globals (
                key TEXT,
                val TEXT,
                type TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uniqkey ON globals(key)
        ''')


    def __call__(self,key,val=None):
        try:
            if val is not None:
                val_type = guess_type(val)
                if val_type not in ('int', 'float', 'str'):
                    raise TypeError(
                        f'val must be in [int, float, str], not {val_type}'
                    )
                self._con.cursor().execute(
                    '''
                    INSERT OR REPLACE INTO globals
                    (key, val, type)VALUES (?, ?, ?)''', (key, val, val_type)
                )
            else:
                (valtype, value) = self._con.cursor().execute(
                    '''SELECT type, val FROM globals WHERE key = ?''', (key, )
                ).fetchone()
                if valtype == 'int':
                    return int(value)
                elif valtype == 'float':
                    return float(value)
                elif valtype == 'str':
                    return str(value)
        except TypeError:
            raise ValueError('{} not in database'.format(key))

    def __contains__(self,key):
        (num,) = self._con.cursor().execute(
            'SELECT COUNT(key) FROM globals WHERE key = ?', (key,)
        ).fetchone()
        if num == 0:
            return False
        elif num == 1:
            return True

    def keys(self):
        all_keys = self._con.cursor().execute('SELECT key from globals')
        return [x for x, in all_keys ]

    def __getitem__(self,key):
        return self(key)

    def __setitem__(self,key,val):
        self(key,val=val)

    def __delitem__(self,key):
        self._con.cursor().execute(
            'DELETE FROM globals WHERE key = ?',(key,)
        )


class Freezable(object):

    '''
    Freezable is an abstract class. Things that inherit from Freezable can
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
        # Keep track of children
        self._children = []
       
        if basedir is None:
            basedir = cf.options.basedir

        # Set up our base directory
        if parent is None:
            # set as the top level basedir as specified in the config file
            self._basedir = os.path.join(
                basedir,
                'databases',
                f'{self._m80_dtype}.{self._m80_name}'
            )
            self._parent = None
        else:
            self._basedir = os.path.join(
                parent._basedir,
                f'{self._m80_dtype}.{self._m80_name}'
            )
            self._parent = parent
            parent._add_child(self)
        os.makedirs(self._basedir,exist_ok=True)

        # Get a handle to the sql database
        self._db = self._sqlite()
        # Set up a table
        self._dict = sqlite_dict(self._db) 


    def _add_child(self,child):
        '''
            Register a child dataset
        '''
        self._children.append(child)

    @contextmanager
    def _bulk_transaction(self):
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
        cur.execute('SAVEPOINT bulk_transaction')
        try:
            yield cur
        except Exception as e:
            cur.execute('ROLLBACK TO SAVEPOINT bulk_transaction')
            raise e
        finally:
            cur.execute('RELEASE SAVEPOINT bulk_transaction')

    def _query(self,q):
        cur = self._db.cursor().execute(q)
        names = [x[0] for x in cur.description]
        rows = cur.fetchall()
        result = pd.DataFrame(rows,columns=names)
        return result


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


    def _sqlite(self):
        '''
            This is the access point to the sqlite database
        '''
        # return a connection if exists
        filename = os.path.join(self._get_dbpath('db.sqlite'))
        return lite.Connection(filename)

    def _bcolz_remove(self,name):
        '''
            Remove a bcolz array from disk
        '''
        path = os.path.join(self._get_dbpath('bcz'),name)
        if not os.path.exists(path):
            raise ValueError(f'{name} does not exist')
        else:
            rmdir(path)

    def _bcolz_list(self):
        '''
            List the available bcolz datasets
        '''
        return os.listdir(self._get_dbpath('bcz'))
       

    def _bcolz_array(self, name, array=None, m80name=None,
                     m80type=None):
        '''
            Routines to set/get arrays from the bcolz store
        '''

        # Fill in the defaults if they were not provided
        if m80type is None:
            m80type = self._m80_dtype
        if m80name is None:
            m80name = self._m80_name
        # function is a getter if df is provided
        path = self._get_dbpath('bcz', create=True)
        if array is None:
            # GETTER
            arr = bcz.open(os.path.join(path, name))
            return arr
        else:
            # SETTER
            bcz.carray(array, mode='w', rootdir=os.path.join(path, name))

    def _bcolz(self, tblname, df=None, m80name=None, m80type=None,
               blaze=False):
        '''
            This is the access point to the bcolz database
        '''
        try:
            import blaze as blz
        except FutureWarning: # pragma: no cover
            pass
        import warnings
        # from flask.exthook import ExtDeprecationWarning
        # warnings.simplefilter('ignore', ExtDeprecationWarning)
        warnings.simplefilter('ignore', FutureWarning)

        # Fill in the defaults if they were not provided
        if m80type is None:
            m80type = self._m80_dtype
        if m80name is None:
            m80name = self._m80_name
        path = self._get_dbpath('bcz',create=True)

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
                    if blaze:
                        df = blz.data(df)
                else:
                    if blaze:
                        df = blz.data(df)
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


def guess_type(object):
    '''
        Guess the type of object from the class attribute
    '''
    # retrieve a list of classes
    classes = re.match(
        "<class '(.+)'>",
        str(object.__class__)
    ).groups()[0].split('.')
    # Return the most specific one
    return classes[-1]
