#!/usr/bin/env python3
import re
import tempfile

import bcolz as bcz
import os as os
import numpy as np
import pandas as pd

from shutil import rmtree as rmdir


from minus80.RelationalDB import relational_db
from minus80.SQLiteDict import sqlite_dict
from minus80.ColumnDB import columnar_db


from .Config import cf
from .Tools import guess_type


__all__ = ['Freezable']


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

        # Set up the columnar db
        self._m80col = columnar_db(self._m80_basedir)
        # Get a handle to the sql database
        self._m80db = relational_db(self._m80_basedir)
        # Set up a table
        self._m80_dict = sqlite_dict(self._m80db) 

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

