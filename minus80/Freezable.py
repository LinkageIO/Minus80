#!/usr/bin/env python3
import re
import tempfile

import bcolz as bcz
import os as os
import numpy as np
import pandas as pd

from shutil import rmtree as rmdir
from tinydb import TinyDB


from minus80.RelationalDB import relational_db
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
    def __init__(self,name,parent=None,basedir=None):
        '''
            Freezable object inherit an insance to the Freezable API.
        '''
        dtype = guess_type(self)
        self.m80 = FreezableAPI(dtype,name,parent,basedir)

class FreezableAPI(object):

    def __init__(self, dtype, name, parent=None, basedir=None):
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
        self.name = name
        # Set the m80 dtype
        self.dtype = dtype
      
        # default to the basedir in the config file
        if basedir is None:
            basedir = cf.options.basedir
        # Set up our base directory
        if parent is None:
            # set as the top level basedir as specified in the config file
            self.basedir = os.path.join(
                basedir,
                'databases',
                f'{self.dtype}.{self.name}'
            )
            self.parent = None
        else:
            # set up the basedir to be within the parent basedir
            self.basedir = os.path.join(
                parent.basedir,
                f'{self.dtype}.{self.name}'
            )
            self.parent = parent
            self.parent.add_child(self)
        # Create the base dir
        os.makedirs(self.basedir,exist_ok=True)

        # Set up the columnar db
        self.col = columnar_db(self.basedir)
        # Get a handle to the sql database
        self.db = relational_db(self.basedir)
        # Set up a table
        self.doc = TinyDB(os.path.join(self.basedir,"tinydb.json"))

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

