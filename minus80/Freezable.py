#!/usr/bin/env python3
import re
import tempfile
import hashlib

import os as os
import numpy as np
import pandas as pd

from shutil import rmtree as rmdir
from tinydb import TinyDB
from pathlib import Path


from minus80.RelationalDB import relational_db
from minus80.ColumnDB import columnar_db


from .Config import cf
from .Tools import guess_type


__all__ = ["Freezable"]


class Freezable(object):
    """
    Freezable is an base class. Things that inherit from Freezable can
    be loaded and unloaded from the Minus80.

    A freezable object is a persistant object that lives in a known directory
    aimed to make expensive to build objects and databases loadable from
    new runtimes.

    The three main things that a Freezable object supplies are:
    * access to a sqlite database (relational records)
    * access to a bcolz databsase (columnar/table data)
    * access to a persistant key/val document store
    * access to named temp files

    """

    def __init__(self, name, parent=None, basedir=None):
        """
            Freezable object inherit an insance to the Freezable API.
        """
        # Guess the dtype based on the class name
        dtype = guess_type(self)
        self.m80 = FreezableAPI(dtype, name, parent, basedir)


class FreezableAPI(object):
    def __init__(self, dtype, name, basedir=None):
        """
        Initialize the Freezable Object.

        Parameters
        ----------
        name : str
            The name of the frozen object.
        parent: Freezable object or None
            The parent object
        """
        # Set the m80 name
        self.name = name
        # Set the m80 dtype
        self.dtype = dtype
        self.tag = tag

        # Default to the basedir in the config file
        if basedir is None:
            basedir = Path(cf.options.basedir).expanduser() / 'databases'
        else:
            basedir = Path(basedir).expanduser()
        # Create the base dir
        self.basedir = basedir / self.slug
        )
        os.makedirs(self.basedir, exist_ok=True)
        # Set up the columnar db
        self.col = columnar_db(self.basedir)
        # Get a handle to the sql database
        self.db = relational_db(self.basedir)
        # Set up a table
        self.doc = TinyDB(os.path.join(self.basedir, "tinydb.json"))

    @property
    def slug(self):
        if self.tag is not None:
            return f'{self.dtype}.{self.name}:{self.tag}'
        else:
            return f'{self.dtype}.{self.name}'

    @property
    def checksum(self):
        """
        Calculates the checksum of all the files in the freezable 
        objects database directory   
        """
        def update_hash(running_hash, filepath):
            with open(filepath, 'rb') as IN:
                while True:
                    # Read file in as little chunks.
                    buf = IN.read(4096)
                    if not buf:
                        break
                    running_hash.update(buf)
        sha_hash = hashlib.sha256('{self.name}.{self.dtype}'.encode('utf-8'))
        # iterate over the direcory and calucalte the hash
        for root, dirs, files in os.walk(self.basedir):
            for names in sorted(files):
                filepath = os.path.join(root, names)
                update_hash(running_hash=sha_hash,
                            filepath=filepath)
        return sha_hash.hexdigest()        

    @staticmethod
    def tmpfile(*args, **kwargs):
        # returns a handle to a tmp file
        return tempfile.NamedTemporaryFile(
            "w",
            dir=os.path.expanduser(
                os.path.join(
                    # use the top level basedir
                    cf.options.basedir,
                    "tmp",
                )
            ),
            **kwargs,
        )
