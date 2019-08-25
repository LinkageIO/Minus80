#!/usr/bin/env python3
import re
import shutil
import hashlib
import logging
import tempfile

import os as os
import numpy as np
import pandas as pd

from tinydb import Query
from tinydb import where
from pathlib import Path
from tinydb import TinyDB
from datetime import datetime

from minus80.RelationalDB import relational_db
from minus80.ColumnDB import columnar_db


from .Config import cf
from .Tools import guess_type

from .Exceptions import (TagExistsError, 
                        TagDoesNotExistError, 
                        UnsavedChangesInThawedError)


__all__ = ["Freezable"]

log = logging.getLogger('minus80')

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

    def __init__(self, name, basedir=None):
        """
            Freezable object inherit an insance to the Freezable API.
        """
        # Guess the dtype based on the class name
        dtype = guess_type(self)
        self.m80 = FreezableAPI(dtype, name, basedir)


class FreezableAPI(object):
    def __init__(self, dtype, name, basedir=None):
        """
        Initialize the Freezable Object.

        Parameters
        ----------
        name : str
            The name of the frozen object.
        """
        # Set the m80 name
        self.name = self._validate_freezable_name(name)
        # Set the m80 dtype
        self.dtype = self._validate_freezable_name(dtype)

        # Default to the basedir in the config file
        if basedir is None:
            basedir = Path(cf.options.basedir).expanduser() / 'datasets'
        else:
            basedir = Path(basedir).expanduser()
        # Create the base dir
        self.basedir = basedir / self.slug
        os.makedirs(self.basedir, exist_ok=True)
        os.makedirs(self.thawed_dir, exist_ok=True)
        os.makedirs(self.frozen_dir, exist_ok=True)

        # Set up the columnar db
        self.col = columnar_db(self.thawed_dir)
        # Get a handle to the sql database
        self.db = relational_db(self.thawed_dir)
        # Set up a table
        self.doc = TinyDB(os.path.join(self.thawed_dir, "tinydb.json"))


    # Class Properties --------------------------------------------------

    @property
    def _manifest(self):
        return TinyDB(self.basedir / 'MANIFEST.json')

    @property
    def slug(self):
        return f'{self.dtype}.{self.name}'

    @property
    def thawed_dir(self):
        return self.basedir / 'thawed'

    @property
    def frozen_dir(self):
        return self.basedir / 'frozen'

    @property
    def tags(self):
        '''
            Returns the available frozen "tags"  
        '''
        tag_docs = self._manifest.search(where('tag').exists())
        tags = set([x['tag'] for x in tag_docs])
        # remove the "thawed" tag
        tags.discard('thawed')
        return tags

    @property
    def thawed_tag(self):
        tag = self._manifest.get(where('tag') == 'thawed')
        if tag is None:
            tag = {
                'tag' : 'thawed',
                'parent' : None
            }
        #tag.update(self.checksum)
        return tag

    @property
    def checksum(self):
        """
        Calculates the checksum of all the files in the freezable 
        objects database directory   
        """
        checksums = {
            'slug': hashlib.sha256(
                '{self.name}.{self.dtype}'.encode('utf-8')
            ).hexdigest(),
            'files': {},
        }
        def file_hash(filepath):
            running_hash = hashlib.sha256()
            with open(filepath, 'rb') as IN:
                while True:
                    # Read file in as little chunks.
                    buf = IN.read(4096)
                    if not buf:
                        break
                    running_hash.update(buf)
            return running_hash.hexdigest()
        # iterate over the direcory and calucalte the hash
        for root, dirs, files in os.walk(self.thawed_dir):
            for file_path in sorted(files):
                # Calculate a relative path to the freezable object
                relative_path = str(Path(root) / file_path).replace(str(self.thawed_dir)+'/','')
                full_file_path = Path(root) / file_path
                # calculate and store the checksums
                checksums['files'][relative_path] = file_hash(full_file_path)
        # calculate the total
        total = hashlib.sha256(checksums['slug'].encode('utf-8'))
        for csum in checksums['files'].values():
            total.update(csum.encode('utf-8'))
        checksums['total'] = total.hexdigest()
        return checksums

    # Class Methods --------------------------------------------------

    def freeze(self, tagname):
        '''
        Freezes tha current working (aka "thawed") dataset and freezes its contents 
        for posterity. 
        '''
        # Create the tag from the current thawed tag
        tag = self.thawed_tag
        tagname = self._validate_tagname(tagname)
        # Make sure the tag doesn't already exist
        if tagname in self.tags:
            raise TagExistsError(f'Tag already exists: {tagname}')
        tag['tag'] = tagname 
        # Update the tag with current file checksums
        tag.update(self.checksum)

        # Check to see what files need to be frozen
        for path,phash in tag['files'].items():
            if not (self.basedir / 'frozen' / phash).exists():
                print(f'Found a new file: {path}')
                shutil.copyfile(
                    self.thawed_dir / path,
                    self.frozen_dir / phash
                )
            else:
                print(f'File already exists: {path}')

        # Add a datetime to the document
        tag['timestamp'] = str(datetime.now())

        # Add the tag to the manifest
        self._manifest.insert(tag)
        # Update the current thawed tag
        self._update_thawed_tag({'parent':tagname})

    def thaw(self, tagname, force=False):
        # Validate tag name
        tagname = self._validate_tagname(tagname)
        # Check that tag exists
        tag_data = self._manifest.get(where('tag') == tagname)
        if tag_data is None:
            raise TagDoesNotExistError(f'{tagname} is not in frozen datasets')
        # Check to see that current thawed dataset doesnt have unsaved work
        current_checksum = self.checksum['total']        
        parent_checksum = self._manifest.get(
            where('tag') == self.thawed_tag['parent']
        )['total']
        if not force and current_checksum != parent_checksum:
            raise UnsavedChangesInThawedError(
                'freeze your current changes or use "force" to dispose '
                'of any unsaved changes in current thawed dataset'
            ) 
        # Thaw it out ----------

        # Remove the current files in the thawed directory 
        shutil.rmtree(self.thawed_dir)
        self.thawed_dir.mkdir(exist_ok=True)

        for path,phash in tag_data['files'].items():
            shutil.copyfile(
                self.frozen_dir / phash,
                self.thawed_dir / path,
            )

        self._update_thawed_tag({'parent':tagname})
            

    # Class internal methods---------------------------------------------

    def _update_thawed_tag(self,doc=None):
        '''
            Updates the tag for "thawed" in the manifest
        '''
        tag = self.thawed_tag
        #tag.update(self.checksum)
        # Update core data
        tag['timestamp'] = str(datetime.now())
        # Update any extra data passed in through doc
        if doc is not None:
            tag.update(doc)
        # Upsert the database
        self._manifest.upsert(
            tag,
            where('tag') == 'thawed'
        )

    # Class static methods---------------------------------------------

    @staticmethod
    def _validate_freezable_name(name):
        '''
        Cannot contain slashes, periods, or colons
        '''
        import re
        if re.search('[./:]+',name) is None:
            return name
        else:
            raise ValueError(f'Invalid Freezable Name: "{name}"')

    @staticmethod
    def _validate_tagname(tagname):
        '''
        '''
        import re
        if re.search('[:]+',tagname) is None:
            return tagname
        else:
            raise ValueError(f'Invalid Tag Name: "{tagname}"')


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
