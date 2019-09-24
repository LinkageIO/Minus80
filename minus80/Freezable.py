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
from minus80 import SLUG_VERSION


from .Config import cf
from .Tools import (guess_type,
                    validate_tagname,
                    validate_freezable_name)

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

    def __freezable_delete_cleanup__(self):
        """
            This method gets called when intefaces delete datasets.
            It is up to the interface to call this method after all
            the internal minus80 files are deleted. E.g. deleting
            any files outside of the base minus80 directory
        """
        pass


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
        self.name = validate_freezable_name(name)
        # Set the m80 dtype
        self.dtype = validate_freezable_name(dtype)

        # Default to the basedir in the config file
        if basedir is None:
            basedir = Path(cf.options.basedir).expanduser() / 'datasets' / SLUG_VERSION
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
        return tag

    @property
    def parent_tag(self):
        tag = self.thawed_tag
        if tag['parent'] is None:
            raise TagDoesNotExistError('parent tag is None') 
        parent = self._manifest.get(where('tag') == tag['parent']) 
        if parent is None:
            raise TagDoesNotExistError('parent tag "{tag["parent"]} is not in manifest"')
        return parent

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
            'files': {}
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
        for root,dirs,files in os.walk(self.thawed_dir):
            for file_path in sorted(files):
                full_path = str(Path(root)/file_path)
                # Calculate a relative path to the freezable object
                rel_path = full_path.replace(str(self.thawed_dir)+'/','')
                # calculate and store the checksums
                phash = file_hash(full_path)
                checksums['files'][rel_path] = phash
        # calculate the total
        total = hashlib.sha256(checksums['slug'].encode('utf-8'))
        # Iterate over filenames AND hashes and update checksum
        for filename,csum in checksums['files'].items():
            total.update(filename.encode('utf-8'))
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
        tagname = validate_tagname(tagname)
        # Make sure the tag doesn't already exist
        if tagname in self.tags:
            raise TagExistsError(f'Tag already exists: {tagname}')
        tag['tag'] = tagname 
        # Update the tag with current file checksums
        tag.update(self.checksum)

        # Check to see what files need to be frozen
        for relative_path,phash in tag['files'].items():
            if not (self.basedir / 'frozen' / phash).exists():
                log.info(f'Found a new file: {relative_path}')
                shutil.copyfile(
                    self.thawed_dir / relative_path,
                    self.frozen_dir / phash
                )
            else:
                log.info(f'Using cached file: {relative_path}')

        # Add a datetime to the document
        tag['timestamp'] = datetime.now().timestamp()

        # Add the tag to the manifest
        self._manifest.insert(tag)
        # Update the current thawed tag
        self._update_thawed_tag({'parent':tagname})

    def thaw(self, tagname, force=False):
        # Validate tag name
        tagname = validate_tagname(tagname)
        # Check that tag exists
        tag_data = self._manifest.get(where('tag') == tagname)
        if tag_data is None:
            raise TagDoesNotExistError(f'{tagname} is not in frozen datasets')
        # Check to see that current thawed dataset doesnt have unsaved work
        current_checksum = self.checksum['total']        
        parent = self._manifest.get(
            where('tag') == self.thawed_tag['parent']
        )
        if not force and current_checksum != parent['total']:
            new = []
            changed = []
            deleted = []
            for f,c in self.checksum['files'].items():
                if f not in parent['files']:
                    new.append(f)
                elif c != parent['files'][f]:
                    changed.append(f)
            for f,c in parent['files'].items():
                if f not in self.checksum['files']:
                    deleted.append(f)
            # poppulate a list of files that changed
            raise UnsavedChangesInThawedError(
                'freeze your current changes or use "force" to dispose '
                'of any unsaved changes in current thawed dataset',
                new=new,changed=changed,deleted=deleted
            ) 
        # Thaw it out ----------

        # Remove the current files in the thawed directory 
        for root, dirs, files in os.walk(self.thawed_dir):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

        for rel_path,phash in tag_data['files'].items():
            (self.thawed_dir/rel_path).parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(
                self.frozen_dir / phash,
                self.thawed_dir / rel_path,
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
        tag['timestamp'] = datetime.now().timestamp()
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
