import os
import pathlib

from tinydb import where
from minus80 import Freezable

class Project(Freezable):

    def __init__(self, name, basedir=None):
        super().__init__(name,basedir=basedir)

        # do some checks
        if self.dir is not None:
            if not self.symdir.exists():
                self.symdir.symlink_to(self.dir)

    @property
    def symdir(self):
        return self.m80.thawed_dir / 'symdir'

    @property   
    def dir(self):
        doc = self.m80.doc.get(where('dir').exists())
        if doc is None:
            return doc
        else:
            return pathlib.Path(doc['dir'])

    @dir.setter
    def dir(self,dir):
        if self.dir is not None:
            raise ValueError('Cannot change project dir')
        dir = pathlib.Path(dir).resolve()
        if not dir.exists():
            raise ValueError(f'directory "{dir}" does not exist')
        self.m80.doc.upsert(
            {'dir':str(dir)},
            where('dir').exists()
        )
        # symlink to project dir
        (self.m80.thawed_dir/'symdir').symlink_to(dir)
        
        


