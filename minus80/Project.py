from pathlib import Path
from tinydb import where
from minus80 import Freezable

class Project(Freezable):

    def __init__(self, name, basedir=None):
        super().__init__(name,basedir=basedir)
        # create the internal data dir 
        (self.m80.thawed_dir / 'data').mkdir(exist_ok=True)

    def create_link(self,path):
        path = Path(path)
        if path.exists():
            raise ValueError(f'"{path}" already exists.')
        path.symlink_to(self.m80.thawed_dir/'data')
