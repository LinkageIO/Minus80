
class Accession(object):
    def __init__(self,name,**kwargs):
        self.name = name
        if 'files' in kwargs:
            self.files = kwargs['files']
            del kwargs['files']
        else:
            self.files = []
        self.metadata = kwargs

    def __getitem__(self,key):
        return self.metadata[key]

    def add_file(self,path):
        self.files.append(path)

    def add_files(self,paths):
        for path in paths:
            self.add_file(path)
