

class Sample(object):
    def __init__(self,id,**kwargs):
        self.id = id
        self.metadata = kwargs
        self.files = []


    def __getitem__(self,key):
        return self.metadata[key]

    def add_file(self,path):
        self.files.append(path)

    def add_files(self,paths):
        for path in paths:
            self.add_file(path)


