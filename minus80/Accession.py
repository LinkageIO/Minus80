
class Accession(object):
    '''
        An Accession is an item that exists in an 
        experimental collection. It can be a sample
        or a tissue. Accessions have a set of files 
        associated with them as well as a 
    '''

    def __init__(self,name,files=None,**kwargs):
        self.name = name
        if files != None:
            self.files = files
        else:
            self.files = set()
        self.metadata = kwargs

    def __getitem__(self,key):
        return self.metadata[key]

    def add_file(self,path):
        self.files.add(path)

    def add_files(self,paths):
        for path in paths:
            self.add_file(path)

    def __repr__(self):
        return f'Accession({self.name},files={self.files},{self.metadata})'
