
class Accession(object):
    '''
    From google: Definition (noun): a new item added to an existing collection of
    books, paintings, or artifacts.  

    An Accession is an item that exists in an experimental collection. 
    
    Most of the time an accession is interoperable with a *sample*. However,
    the term sample can become confusing when an experiment has multiple
    samplings from the same sample, e.g. timecourse or different tissues. 
    '''

    def __init__(self,name,files=None,**kwargs):
        '''
        Create a new accession.

        Parameters
        ----------
        name : str
            The name of the accession
        files : iterable of str
            Files associated with the accession
        **kwargs : keyword arguments
            Any number of key=value arguments that
            contain metadata.

        Returns
        -------
        An accession object
        '''
        self.name = name
        if files != None:
            self.files = set(files)
        else:
            self.files = set()
        self.metadata = kwargs

    def __getitem__(self,key):
        '''
        Retrieve metadata about an accession. 

        Parameters
        ----------
        key : str

        Returns
        -------
        Value from the accession metadata corresponding
        to the key.
        '''
        return self.metadata[key]

    def __setitem__(self,key,val):
        '''
        Set metadata about an accession

        Parameters
        ----------
        key : str
            The metadata name
        val : str
            The value of the metadata
        '''
        self.metadata[key] = val

    def add_file(self,path):
        '''
        Add a file that is associated with the accession.

        Parameters
        ----------
        path: string

        Returns
        -------
        None
        '''
        self.files.add(path)

    def add_files(self,paths):
        '''
        Add multiple paths that are associated with an accession

        Parameters
        ----------
        paths : iterable of strings

        Returns
        -------
        None
        '''
        for path in paths:
            self.add_file(path)

    def __repr__(self): #pragma: no cover
        '''
        String representation of Accession
        '''
        return f'Accession({self.name},files={self.files},{self.metadata})'
