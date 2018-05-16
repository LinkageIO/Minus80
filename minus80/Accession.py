import os
import getpass
import socket
import urllib

class Accession(object):
    '''
    From google: Definition (noun): a new item added to an existing collection
    of books, paintings, or artifacts.

    An Accession is an item that exists in an experimental collection.

    Most of the time an accession is interoperable with a *sample*. However,
    the term sample can become confusing when an experiment has multiple
    samplings from the same sample, e.g. timecourse or different tissues.
    '''

    def __init__(self, name, files=None, **kwargs):
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
        if files is not None:
            self.files = set(files)
        else:
            self.files = set()
        self.metadata = kwargs

    def __getitem__(self, key):
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

    def __setitem__(self, key, val):
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

    def add_file(self, path, skip_check=False, scheme='ssh',
                 username=None, hostname=None):
        '''
        Add a file that is associated with the accession.
        This method will attempt to determine where the file
        is actually stored based on its path. Currently it 
        supports three different protocols: local, ssh and
        s3. A local file will looks something like:
        `/tmp/file1.fastq`.  

        Parameters
        ----------
        path/URL: string
            The path/URL the the file. The string is parsed
            for default information (e.g. 
        skip_check : bool
            If true, the method will not test if the file
            exists
        scheme: string (default: ssh)
            Specifies the scheme/protocol for accessing the file.
            Defaults to ssh, also supports s3
        username : string (default: None)
            Defines a username that is authorized to access
            `hostname` using `protocol`. Defaults to None 
            in which case it will be determined by calling
            `getpass.getuser()`.
        hostname : sting (default: None)
            Defines the hostname that the file is accessible
            through. Defaults to None, where the hostname
            will be determined 
        port: int (default: 22)
            Port to access the file through. Defaults to 22,
            which is for ssh.

        NOTE: any keyword arguments passed in will override
              the values parsed out of the path. 

        Returns
        -------
        None
        '''
        url = urllib.parse.urlparse(path)
        # Override parsed url values with keywords
        if scheme is not None:
            url = url._replace(scheme=scheme)
        # check if URL parameters were provided via path
        if url.netloc == '':
            if username is None:
                username = getpass.getuser()
            if hostname is None:
                hostname = socket.gethostname()
            netloc = f'{username}@{hostname}'
            url = url._replace(netloc=netloc)
        if not skip_check:
            # Get absolute path
            path = os.path.abspath(path)
            if not os.path.exists(path):
                raise ValueError(f'{path} does not exist')
        url = urllib.parse.urlunparse(url)
        self.files.add(url)

    def add_files(self, paths, skip_test=False):
        '''
        Add multiple paths that are associated with an accession

        Parameters
        ----------
        paths : iterable of strings
            The paths the the files
        skip_test : bool
            If true, the method will not test if the file
            exists

        Returns
        -------
        None
        '''
        for path in paths:
            self.add_file(path, skip_test=skip_test)

    def __repr__(self):  # pragma: no cover
        '''
        String representation of Accession
        '''
        return f'Accession({self.name}, files={self.files}, {self.metadata})'



