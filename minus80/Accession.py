import os
import getpass
import socket
import urllib
import asyncio
import asyncssh
import os

from contextlib import contextmanager

from .Config import cf

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

    def add_file(self, path, scheme='ssh',
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
        scheme: string (default: ssh)
            Specifies the scheme/protocol for accessing the file.
            Defaults to ssh, also supports s3
        username : string (default: None)
            Defines a username that is authorized to access
            `hostname` using `protocol`. Defaults to None 
            in which case it will be determined by calling
            `getpass.getuser()`.
        hostname : sting (default: None)
            Defines the ostname that the file is accessible
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
        # Convert to absolute path
        if url.path.startswith('./') or url.path.startswith('../'):
            path = os.path.abspath(path)
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
            self.add_file(path)

    def __str__(self):
        return '\n'.join(repr(self).split(','))

    def __repr__(self):  # pragma: no cover
        '''
        String representation of Accession
        '''
        if len(self.metadata) == 0:
            max_key_len = 1
        else:
            max_key_len = max([len(x) for x in self.metadata.keys()]+[0])

        return (
            'Accession(\n'
            f'  {self.name},' + '\n'
            '  Metadata:{\n'
            '  \t'+"\n\t".join(['{0: <{width}}:'.format(k,width=max_key_len)+' {}'.format(v) for k,v in self.metadata.items()])+
            '\n  },\n'
            '  files=[' + '\n\t'.join(self.files)+']\n)\n'
        )

    @staticmethod
    async def _check_file(url): #pragma: no cover
        '''
        asyncronously checks a URL
        based in its scheme
        '''
        # Parse the URL and connect
        url = urllib.parse.urlparse(url)
        async with asyncssh.connect(
                url.hostname,
                username=url.username) as conn:
            return await conn.run(
                f'[[ -f {url.path} ]] && echo -n "Y" || echo -n "N"'
            )


    def _check_files(self): #pragma: no cover
        '''
        Check to see if files attached to an accession are 
        accessible through ssh

        Parameters
        ----------
        None

        Returns
        -------
        Returns True if all files are accessible, otherwise 
        returns a list of files that were unreachable.
        '''
        # Set us up the loop
        tasks = [] 
        loop = asyncio.get_event_loop() 
        # loop through the files and create tasks
        files = list(self.files)
        for url in files:
            tasks.append(self._check_file(url))
        tasks = asyncio.gather(*tasks)
        loop.run_until_complete(tasks)
        unreachable = [i for i,r in enumerate(tasks.result()) if r.stdout != 'Y']
        if len(unreachable) == 0:
            return True
        else:
            return [files[x] for x in unreachable]


