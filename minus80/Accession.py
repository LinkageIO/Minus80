import yaml
import getpass
import socket
import urllib


class Accession(object):
    """
    From google: Definition (noun): a new item added to an existing collection
    of books, paintings, or artifacts.

    An Accession is an item that exists in an experimental collection.

    Most of the time an accession is interoperable with a *sample*. However,
    the term sample can become confusing when an experiment has multiple
    samplings from the same sample, e.g. timecourse or different tissues.
    """

    def __init__(self, name, files=None, **kwargs):
        """
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
        """
        self.name = name
        if files is not None:
            self.files = set(files)
        else:
            self.files = set()
        self.metadata = kwargs

    def __getitem__(self, key):
        """
        Retrieve metadata about an accession.

        Parameters
        ----------
        key : str

        Returns
        -------
        Value from the accession metadata corresponding
        to the key.
        """
        return self.metadata[key]

    def __setitem__(self, key, val):
        """
        Set metadata about an accession

        Parameters
        ----------
        key : str
            The metadata name
        val : str
            The value of the metadata
        """
        self.metadata[key] = val

    def add_file(self, path, scheme="ssh", username=None, hostname=None):
        """
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
        """
        url = urllib.parse.urlparse(path)
        # Override parsed url values with keywords
        if scheme is not None:
            url = url._replace(scheme=scheme)
        # check if URL parameters were provided via path
        if url.netloc == "":
            if username is None:
                username = getpass.getuser()
            if hostname is None:
                hostname = socket.gethostname()
            netloc = f"{username}@{hostname}"
            url = url._replace(netloc=netloc)
        # Convert to absolute path
        # if not url.path.startswith('/'): #url.path.startswith('./') or url.path.startswith('../'):
        # url._replace(path=os.path.abspath(path))
        url = urllib.parse.urlunparse(url)
        self.files.add(url)

    def add_files(self, paths, skip_test=False):
        """
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
        """
        for path in paths:
            self.add_file(path)

    def __str__(self):
        return "\n".join(repr(self).split(","))

    def __repr__(self):  # pragma: no cover
        """
        String representation of Accession
        """
        if len(self.metadata) == 0:
            max_key_len = 1
        else:
            max_key_len = max([len(x) for x in self.metadata.keys()] + [0])

        return (
            "Accession(\n"
            f"  {self.name}," + "\n"
            "  Metadata:{\n"
            "  \t"
            + "\n\t".join(
                [
                    "{0: <{width}}:".format(k, width=max_key_len) + " {}".format(v)
                    for k, v in self.metadata.items()
                ]
            )
            + "\n  },\n"
            "  files=[" + "\n\t".join(self.files) + "]\n)\n"
        )

    @classmethod
    def from_yaml(cls, yaml_file):
        """
        Create Accessions from a YAML file
        e.g.
        10F:
            files:
                - ssh://user@hostname.edu/path/to/file/10_F_S66_R1_001.fastq.gz
                - ssh://user@hostname.edu/path/to/file/10_F_S66_R2_001.fastq.gz
            metadata:
                seqtype: novaseq
        10M:
            files:
                - ssh://user@hostname.edu/path/to/file/10_M_S10_R1_001.fastq.gz
                - ssh://user@hostname.edu/path/to/file/10_M_S10_R2_001.fastq.gz
                - ssh://user@hostname.edu/path/to/file/10_M_S40_R1_001.fastq.gz
                - ssh://user@hostname.edu/path/to/file/10_M_S40_R2_001.fastq.gz
        """
        accessions = []
        with open(yaml_file, "r") as IN:
            for name, v in yaml.safe_load(IN).items():
                files = v["files"] if "files" in v else []
                metadata = v["metadata"] if "metadata" in v else {}
                a = cls(name, files=files, **metadata)
                accessions.append(a)
        return accessions
