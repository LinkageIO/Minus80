from functools import lru_cache

from minus80 import Accession, Freezable


class Cohort(Freezable):
    '''
        A Cohort is a named set of accessions. Once cohorts are
        created, they are persistant as they are stored in the
        disk by minus80.
    '''

    def __init__(self, name, parent=None):
        super().__init__(name,parent=parent)
        self.name = name
        self._initialize_tables()

    def _initialize_tables(self):
        cur = self._db.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS accessions (
                AID INTEGER PRIMARY KEY AUTOINCREMENT,
                name NOT NULL UNIQUE
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS aliases (
                alias TEXT,
                AID INTEGER,
                FOREIGN KEY(AID) REFERENCES accessions(AID)
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS files (
                AID INTEGER,
                path TEXT ID NOT NULL UNIQUE,
                FOREIGN KEY(AID) REFERENCES accessions(AID)
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                AID NOT NULL,
                key TEXT NOL NULL,
                val TEXT NOT NULL,
                FOREIGN KEY(AID) REFERENCES accessions(AID)
                UNIQUE(AID, key, val)
            );
        ''')

    #------------------------------------------------------#
    #               Magic Methods                          #
    #------------------------------------------------------#

    def __repr__(self):
        return f'Cohort("{self.name}") -- contains {len(self)}'

    def __delitem__(self, name):
        '''
            Remove a sample by name (or by composition)
        '''
        # First try
        AID = self._get_AID(name)

        self._db.cursor().execute('''
            DELETE FROM accessions WHERE AID = ?;
            DELETE FROM metadata WHERE AID = ?;
            DELETE FROM files WHERE AID = ?;
        ''', (AID, AID, AID))

    def __getitem__(self, name):
        '''
            Get an accession from the database the pythonic way.

            Paremeters
            ----------
            name : object
                Can be a string, i.e. the name or alias of an Accession,
                it can be an Actual Accession OR the AID which
                is an internal ID for accession
        '''
        AID = self._get_AID(name)
        cur = self._db.cursor()
        metadata = {
            k: v for k, v in cur.execute('''
                SELECT key, val FROM metadata WHERE AID = ?;
                ''', (AID, )
            ).fetchall()
        }
        metadata['AID'] = AID
        files = [x[0] for x in cur.execute('''
                SELECT path FROM files WHERE AID = ?;
            ''', (AID, )
            ).fetchall()
        ]
        return Accession(name, files=files, **metadata)

    def __len__(self):
        return self._db.cursor().execute('''
            SELECT COUNT(*) FROM accessions;
        ''').fetchone()[0]

    def __iter__(self):
        for name in (x[0] for x in self._db.cursor().execute('''
                SELECT name FROM accessions
                ''').fetchall()):
            yield self[name]

    def __contains__(self, item):
        if isinstance(item, Accession):
            name = item.name
        else:
            name = item
        try:
            self._get_AID(name)
        except NameError:
            return False
        else:
            return True

    @property
    def AID_mapping(self):
        return {
            x.name: x['AID']
            for x in self
        }

    #------------------------------------------------------#
    #               Internal Methods                       #
    #------------------------------------------------------#


    @lru_cache(maxsize=2048)
    def _get_AID(self, name):
        '''
            Return a Sample ID (AID)
        '''
        if isinstance(name, Accession):
            name = name.name
        cur = self._db.cursor()
        try:
            return cur.execute(
                'SELECT AID FROM accessions WHERE name = ?', (name, )
            ).fetchone()[0]
        except TypeError:
            pass
        try:
            return cur.execute(
                'SELECT AID FROM aliases WHERE alias = ?', (name, )
            ).fetchone()[0]
        except TypeError:
            raise NameError(f'{name} not in Cohort')


    #------------------------------------------------------#
    #               Class Methods                          #
    #------------------------------------------------------#

    @classmethod
    def from_yaml(cls, name, yaml_file): #pragma: no cover
        '''
        Create a Cohort from a YAML file. Note: this yaml file
        must be created from

        Parameters
        ----------
        name : str
            The name of the Cohort
        yaml_file : pathlike
            The path to the YAML file that contains the
            Accessions

        Returns
        -------
        A Cohort object
        '''
        import yaml
        self = cls(name)
        accessions = yaml.load(open(yaml_file, 'r'))
        self.add_accessions(accessions)
        return self

    @classmethod
    def from_accessions(cls, name, accessions):
        '''
        Create a Cohort from an iterable of Accessions.

        Parameters
        ----------
        name : str
            The name of the Cohort
        accessions : iterable of Accessions
            The accessions that will be frozen in the cohort
            under the given name

        Returns
        -------
        A Cohort object

        '''
        self = cls(name)
        self.add_accessions(accessions)
        return self

    def random_accession(self):
        '''
            Returns a random accession from the Cohort

            Parameters
            ----------
            None

            Returns
            -------
            Accession
                An Accession object
        '''
        name = self._db.cursor().execute('''
            SELECT name from accessions ORDER BY RANDOM() LIMIT 1;
        ''').fetchone()[0]
        return self[name]

    def random_accessions(self, n=1, replace=False):
        '''
            Returns a list of random accessions from the Cohort, either
            with or without replacement.

            Parameters
            ----------
            n : int
                The number of random accessions to retrieve
            replace: bool
                If false, randomimzation does not include replacement
        '''
        if replace is False:
            if n > len(self):
                raise ValueError(
                        f'Only {len(self)} accessions in cohort. Cannot'
                        ' get {n} samples. See replace parameter in help.'
                )
            return (
                self[name] for (name, ) in self._db.cursor().execute('''
                    SELECT name from accessions ORDER BY RANDOM() LIMIT ?;
                ''', (n, ))
            )
        else:
            return (self.random_accession() for _ in range(n))

    def add_accessions(self, accessions):
        '''
            Add multiple Accessions at once
        '''
        with self.bulk_transaction() as cur:
            # When a name is added, it is automatically assigned an ID
            cur.executemany('''
                INSERT OR IGNORE INTO accessions (name) VALUES (?)
            ''', [(x.name, ) for x in accessions])
            # Fetch that ID
            AID_map = self.AID_mapping
            # Populate the metadata and files tables
            cur.executemany('''
                INSERT OR REPLACE INTO metadata (AID, key, val)
                VALUES (?, ?, ?)
            ''', (
                    (AID_map[accession.name], k, v)
                    for accession in accessions
                    for k, v in accession.metadata.items()
                )
            )
            cur.executemany('''
                INSERT OR REPLACE INTO files (AID, path) VALUES (?, ?)
            ''', (
                    (AID_map[accession.name], file)
                    for accession in accessions
                    for file in accession.files
                )
            )
        return [self[x] for x in accessions]

    def add_accession(self, accession):
        '''
            Add a sample to the Database
        '''
        with self.bulk_transaction() as cur:
            # When a name is added, it is automatically assigned an ID
            cur.execute('''
                INSERT OR IGNORE INTO accessions (name) VALUES (?)
            ''', (accession.name, ))
            # Fetch that ID
            AID = self._get_AID(accession)
            # Populate the metadata and files tables
            cur.executemany('''
                INSERT OR REPLACE INTO metadata (AID, key, val)
                VALUES (?, ?, ?)
            ''', ((AID, k, v) for k, v in accession.metadata.items())
            )
            cur.executemany('''
                INSERT OR REPLACE INTO files (AID, path) VALUES (?, ?)
            ''', ((AID, file) for file in accession.files)
            )
        return self[accession]



