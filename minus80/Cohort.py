import pandas as pd
import os as os

from minus80 import Accession,Freezable

class Cohort(Freezable):
    '''
        A named set of Accessions
    '''

    def __init__(self,name):
        super().__init__(name)
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
                UNIQUE(AID,key,val)
            );
        ''')

    def _get_AID(self,name):
        '''
            Return a Sample ID (AID) 
        '''
        if isinstance(name,Accession):
            name = name.name
        cur = self._db.cursor()
        try:
            return cur.execute(
                'SELECT AID FROM accessions WHERE name = ?',(name,)
            ).fetchone()[0]
        except TypeError as e:
            pass
        try:
            return cur.execute(
                'SELECT AID FROM aliases WHERE alias = ?',(name,) 
            ).fetchone()[0]
        except TypeError as e:
            raise NameError(f'{item} not in Cohort')

    def add_accession(self,accession):
        '''
            Add a sample to the Database
        '''
        cur = self._db.cursor()
        cur.execute('BEGIN TRANSACTION')
        try:
            # When a name is added, it is automatically assigned an ID 
            cur.execute(''' 
                INSERT OR REPLACE INTO accessions (name) VALUES (?)
            ''',(accession.name,))
            # Fetch that ID
            AID = self._get_AID(accession)
            # Populate the metadata and files tables
            cur.executemany('''
                INSERT OR REPLACE INTO metadata (AID,key,val) VALUES (?,?,?)
            ''',((AID,k,v) for k,v in accession.metadata.items())
            )
            cur.executemany('''
                INSERT OR REPLACE INTO files (AID,path) VALUES (?,?)
            ''',((AID,file) for file in accession.files)
            )
            cur.execute('END TRANSACTION')
        except Exception as e:
            cur.execute('ROLLBACK')
            raise e

    def __delitem__(self,name):
        '''
            Remove a sample by name (or by composition)
        '''
        # First try 
        AID = self._get_AID(name)
        
        self._db.cursor().execute('''
            DELETE FROM accessions WHERE AID = ?;
            DELETE FROM metadata WHERE AID = ?;
            DELETE FROM files WHERE AID = ?;
        ''',(AID,AID,AID))

    def __getitem__(self,name):
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
            k:v for k,v in cur.execute('''
                SELECT key,val FROM metadata WHERE AID = ?;
                ''',(AID,)
            ).fetchall()
        } 
        files = [x[0] for x in cur.execute(''' 
                SELECT path FROM files WHERE AID = ?;
            ''',(AID,)
            ).fetchall()
        ]
        return Accession(name,files=files,**metadata)

    def __len__(self):
        return self._db.cursor().execute('''
            SELECT COUNT(*) FROM accessions;
        ''').fetchone()[0]

    def __iter__(self):
        raise NotImplemented()

    def __contains__(self,item):
        if isinstance(item,Accession):
            name = item.name
        else:
            name = item
        try:
            self._get_AID(name)
        except NameError as e:
            return False
        else:
            return True

