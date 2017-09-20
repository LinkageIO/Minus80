import pandas as pd
import os as os

from minus80 import Accession,Freezable

class Accessions(Freezable):

    '''
        A names set of Accessions
    '''

    def __init__(self,name):
        super().__init__('Samples')
        self.name = name
        self._initialize_tables()


    def _initialize_tables(self):
        cur = self._db.cursor()
        cur.execute(''' 
            CREATE TABLE IF NOT EXISTS samples (
                rowid INTEGER PRIMARY KEY,
                name NOT NULL UNIQUE
            );        
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS files (
                rowid INTEGER PRIMARY KEY,
                system TEXT,
                path TEXT ID NOT NULL UNIQUE
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                sample_rowid NOT NULL,
                key TEXT NOL NULL,
                val TEXT NOT NULL,
                FOREIGN KEY(sample_rowid) REFERENCES samples(rowid)
                PRIMARY KEY(sample_rowid,key,val)
            );
        ''')

    def add_sample(self,sample):
        '''
            Add a sample to the Database
        '''
        cur = self._db.cursor()
        cur.execute('BEGIN TRANSACTION')
        try:
            cur.execute(''' 
                INSERT INTO Samples (ID) VALUES (?)
            ''',(sample.id,))
            cur.executemany('''
                INSERT INTO Metadata (ID,key,val) VALUES (?,?,?)
            ''',((sample.id,k,v) for k,v in sample.metadata.items())
            )
            cur.executemany('''
                INSERT INTO Files (ID,system,path) VALUES (?,?,?)
            ''',((sample.id,'',file) for file in sample.files)
            )
            cur.execute('END TRANSACTION')
        except Exception as e:
            cur.execute('ROLLBACK')
            raise e

    def update_sample(self,sample):
        '''
        '''
        cur = self._db.cursor()
        cur.execute('BEGIN TRANSACTION')
        try:
            cur.execute(''' 
                INSERT OR REPLACE INTO Samples (ID) VALUES (?)
            ''',(sample.id,))
            cur.executemany('''
                INSERT OR REPLACE INTO Metadata (ID,key,val) VALUES (?,?,?)
            ''',((sample.id,k,v) for k,v in sample.metadata.items())
            )
            cur.executemany('''
                INSERT OR REPLACE INTO Files (ID,system,path) VALUES (?,?,?)
            ''',((sample.id,'',file) for file in sample.files)
            )
            cur.execute('END TRANSACTION')
        except Exception as e:
            cur.execute('ROLLBACK')
            raise e

        

    def del_sample(self,id):
        '''
            Remove a sample by id
        '''
        self._db.cursor().execute('''
            DELETE FROM Samples WHERE ID = ?;
            DELETE FROM Metadata WHERE ID = ?;
            DELETE FROM Files WHERE ID = ?;
        ''',(id,id,id))

    def get_sample(self,id):
        '''
            Get a sample from the database from ID.
        '''
        if id not in self:
            raise ValueError('{} not in database.'.format(id))
        metadata = {
            k:v for k,v in self._db.cursor().execute('''
                SELECT key,val FROM Metadata WHERE ID = ?;
                ''',(id,)
            ).fetchall()
            
        } 
        files = [x for x, in self._db.cursor().execute(''' 
                SELECT path FROM Files WHERE ID = ?;
            ''',(id,)
            ).fetchall()
        ]
        x = Sample(id,**metadata)
        [x.add_file(f) for f in files]
        return x

    def __len__(self):
        return self._db.cursor().execute('''
            SELECT COUNT(*) FROM Samples;
        ''').fetchone()[0]

    def __getitem__(self,id):
        return self.get_sample(id)

    def __iter__(self):
        raise NotImplemented()

    def __contains__(self,item):
        if isinstance(item,str):
            (num_rows,) = self._db.cursor().execute('''
                SELECT COUNT(*) FROM Samples WHERE name = ?
            ''',(item,)).fetchone()    
            if num_rows > 0:
                return True
        return False
