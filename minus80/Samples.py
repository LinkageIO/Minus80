import pandas as pd
import os as os

from minus80 import Sample,Freezable

class Samples(Freezable):

    '''
        Samples is a Minus80 class that stores and accesses
        Samples from the Minus80 Freezer. 
    '''

    def __init__(self):
        super().__init__('Samples')
       # basedir = os.path.expanduser('~/.M80') 

        #os.makedirs(os.path.join(basedir,'databases'),exist_ok=True)
        #self._db = lite.Connection(
        #    os.path.join(basedir,'databases','M80.sqlite')
        #) 

        #self._db.cursor().execute(''' 
        #    CREATE TABLE IF NOT EXISTS Samples (
        #        ID NOT NULL,
        #        PRIMARY KEY(ID)
        #    );        
        #    CREATE TABLE IF NOT EXISTS Files (
        #        ID NOT NULL,
        #        system TEXT,
        #        path TEXT,
        #        PRIMARY KEY(ID, system, path)
        #    );
        #    CREATE TABLE IF NOT EXISTS Metadata (
        #        ID NOT NULL,
        #        key NOL NULL,
        #        val NOT NULL,
        #        PRIMARY KEY(ID,key,val)
        #    );
        #''')

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

    def update_sample(self,sample,drop=False):
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
                SELECT COUNT(*) FROM Samples WHERE ID = ?
            ''',(item,)).fetchone()    
            if num_rows > 0:
                return True
        return False


    def import_sample_files_from_DataFrame(
                        self,df,id_col='ID',system_col='System',
                        path_col='Path'):
        '''
            Imports Sample file paths from a dataframe. Expects a DataFrame
            that has columns for (ID,system, and path).
        '''
        for id,df in df.groupby(id_col):
            x = Sample(id)
            x.add_files(df[path_col].values)
            self.add_sample(x)
