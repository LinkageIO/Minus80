from functools import lru_cache
from collections import Counter,defaultdict,namedtuple

from minus80 import Accession, Freezable
from difflib import SequenceMatcher
from itertools import chain
from tqdm import tqdm
from pprint import pprint

import numpy as np

import numbers
import click
import math
import warnings
import logging
import asyncssh
import urllib
import asyncio
import os
import backoff
import getpass
import socket
import inspect

__all__ = ['Cohort']

def invalidates_AID_cache(fn):
    from functools import wraps
    @wraps(fn)
    def wrapped(self,*args,**kwargs):
        fn(self,*args,**kwargs) 
        self._get_AID.cache_clear()
    return wrapped


class Cohort(Freezable):
    '''
        A Cohort is a named set of accessions. Once cohorts are
        created, they are persistant as they are stored in the
        disk by minus80.
    '''

    # This is a named tuple that will be populated by self.get_fileinfo
    fileinfo = None

    def __init__(self, name, parent=None):
        super().__init__(name,parent=parent)
        self.name = name
        self._initialize_tables()
        self.log = logging.getLogger(f'minus80.Cohort.{name}')
        logging.basicConfig()
        self.log.setLevel(logging.INFO)

    #------------------------------------------------------#
    #                 Properties                           #
    #------------------------------------------------------#

    @property
    def columns(self):
        '''
            Return a list of all the available metadata stored
            for available Accessions
        '''
        return [ x[0] for x in self._db.cursor().execute('''
            SELECT DISTINCT(key) FROM metadata;
        ''').fetchall() ]

    @property
    def names(self):
        '''
            Return a list of all available names and aliases
        '''
        names = [x[0] for x in self._db.cursor().execute(
            'SELECT name FROM accessions'
        )]
        aliases = [x[0] for x in self._db.cursor().execute(
            'SELECT alias FROM aliases'   
        )]
        return names + aliases

    @property
    def files(self):
        return [x[0] for x in self._db.cursor().execute('''
            SELECT url FROM raw_files WHERE ignore != 1
        ''').fetchall() ]

    @property
    def raw_files(self):
        return [x[0] for x in self._db.cursor().execute('''
            SELECT url FROM raw_files
        ''').fetchall() ]

    @property
    def unassigned_files(self):
        assigned = set([x[0] for x in 
            self._db.cursor().execute('''
                SELECT DISTINCT(url) 
                FROM files
            ''').fetchall()
        ])
        return [x for x in self.files if x not in assigned]

    @property
    def ignored_files(self):
        ignored = [x[0] for x in 
            self._db.cursor().execute('''
                SELECT DISTINCT(url) 
                FROM raw_files WHERE ignore != 0
            ''').fetchall()
        ]
        return ignored

    @property
    def _AID_mapping(self):
        return {
            x.name: x['AID']
            for x in self
        }

    @property
    def num_files(self):
        return len(self.files)

    def as_DataFrame(self):
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError('Pandas must be installed to use this feature')
        long_form = pd.DataFrame(self._db.cursor().execute('''
            SELECT name,key,val FROM accessions acc 
            JOIN metadata met on acc.AID = met.AID;
        ''').fetchall(),columns=['name','key','val'])
        return long_form.pivot(index='name',columns='key',values='val')

    #------------------------------------------------------#
    #                   Methods                            #
    #------------------------------------------------------#

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

    def get_fileinfo(self, url):
        '''
        Get file info from a url.
        
        Parameters
        ----------
        url : str
            A URL to get the info for.

        Returns
        -------
        A named tuple contianing the url info.

        '''
        if self.fileinfo is None:
            # create a named tuple
            cols = [x[0] for x in self._db.cursor().execute('SELECT * FROM raw_files').description]
            self.fileinfo = namedtuple('fileinfo',cols)

        info = self._db.cursor().execute('''
            SELECT *
            FROM raw_files WHERE url = ?
        ''',(url,)).fetchone()
        return self.fileinfo(*info)

    def update_fileinfo(self,info):
        '''
            Update the fileinfo 
        '''
        info_list = []
        if isinstance(info,self.fileinfo):
            info = [info]
        for x in info:
            url = x.url
            info_list.append(
                (x.ignore, x.canonical_path, x.md5, x.size, x.url)    
            )		
        # Update the info 
        self._db.cursor().executemany('''
            UPDATE raw_files SET
                ignore = ?,
                canonical_path = ?,
                md5 = ?,
                size = ?
            WHERE
                url = ?
        ''',info_list)

    def add_accessions(self, accessions):
        '''
            Add multiple Accessions at once
        '''
        with self._bulk_transaction() as cur:
            # When a name is added, it is automatically assigned an ID
            cur.executemany('''
                INSERT OR IGNORE INTO accessions (name) VALUES (?)
            ''', [(x.name, ) for x in accessions])
            # Fetch that ID
            AID_map = self._AID_mapping
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
                INSERT OR REPLACE INTO files (AID, url) VALUES (?, ?)
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
        with self._bulk_transaction() as cur:
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
                INSERT OR IGNORE INTO files (AID,url) VALUES (?,?)
            ''', ((AID,file) for file in accession.files)
            )
        return self[accession]


    def add_accessions_from_data_frame(self,df,name_col):
        '''
            Add accessions from data frame. This assumes
            each row is an Accession and that the properties
            of the accession are stored in the columns. 

            Parameters
            ----------
            df : pandas.DataFrame
                The pandas data frame containing one accession
                per row
            name_col : string
                The column containing the accession names

            Example
            -------

            >>> df = pd.DataFrame( 
                  [['S1'    23    'O'],
                   ['S2'    30    'O+']],
                   columns =  ['Name','Age','Type']
                )
            >>> x = m80.add_accessions_from_data_frame(df,'Name')

            Would yield two Accessions: S1 and S2 with Age and Type
            properties.

        '''
        if name_col not in df.columns:
            raise ValueError(f'{name_col}S not a valid column name')
        # filter out rows with NaN name_col values
        # The tilda operator is a boolean inversion
        df = df.loc[~df[name_col].isnull(),:]
        accessions = []
        # Iterate over the rows and create and accessions from each one
        for i,row in df.iterrows():
            d = dict(row)  
            name = d[name_col]
            del d[name_col]
            # Get rid of missing data
            for k,v in list(d.items()):
                if isinstance(v,numbers.Number) and math.isnan(v): 
                    del d[k]
                else:
                    d[k] = str(v)
            accessions.append(Accession(name, files=None, **d))
        self.add_accessions(accessions)

    
    def alias_column(self, colname,min_alias_length=3):
        '''
            Assign an accession column as aliases
        '''
        cur_names = set(self.names)
        with self._bulk_transaction() as cur: 
            alias_dict = {a:aid for a,aid in cur.execute('''
                SELECT val,AID FROM metadata 
                WHERE key = ?
            ''',(colname,)).fetchall()}
            # We only want unique aliases
            unique_aliases = []
            alias_counts = Counter([x for x in alias_dict.keys()]) 
            for alias,count in alias_counts.items():
                if count > 1 or alias in cur_names:
                    self.log.warning(f"Cannot use {alias} as it is not unique")
                elif len(alias) < min_alias_length:
                    self.log.warning(f"Skipping {alias} as it is too short (<{min_alias_length})")
                else:
                    unique_aliases.append((alias,alias_dict[alias]))

            cur.executemany('''
                INSERT INTO aliases (alias,AID) VALUES (?,?)      
            ''',unique_aliases)

    @invalidates_AID_cache
    def drop_aliases(self):
        '''
            Clear the aliases from the database
        '''
        self._db.cursor().execute('DELETE FROM aliases')

    def drop_accessions(self):
       with self._bulk_transaction() as cur:
            cur.execute('''
                DELETE FROM accessions;
                DELETE FROM aliases;
                DELETE FROM metadata;
                DELETE FROM aid_files;
            ''')

    def assimilate_files(self,files,best_only=True):
        '''
            Take a list of files and assign them to Accessions
        '''
        results = defaultdict(set)
        for f in files:
            matches = self.search_accessions(os.path.basename(f))
            if len(matches) == 0:
                results['unmatched'].add(f)
            elif best_only:
                results[matches[0]].add(f)
            else:
                for m in matches:
                    results[m].add(f)
        return results

    async def _info_worker(self, url_queue,pbar=None):
        '''
        Given a queue of URLs, this worker will calculate the md5 checksums
        and commit them to the 'raw_files' database table
        '''
        def backoff_hdlr(details):
            print("Backing off {wait:0.1f} seconds afters {tries} tries "
                "calling function {target} with args {args} and kwargs "
                "{kwargs}".format(**details)
            )
        @backoff.on_exception(backoff.expo,asyncssh.DisconnectError,max_tries=8,
                            on_backoff=backoff_hdlr)
        async def get_info(url):
            current_info = self.get_fileinfo(url)
            purl = urllib.parse.urlparse(url)
            async with asyncssh.connect(purl.hostname,username=purl.username) as conn:
                if current_info.canonical_path is None:
                    readlink = await conn.run(f'readlink -f {purl.path}',check=False)
                    if readlink.exit_status == 0:
                        readlink = readlink.stdout.strip()
                        current_info = current_info._replace(canonical_path=readlink)
                if current_info.md5 is None: 
                    md5sum = await conn.run(f'md5sum {purl.path}',check=False)
                    if md5sum.exit_status == 0:
                        md5sum = md5sum.stdout.strip().split()[0]
                        current_info = current_info._replace(md5=md5sum)
                if current_info.size is None:
                    size = await conn.run(f'stat -c "%s" {purl.path}',check=False)
                    if size.exit_status == 0:
                        size = int(size.stdout.strip())
                        current_info = current_info._replace(size=size)
            return current_info

        results = [] 
        # Define the coro here so we can decorate with a backoff
        # while there is work to do, loop 
        while not url_queue.empty():
            url = await url_queue.get()
            info = await get_info(url)
            results.append(info)
            url_queue.task_done()
            if len(results) >= 50:
                self.update_fileinfo(results)
                pbar.update(len(results))
                results = []
        pbar.update(len(results))
        self.update_fileinfo(results)

    async def _calculate_fileinfo(self,files,max_tasks=7):
        # Get a url  queue and fill it
        url_queue = asyncio.Queue()
        hosts = set()
        for f in files:
            url_queue.put_nowait(f)
            hosts.add(urllib.parse.urlparse(f).hostname)  
        self.log.info(f'There are {url_queue.qsize()} urls to process')
        # Create a progress bar
        with tqdm(total=url_queue.qsize()) as pbar:
            # Get the event loop and control flow
            tasks = []
            for i in range(max_tasks):
                task = asyncio.create_task(self._info_worker(url_queue,pbar=pbar))
                await asyncio.sleep(3)
                tasks.append(task)
            await url_queue.join()
        # cancel tasks
        for task in tasks:
            task.cancel()
        # wait until all tasks cancel
        await asyncio.gather(*tasks,return_exceptions=True)

    def interactive_ignore_pattern(self,pattern,n=20):
        '''
            Start an interactive prompt to ignore patterns
            in file names (e.g. "test")
        '''
        from pprint import pprint
        import click
        matched_files = self.search_files(pattern)
        for i in range(0,len(matched_files),n):
            subset = matched_files[i:i+n]
            print('Ignore the following?')
            pprint(subset)
            if input("[Y/n]:").upper() == 'Y': 
                self.ignore_files(subset) 
            click.clear()

    def ignore_files(self,files):
        '''
            ignore files
        '''
        with self._bulk_transaction() as cur:
            cur.executemany('''
                UPDATE raw_files SET ignore = 1 
                WHERE url = ?
            ''',[(x,) for x in files])

    def search_files(self,url):
        '''
            Perform a search of files names (url/path)
        '''
        cur = self._db.cursor()
        name = f'%{url}%'
        names = cur.execute(
            'SELECT url FROM raw_files WHERE url LIKE ? and ignore != 1',(name,)        
        ).fetchall()
        return [x[0] for x in names]

    def search_accessions(self,name,include_scores=False,recurse=True):
        '''
            Performs a search of accession names 
        '''
        cur = self._db.cursor()
        sql_name = f'%{name}%'
        names = cur.execute(
            'SELECT name FROM accessions WHERE name LIKE ?',(sql_name,)
        ).fetchall()
        aliases = cur.execute(
            'SELECT alias FROM aliases WHERE alias LIKE ?',(sql_name,)
        ).fetchall()
        results = [(x[0],100) for x in names + aliases]
        # Find and Subset matches. e.g. Fat_shoulder_1 would
        # match 'M7956_Fat_shoulder_1'
        if len(results) == 0 and recurse == True:
            matches = [
                SequenceMatcher(None,name,x).find_longest_match(0,len(name),0,len(x)) \
                for x in self.names
            ] 
            best = sorted(matches,key=lambda x:x.size,reverse=True)[0]
            best = name[best.a:best.a+best.size]
            best = self.search_accessions(best,include_scores=True,recurse=False)[0]
            if len(best) > 0:
                results.append(best)
        results = sorted(results,key=lambda x:x[1],reverse=True)
        if include_scores == False:
            results = [x[0] for x in results]
        return results

    def search_metadata(self,**kwargs):
        '''
        '''
        n_crit = 0
        criteria = []
        for k,v in kwargs.items():
            criteria.append(f"(key = '{k}' AND val = '{v}')")
            n_crit += 1
        criteria = " OR ".join(criteria)
        # Build the query
        query = f'''
            SELECT AID FROM (
                SELECT AID, COUNT(*) as count FROM metadata 
                WHERE {criteria}
                GROUP BY AID
            )
            WHERE count = {n_crit}
        '''
        return [self[x] for (x,) in \
                self._db.cursor().execute(query).fetchall()
        ]
       
    async def crawl_host(self,hostname='localhost',path='/',
                         username=None,glob='*.fastq'):
        '''
            Use SSH to crawl a host looking for raw files
        '''
        if username is None:
            username = getpass.getuser()
        find_command = f'find -L {path} ! -readable -prune -o -name "{glob}" '
        async with asyncssh.connect(hostname,username=username) as conn:
            result = await conn.run(find_command,check=False)
        if result.exit_status == 0:
            files = result.stdout.split("\n")
        else:
            raise ValueError(f"Crawl failed: {result.stderr}")
        # add new files
        added = 0
        for f in files:
            if not f.startswith('/'):
                f = path + f 
            added += self.add_raw_file(f,scheme='ssh',username=username,
                    hostname=hostname)
        self.log.info(f'Found {added} new raw files')

    def add_raw_file(self,url,scheme='ssh',
        username=None,hostname=None):
        '''
            Add a raw file to the Cohort
        '''
        url = urllib.parse.urlparse(url)
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
            raise ValueError(f'url cannot be relative ({url.path})')
        url = urllib.parse.urlunparse(url)
        cur = self._db.cursor()
        cur.execute('''
            INSERT OR IGNORE INTO raw_files (url) VALUES (?)
        ''',(url,))
        # Return the num of db changes
        return self._db.changes()

    #------------------------------------------------------#
    #               Magic Methods                          #
    #------------------------------------------------------#

    def __repr__(self):
        return (f'Cohort("{self.name}") -- \n'
            f'\tcontains {len(self)} Accessions\n'
            f'\t{len(self.files)} files ({len(self.unassigned_files)} unassigned)')

    @invalidates_AID_cache
    def __delitem__(self, name):
        '''
            Remove a sample by name (or by composition)
        '''
        # First try
        AID = self._get_AID(name)

        self._db.cursor().execute('''
            DELETE FROM accessions WHERE AID = ?;
            DELETE FROM metadata WHERE AID = ?;
            DELETE FROM aid_files WHERE AID = ?;
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
        # Get the name based on AID
        name, = cur.execute('SELECT name FROM accessions WHERE AID = ?',(AID,)).fetchone() 
        metadata = {
            k: v for k, v in cur.execute('''
                SELECT key, val FROM metadata WHERE AID = ?;
                ''', (AID, )
            ).fetchall()
        }
        metadata['AID'] = AID
        files = [x[0] for x in cur.execute('''
                SELECT url FROM files WHERE AID = ?;
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

    #------------------------------------------------------#
    #               Internal Methods                       #
    #------------------------------------------------------#

    def _initialize_tables(self):
        cur = self._db.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS accessions (
                AID INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS aliases (
                alias TEXT UNIQUE,
                AID INTEGER,
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
        cur.execute('''
            CREATE TABLE IF NOT EXISTS raw_files (
                -- Basic File Info
                FID INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                -- MetaData
                ignore INT DEFAULT 0,
                canonical_path TEXT DEFAULT NULL,
                md5 TEXT DEFAULT NULL,
                size INT DEFAULT NULL
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS aid_files (
                AID INTEGER,
                FID INTEGER,
                PRIMARY KEY(AID,FID)
                FOREIGN KEY(AID) REFERENCES accessions(AID),
                FOREIGN KEY(FID) REFERENCES raw_files(FID)
            );
        ''')
        # Views ----------------------------------------------
        cur.execute('''
            CREATE VIEW IF NOT EXISTS files AS 
            SELECT AID,url
            FROM aid_files 
            JOIN raw_files 
                ON aid_files.FID = raw_files.FID;
        ''')
        cur.execute('''
            CREATE TRIGGER IF NOT EXISTS assign_FID INSTEAD OF INSERT ON files
            FOR EACH ROW
            BEGIN
                INSERT OR IGNORE INTO raw_files (url) VALUES (NEW.url);
                INSERT INTO aid_files (AID,FID) 
                  SELECT NEW.AID, FID
                  FROM raw_files WHERE url=NEW.url;
            END;
        ''')


    def get_name(self,name):
        AID = self._get_AID(name)
        name = self._db.cursor().execute(
            'SELECT name FROM accessions WHERE AID = ?',(AID,)
        ).fetchone()[0]
        return name

    def get_aliases(self,name):
        AID = self._get_AID(name)
        aliases = [ x[0] for x in self._db.cursor().execute(
            'SELECT alias FROM aliases WHERE AID = ?', (AID,)        
        )]
        return [self.get_name(name)] + aliases


    @lru_cache(maxsize=32768)
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
            pass
        try:
            return cur.execute(
                'SELECT AID FROM accessions WHERE AID = ?', (name,)
            ).fetchone()[0]
        except TypeError:
            raise NameError(f'{name} not in Cohort')


    #------------------------------------------------------#
    #               Class Methods                          #
    #------------------------------------------------------#

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



class interactive_assign_files(object):

    def __init__(self,cohort):
        # Global State Variables
        self.cohort = cohort
        self.fullpath = False
        self.names = None
        self.files = None
        self.fmask = None
        self.done = False
        self.edited = set()
        # Local State Variables
        self.cur = None
        self.cur_name = None
        self.cur_files = None
        self.cur_fmask = None

    @property
    def commands(self):
        commands = [
            x[0] for x in inspect.getmembers(interactive_assign_files) \
            if x[0].endswith('_cmd') 
        ]
        return commands

    @property
    def command_map(self):
        cmd_map = {}
        for cmd in self.commands:
            long_form = cmd.replace('_cmd','')
            short_form = cmd[0]
            cmd_map[long_form] = self.__getattribute__(cmd)
            cmd_map[short_form] = self.__getattribute__(cmd)
        return cmd_map

    def __call__(self,fdict=None):
        if fdict is None:
            fdict = self.cohort.assimilate_files(self.cohort.unassigned_files)
        self.names = []
        self.files = []
        for name,files in fdict.items():
            self.names.append(name)
            self.files.append([(False,x) for x in files])
        # Start at the beginning of the cursor
        self.update_cur(0)
        # Loop through the states
        self.done = False
        while not self.done:
            self.print_status()
            cmd = input('Input Command (h for help): ')
            try:
                self.command_map[cmd.strip()]()
            except KeyError as e:
                print('Not a valid command')
                self.help_cmd()

    def update_cur(self,i):
        i = min(max(0,i),len(self.names))
        self.cur = i
        self.cur_name = self.names[i]
        # Update the files to exclude 
        self.stage_files()
        self.edited.add(i)

    def stage_files(self):
        # Get the current accession
        acc = self.cohort[self.cur_name]
        # Update files to be ones not present in the accession
        cur_files = sorted([(m,f) for m,f in self.files[self.cur] if f not in acc.files])
        self.files[self.cur] = cur_files
        # remove the 
        if self.fullpath == False:
            cur_files = [(m,os.path.basename(x)) for m,x in cur_files]
        self.cur_files = cur_files

    def print_status(self):
        if self.cur is None:
            print('Currently no files to be assigned')
        else:
            click.clear()   
            aliases = self.cohort.get_aliases(self.cur_name)
            print(
                f'On {self.cur}/{len(self.names)}: {self.cur_name}, aka:{aliases}'        
            )
            print(self.cohort[self.cur_name])
            pprint([f'{i}:{mask}:{filename}' for i,(mask,filename) in enumerate(self.cur_files)])


    # Commands -----------------------------------
    def help_cmd(self):
        '(h)elp: prints commands'
        commands = [
            x[0] for x in inspect.getmembers(interactive_assign_files) \
            if x[0].endswith('_cmd') 
        ]
        docstrs = [
            inspect.getdoc(self.__getattribute__(cmd)) \
            for cmd in commands
        ] 
        print("\n".join(filter(None,docstrs)))
        input('Hit enter to continue')

    def quit_cmd(self):
        '(q)uit: quit the program'
        self.print_status()
        x = input("Would you like to save your work? [y/n]:")
        if x.lower() == 'y':
            self.save_cmd()
        self.done = True

    def next_cmd(self):
        '(n)ext: move to the next accession'
        self.update_cur(self.cur+1)

    def prev_cmd(self):
        '(p)rev: move to the previous accession'
        self.update_cur(self.cur-1)

    def goto_cmd(self):
        '(g)oto: go to a specific accession index'
        try:
            i = int(input('Goto which index?:'))
        except Exception as e:
            self.print_status()
            print("Please provide a valid integer index")
            self.goto_cmd()
        self.update_cur(i)

    def fullpath_cmd(self):
        '(f)ullpath: toggle full paths for files'
        self.fullpath = not self.fullpath
        self.stage_files()

    def save_cmd(self):
        '(s)ave: save the assigned files'
        updated_accessions = []
        for i in self.edited:
            files = self.files[i]
            acc  = self.cohort[self.names[i]]
            valid_files = [filename for mask,filename in files if mask is True]
            if len(valid_files) > 0:
                acc.files.update(valid_files)
            updated_accessions.append(acc)
        self.cohort.add_accessions(updated_accessions)
        self.stage_files()

    def add_cmd(self):
        '(a)dd: toggle whether or not to add files based on ranges'
        while True:
            self.print_status()
            try:
                rng = input('Enter Range :')
                if rng == 'q':
                    break
                if rng == '':
                    rng = ':'
                if rng.endswith(':'):
                    rng = rng + str(len(self.cur_files))
                if rng.startswith(':'):
                    rng = '0' + rng
                rng = rng.split(',')
                for r in rng:
                    if ':' in r:
                        r = r.split(':')
                        if len(r) == 2:
                            start,stop = map(int,r)
                            step = 1
                        elif len(r) == 3:
                            start,stop,step = map(int,r)
                        indices = np.arange(start,stop,step)
                    else:
                        indices = [int(r)]
                    for ind in indices:
                        m,f = self.cur_files[ind]
                        m = not m
                        self.cur_files[ind] = (m,f)
            except Exception as e:
                print(f"Invalid range: {rng}, use blank or 'q' to end")
                input(f'Press enter to continue')



        

