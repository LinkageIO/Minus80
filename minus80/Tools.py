from .Config import cf
import os
import shutil

from glob import glob
from collections import defaultdict
from pprint import pprint

__all__ = ['available', 'delete']

def get_files(name, dtype=None, fullpath=False):
    '''
        List the files in the minus80 directory
        associated with a dtype and a name.

        Parameters
        ----------
        name: str, required
            The name of the dataset. Note: accepts glob arguments.
        dtype: str, default=None
            The data type of the dataset. E.g.: Cohort.
            If None, a wildward will be used to retrieve all
            dtypes with the name will be returned.
        fullpath: bool, default=False
            If true, full paths to files will be returned
            if false, only filenames will be returned.


        .. note:: This will only return top level files which sometimes
                  will be directories.
    '''
    bdir = os.path.expanduser(cf.options.basedir)
    if dtype is not None:
        name = f'{name}.*{dtype}.*'
    else:
        name = f'{name}.*'
    data_dir = os.path.join(bdir, 'databases', name)
    files = sorted(glob(data_dir))
    #if dtype is not None:
    #    files = [x for x in files if x.endswith(f'{dtype}.db')]
    #if name is not None:
    #    files = [x for x in files if x.startswith(f'{name}.')]
    if fullpath:
        files = files
    else:
        files = [os.path.basename(x) for x in files]
    return files

def available(name='*',  dtype=''):
    ''' 
        Reports the available datasets **Frozen** in the minus80
        database.

        Parameters
        ----------
        dtype : str
            Each dataset has a datatype associated with it. E.g.:
            `Cohort`. If no dtype is specified, all available dtypes
            will be returned.
        name : str, default:'*'
            The name of the dataset you want to check is available.
            The default value is the wildcard '*' which will return
            all available datasets with the specified dtype.

        Returns
        -------
        bool, None
            If both dtype and name are specified, a bool is returned
            indiciating if the dataset is available. Otherise a formatted
            table is printed and None is returned.
    '''
    files = get_files(name, dtype)

    bdir = os.path.expanduser(cf.options.basedir)
    print(f'Using basedir: {bdir}')
    # Get the names of the individual datasets
    datasets = defaultdict(list)
    for f in files:
        if f.endswith('.db'):
            x, *_, y = f.replace('.db', '').split('.')
            datasets[y].append(x)
    # If both are specified, return a boolean
    if dtype != '' and name != '*':
        if name in datasets[dtype]:
            return True
        else:
            return False
    # Else, print a formatted table
    if len(files) == 0:
        print("--- Nothing here yet ---")
        return None
    for dtype, names in datasets.items():
        print(f"--- {dtype}: -----------------")
        for i, name in enumerate(names, 1):
            print(f'\t{i}. {name}')

def delete(name, dtype=None, force=False):
    ''' 
        Deletes files associated with Minus80 datasets.

        Parameters
        ----------
        name : str
            The name of the dataset you want to delete
        dtype : str
            Each dataset has a datatype associated with it. E.g.:
            `Cohort`. If no dtype is specified, all available dtypes
            will be returned.
        force : bool, default: False
            If False, the function will list off the files it wants to delete.
            If True, it will do what you tell it to do and just delete things
            (not recommended).

        Returns
        -------
        int
            Returns the number of files deleted

        .. warning:: This is damaging. Deleted datasets cannot be (easily) recovered.
    '''
    # Get a filecard for all the minus80 filenames that match the
    # type and the name
    files = get_files(name, dtype=dtype)
    if force != True:
        print(f'Are you sure you want to delete {len(files)} files?:\n')
        pprint(f'{files}')
        if input('[y/n]: ').upper() != 'Y':
            print('Nothing deleted.')
            return 0
    # delete them
    num_deleted = 0
    for filename in files:
        bdir = os.path.expanduser(cf.options.basedir)
        data_dir = os.path.join(bdir, 'databases')
        filename = os.path.join(data_dir, filename)
        if os.path.isfile(filename):
            os.remove(filename)
        elif os.path.isdir(filename):
            shutil.rmtree(filename)
        num_deleted += 1
    return num_deleted


def directory_search(directory, suffix='.fastq'):
    '''
        Search a directory and create Accessions from the files found there.
    '''
    from minus80 import Accession

    accessions = dict()
    for root, _, files in os.walk(directory, followlinks=True):
        for f in files:
            if f.endswith(suffix):
                fid, *_ = f.split('_')
                if fid not in accessions:
                    accessions[fid] = Accession(fid, files=[])
                x = accessions[fid]
                x.add_file(os.path.join(root, f))


    return accessions




