from .Config import cf
import os
import shutil

from glob import glob
from collections import defaultdict
from pprint import pprint
from subprocess import check_call,CalledProcessError

__all__ = ['available', 'delete']


def install_apsw(method='pip',version='3.27.2',tag='-r1'):
    if method == 'pip':
        print('Installing apsw from GitHub using ')
        version = '3.27.2'
        tag = '-r1'
        check_call(f'''\
            pip install  \
            https://github.com/rogerbinns/apsw/releases/download/{version}{tag}/apsw-{version}{tag}.zip \
            --global-option=fetch \
            --global-option=--version \
            --global-option={version} \
            --global-option=--all \
            --global-option=build  \
            --global-option=--enable=rtree \
        '''.split())
    else:
        raise ValueError(f'{method} not supported to install apsw')


def get_files(dtype=None, name=None, fullpath=False):
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
    if name is None:
        name = "*"
    if dtype is None:
        dtype = "*"
    data_dir = os.path.join(bdir, 'databases', f'{dtype}.{name}')
    files = sorted(glob(data_dir))
    if fullpath:
        files = files
    else:
        files = [os.path.basename(x) for x in files]
    return files

def available(dtype=None,name=None):
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
    files = get_files(dtype=dtype,name=name)

    bdir = os.path.expanduser(cf.options.basedir)
    print(f'Using basedir: {bdir}')
    # handle case where bool is returns when both params specified
    if dtype != None and name != None:
        if len(files) > 0:
            return True
        else:
            return False
    else:
        # Print message if nothing is here
        if len(files) == 0: # pragma: no cover
            print("--- Nothing here yet ---")
            return None
        # group by dtype and print
        datasets = defaultdict(list)
        for f in files:
            dtype,name = f.split('.') 
            datasets[dtype].append(name)
        # Print a formatted table
        for dtype, names in datasets.items():
            print(f"--- {dtype}: -----------------")
            for i, name in enumerate(names, 1):
                print(f'\t{i}. {name}')

def delete(dtype=None, name=None, force=False):
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
    if not available(dtype,name):
        print(f'{dtype}.{name} does not exist -- nothing deleted')
        return
    # Get a filecard for all the minus80 filenames that match the
    # type and the name
    files = get_files(name=name, dtype=dtype)
    if force != True: # pragma: no cover
        print(f'Are you sure you want to delete {dtype}.{name}?:\n')
        if input('[y/n]: ').upper() != 'Y':
            print('Nothing deleted.')
            return 0
    # delete them
    num_deleted = 0
    for filename in files:
        bdir = os.path.expanduser(cf.options.basedir)
        data_dir = os.path.join(bdir, 'databases')
        filename = os.path.join(data_dir, filename)
        # delete it
        shutil.rmtree(filename)
        num_deleted += 1
    return num_deleted





