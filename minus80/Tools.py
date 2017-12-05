from .Config import cf
import os

from glob import glob
from collections import defaultdict
from pprint import pprint

def get_files(dtype=None,name=None,fullpath=False):
    '''
        List the files in the minus80 directory
    '''
    bdir = os.path.expanduser(cf.options.basedir)
    data_dir = os.path.join(bdir,'databases')
    files = os.listdir(data_dir)
    if dtype is not None:
        files = [x for x in files if x.endswith(f'{dtype}.db')]
    if name is not None:
        files = [x for x in files if x.startswith(f'{name}.')]
    if fullpath:
        files = [f'{data_dir}/{file}' for file in files]
    return files

def available(dtype=None,name=None):
    '''
        Reports
    '''
    files = get_files(dtype,name)
    
    if len(files) == 0:
        print("--- Nothing here yet ---")
        return False

    # Get the names of the individual datasets
    datasets = defaultdict(list)
    for f in files:
        if f.endswith('.db'):
            x,*subjunk,y = f.replace('.db','').split('.')
            datasets[y].append(x)
    if dtype is not None and name is None:
        datasets = {dtype:datasets[dtype]}
    elif dtype is not None and name is not None:
        if name in datasets[dtype]:
            return True
    for dtype,names in datasets.items():
        print(f"--- {dtype}: -----------------")
        for i,name in enumerate(names,1):
            print(f'\t{i}. {name}')
    
def delete(name,dtype=None,force=False):
    '''
        Deletes all of the Minus80 datasets

        Warning: This is damaging.
    '''
    # Get a filecard for all the minus80 filenames that match the 
    # type and the name
    files = get_files(dtype,name)
    if force != True:
        print(f'Are you sure you want to delete {len(files)} files?:\n{files}')
        if input('[y/n]').upper() != 'Y':
            print('Nothing deleted.')
            return
    # delete them
    for filename in files: 
        bdir = os.path.expanduser(cf.options.basedir)
        data_dir = os.path.join(bdir,'databases')
        filename = os.path.join(data_dir,filename)
        if os.path.isfile(filename):
            os.remove(filename)
        elif os.path.isdir(filename):
            shutil.rmtree(filename)



