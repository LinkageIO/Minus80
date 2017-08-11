import os
import sys
import time
import re
import functools
import glob
import shutil

from termcolor import colored, cprint
from itertools import chain
from collections import OrderedDict

from .Locus import Locus
from .Config import cf
from apsw import CantOpenError

import camoco as co

import matplotlib.pylab as pylab
import numpy as np
import pandas as pd
import statsmodels.api as sm

import gzip
import bz2

def available_datasets(type='%', name='%'):
    try:
        cur = co.Camoco("Camoco", type='Camoco').db.cursor()
        datasets = cur.execute('''
            SELECT type, name, description, added
            FROM datasets 
            WHERE type LIKE ?
            AND name LIKE ?
            ORDER BY type;''', (type,name)).fetchall()
        if datasets:
            datasets = pd.DataFrame(
                datasets, 
                columns=["Type", "Name", "Description", "Date Added"],
            ).set_index(['Type'])
        else:
            datasets = pd.DataFrame(
                columns=["Type", "Name", "Description", "Date Added"]
            )
        # Check to see if we are looking for a specific dataset
        if '%' not in type and '%' not in name:
            return True if name in datasets['Name'].values else False
        else:
            return datasets
    except CantOpenError as e:
        raise e
        

def available(type=None,name=None):
    # Laaaaaaaaazy
    return available_datasets(type=type,name=name)

def del_dataset(type, name, force=False):
    try:
        c = co.Camoco("Camoco")
    except CantOpenError:
        return True
    if force == False:
        c.log("Are you sure you want to delete:\n {}.{}", type, name)
        if input("[Y/n] (Notice CAPS):") != 'Y':
            c.log("Nothing Deleted")
            return
    c.log("Deleting {}", name)
    try:
        c.db.cursor().execute('''
            DELETE FROM datasets 
            WHERE name LIKE '{}' 
            AND type LIKE '{}';'''.format(name, type)
        )
    except CantOpenError:
        pass
    try:
        dfiles = glob.glob(
            os.path.join(
                cf.options.basedir,
                'databases',
                '{}.{}.*'.format(type,name)
            )
        )
        for f in dfiles:
            c.log('Removing {}',f)
            try:
                os.remove(f)
            except IsADirectoryError:
                shutil.rmtree(f)
    except FileNotFoundError as e:
        pass
    if type == 'Expr':
        # also have to remove the COB specific refgen
        del_dataset('RefGen', 'Filtered'+name, force=force)
        del_dataset('Ontology', name+'MCL', force=force)
    return True

def mv_dataset(type,name,new_name):
    c = co.Camoco("Camoco")
    c.db.cursor().execute('''
        UPDATE datasets SET name = ? 
        WHERE name = ? AND 
        type = ?''',(new_name,name,type)
    )
    os.rename(
        c._resource('databases','.'.join([type,name])+".db"),
        c._resource('databases',".".join([type,new_name])+".db")
    )

class rawFile(object):
    def __init__(self,filename):
        self.filename = filename
        if filename.endswith('.gz'):
            self.handle = gzip.open(filename,'rt')
        elif filename.endswith('bz2'):
            self.handle = bz2.open(filename,'rt')
        else:
            self.handle = open(filename,'r')
    def __enter__(self):
        return self.handle
    def __exit__(self,type,value,traceback):
        self.handle.close()


def redescribe_dataset(type,name,new_desc):
    c = co.Camoco("Camoco")
    c.db.cursor().execute('''
        UPDATE datasets SET description = ? 
        WHERE name = ? AND type = ?''',
        (new_desc,name,type)
    )

def memoize(obj):
    cache = obj.cache = {}
    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        # Give us a way to clear the cache
        if 'clear_cache' in kwargs:
            cache.clear()
        # This wraps the calling of the memoized object
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer


class log(object):
    def __init__(self, msg=None, *args, color='green'):
        if msg is not None and cf.logging.log_level == 'verbose':
            print(
                colored(
                    " ".join(["[LOG]", time.ctime(), '-', msg.format(*args)]), 
                    color=color
                ), file=sys.stderr
            )

    @classmethod
    def warn(cls, msg, *args):
        cls(msg, *args, color='red')

    def __call__(self, msg, *args, color='green'):
        if cf.logging.log_level == 'verbose':
            print(
                colored(
                    " ".join(["[LOG]", time.ctime(), '-', msg.format(*args)]), 
                    color=color
                ),
            file=sys.stderr
        )
