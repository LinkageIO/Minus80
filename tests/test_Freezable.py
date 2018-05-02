import pytest
import numpy as np
import pandas as pd

from minus80 import Cohort

def test_guess_type(simpleCohort):
    assert Cohort.guess_type(simpleCohort) == 'Cohort'

def test_dbfilename(simpleCohort):
    mname = simpleCohort._m80_name
    mtype = simpleCohort._m80_type
    assert simpleCohort._dbfilename().endswith(f'{mname}.{mtype}.db')

def test_open_db(simpleCohort):
    simpleCohort._open_db(simpleCohort._m80_name)

def test_store_bcolz(simpleCohort):
    df = pd.DataFrame([[1,2,3],[4,5,6],[7,8,9]],columns=['a','b','c']) 
    simpleCohort._bcolz('testTable',df=df)


def test_get_bcolz(simpleCohort):
    df = pd.DataFrame([[1,2,3],[4,5,6],[7,8,9]],columns=['a','b','c']) 
    simpleCohort._bcolz('testTable',df=df)
    df2 = simpleCohort._bcolz('testTable')
    assert all(df == df2)

def test_store_bcolz_array(simpleCohort):
    arr = np.array([1,2,3,4,5,6,7,8,9])
    simpleCohort._bcolz_array('testArray',array=arr)

def test_get_bcolz_array(simpleCohort):
    arr = np.array([1,2,3,4,5,6,7,8,9])
    simpleCohort._bcolz_array('testArray',array=arr)
    arr2 = simpleCohort._bcolz_array('testArray')
    assert all(arr == arr2)
    
def test_tmpfile(simpleCohort):
    tmpfile = simpleCohort._tmpfile()
    a = open(tmpfile.name,'w')
    print('test',file=a)
    a.close()
    b = open(tmpfile.name,'r')
    assert b.readline().strip() == 'test'

def test_dict(simpleCohort):
    simpleCohort._dict('test','TEST')
    assert simpleCohort._dict('test') == 'TEST'
    try:
        simpleCohort._dict('TEST')
    except ValueError:
        pass
    assert True

def test_m80_name(simpleCohort):
    assert simpleCohort._m80_name == 'TestCohort'

def test_m80_type(simpleCohort):
    assert simpleCohort._m80_type == 'Cohort'

def test_delete_m80():
    import os
    from minus80.Tools import delete
    c = Cohort('DeleteMe')
    dbFile = c._dbfilename()
    assert os.path.exists(dbFile) == True
    delete('DeleteMe',force=True)
    assert os.path.exists(dbFile) == False
