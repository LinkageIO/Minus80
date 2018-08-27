import os
import numpy as np
import pytest
import pandas as pd

from minus80.Config import cf
from minus80 import Cohort
from minus80.Freezable import guess_type

def test_guess_type(simpleCohort):
    assert guess_type(simpleCohort) == 'Cohort'

def test_dbpath(simpleCohort):
    assert os.path.exists(simpleCohort._get_dbpath('db.sqlite'))
    

def test_store_bcolz(simpleCohort):
    df = pd.DataFrame([[1,2,3],[4,5,6],[7,8,9]],columns=['a','b','c'])
    simpleCohort._bcolz('testTable',df=df)


def test_get_bcolz(simpleCohort):
    df = pd.DataFrame([[1,2,3],[4,5,6],[7,8,9]],columns=['a','b','c'])
    simpleCohort._bcolz('testTable',df=df)
    df2 = simpleCohort._bcolz('testTable')
    assert all(df == df2)

def test_get_bcolz_idxcol(simpleCohort):
    df = pd.DataFrame([[1,2,3],[4,5,6],[7,8,9]],columns=['a','b','c'])
    df.set_index('a',inplace=True)
    simpleCohort._bcolz('testTable_idx',df=df)
    df2 = simpleCohort._bcolz('testTable_idx')
    assert all(df == df2)

def test_get_bcolz_blaze(simpleCohort):
    df = pd.DataFrame([[1,2,3],[4,5,6],[7,8,9]],columns=['a','b','c'])
    simpleCohort._bcolz('testTable_blaze',df=df)
    df2 = simpleCohort._bcolz('testTable_blaze',blaze=True)
    assert all(df == df2)

def test_empty_bcolz_df(simpleCohort):
    df = pd.DataFrame()
    simpleCohort._bcolz('empty_testTable',df=df)
    df2 = simpleCohort._bcolz('empty_testTable')
    assert all(df == df2)

def test_empty_bcolz_df_blaze(simpleCohort):
    df = pd.DataFrame()
    simpleCohort._bcolz('empty_testTable',df=df)
    df2 = simpleCohort._bcolz('empty_testTable',blaze=True)
    assert all(df == df2)


def test_get_bcolz_IO_error(simpleCohort):
    with pytest.raises(Exception) as e_info:
        df2 = simpleCohort._bcolz('ERROR')

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

def test_dict_float(simpleCohort):
    simpleCohort._dict('test_float',12.5)
    assert simpleCohort._dict('test_float') == 12.5

def test_dict_bad_val_type(simpleCohort):
    with pytest.raises(Exception) as e_info:
        simpleCohort._dict('test',[])

def test_m80_name(simpleCohort):
    assert simpleCohort._m80_name == 'TestCohort'

def test_m80_dtype(simpleCohort):
    assert simpleCohort._m80_dtype == 'Cohort'

def test_delete_m80():
    import os
    from minus80.Tools import delete
    c = Cohort('DeleteMe')
    dbFile = c._get_dbpath('db.sqlite')
    assert os.path.exists(dbFile) == True
    delete('DeleteMe',force=True)
    assert os.path.exists(dbFile) == False


def test_bulk_transaction(simpleCohort):
    with simpleCohort.bulk_transaction() as cur:
        cur.execute('''INSERT OR REPLACE INTO globals                                               
            (key, val, type) VALUES (?, ?, ?)''',
            ('test_bulk', '2', 'int'))
    assert simpleCohort._dict('test_bulk') == 2

def test_bulk_transaction_rollback(simpleCohort):
    with pytest.raises(Exception) as e_info:
        with simpleCohort.bulk_transaction() as cur:
            cur.execute('''INSERT OR REPLACE INTO ERROR                                               
                (key, val, type) VALUES (?, ?, ?)''',
                ('test_bulk', 2, 'int'))
