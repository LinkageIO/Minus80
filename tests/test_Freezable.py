import os
import numpy as np
import pytest
import pandas as pd

import minus80 as m80
from minus80.Config import cf
from minus80 import Cohort
from minus80.Freezable import guess_type


def test_guess_type(simpleCohort):
    assert guess_type(simpleCohort) == "Cohort"


def test_store_df(simpleCohort):
    df = pd.DataFrame([
        [1, 2, 3], 
        [4, 5, 6], 
        [7, 8, 9]], 
        columns=["a", "b", "c"]
    )
    simpleCohort.m80.col["testTable"] = df


def test_get_df(simpleCohort):
    df = pd.DataFrame([
        [1, 2, 3], 
        [4, 5, 6], 
        [7, 8, 9]], 
        columns=["a", "b", "c"]
    )
    simpleCohort.m80.col["testTable"] = df
    df2 = simpleCohort.m80.col["testTable"]
    assert all(df == df2)


def test_get_bcolz_idxcol(simpleCohort):
    df = pd.DataFrame([
        [1, 2, 3], 
        [4, 5, 6], 
        [7, 8, 9]], 
        columns=["a", "b", "c"]
    )
    df.set_index("a", inplace=True)
    simpleCohort.m80.col["testTable_idx"] = df
    df2 = simpleCohort.m80.col["testTable_idx"]
    assert all(df == df2)


def test_empty_df(simpleCohort):
    df = pd.DataFrame()
    simpleCohort.m80.col["empty_testTable"] = df
    df2 = simpleCohort.m80.col["empty_testTable"]
    assert all(df == df2)


def test_store_array(simpleCohort):
    arr = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9])
    simpleCohort.m80.col["testArray"] = arr


def test_get_array(simpleCohort):
    arr = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9])
    simpleCohort.m80.col["testArray"] = arr
    arr2 = simpleCohort.m80.col["testArray"]
    assert all(arr == arr2)


def test_tmpfile(simpleCohort):
    tmpfile = simpleCohort.m80.tmpfile()
    a = open(tmpfile.name, "w")
    print("test", file=a)
    a.close()
    b = open(tmpfile.name, "r")
    assert b.readline().strip() == "test"


def test_m80_name(simpleCohort):
    assert simpleCohort.m80.name == "TestCohort"


def test_m80_dtype(simpleCohort):
    assert simpleCohort.m80.dtype == "Cohort"


def test_delete_m80():
    import os
    from minus80.Tools import delete

    c = Cohort("DeleteMe")
    dbFile = os.path.join(c.m80.basedir,"db.sqlite")
    assert os.path.exists(dbFile) == True
    delete("Cohort", "DeleteMe", force=True)
    assert os.path.exists(dbFile) == False


def test_delete_missing():
    import os
    from minus80.Tools import delete

    c = Cohort("DeleteMe")
    dbFile = os.path.join(c.m80.basedir,"db.sqlite")
    assert os.path.exists(dbFile) == True
    # Giving the wrong information shouldnt do anything
    delete("Cohort", "DeleteMeee", force=True)
    assert os.path.exists(dbFile) == True
    delete("Cohort", "DeleteMe", force=True)
    assert os.path.exists(dbFile) == False

