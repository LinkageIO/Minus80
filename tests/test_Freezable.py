import os

import numpy as np
import pytest
import pandas as pd

from minus80 import Cohort
from minus80.Freezable import FreezableAPI

from minus80.Exceptions import TagInvalidError, FreezableNameInvalidError


def test_guess_type(simpleCohort):
    assert FreezableAPI.guess_type(simpleCohort) == "Cohort"


def test_store_df(simpleCohort):
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["a", "b", "c"])
    simpleCohort.m80.col["testTable"] = df


def test_get_df(simpleCohort):
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["a", "b", "c"])
    simpleCohort.m80.col["testTable"] = df
    df2 = simpleCohort.m80.col["testTable"]
    assert all(df == df2)


def test_get_col_idxcol(simpleCohort):
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["a", "b", "c"])
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


def test_list_datasets(simpleCohort):
    arr = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9])
    simpleCohort.m80.col["testArray"] = arr
    assert "testArray" in simpleCohort.m80.col.list()


def test_get_array(simpleCohort):
    arr = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9])
    simpleCohort.m80.col["testArray"] = arr
    arr2 = simpleCohort.m80.col["testArray"]
    assert all(arr == arr2)


def test_bad_columnar_dataset(simpleCohort):
    # Try to put a list in there
    with pytest.raises(ValueError):
        arr = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        simpleCohort.m80.col["testArray"] = arr


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


def test_manifest(simpleCohort):
    assert isinstance(simpleCohort.m80.manifest.all(), list)


def test_delete_m80():

    c = Cohort("DeleteMe")
    dbFile = os.path.join(c.m80.thawed_dir, "db.sqlite")
    assert os.path.exists(dbFile) == True
    FreezableAPI.delete("Cohort", "DeleteMe")
    assert os.path.exists(dbFile) == False


def test_delete_missing():

    c = Cohort("DeleteMe")
    dbFile = os.path.join(c.m80.thawed_dir, "db.sqlite")
    assert os.path.exists(dbFile) == True
    # Giving the wrong information shouldnt do anything
    FreezableAPI.delete("Cohort", "DeleteMeee")
    assert os.path.exists(dbFile) == True
    FreezableAPI.delete("Cohort", "DeleteMe")
    assert os.path.exists(dbFile) == False


def test_db_query(simpleCohort):

    assert isinstance(
        simpleCohort.m80.db.query("SELECT * FROM accessions"), pd.DataFrame
    )


def test_db_rollback(simpleCohort):
    # Store the number of samples
    num_samples = len(simpleCohort)
    with pytest.raises(Exception):
        # Start a transaction
        with simpleCohort.m80.db.bulk_transaction():
            # Delete a samples
            del simpleCohort["Sample1"]
            assert len(simpleCohort) == num_samples - 1
            # Raise an excpetion to trigger a rollback
            raise Exception
    # The number of samples should be as before
    assert len(simpleCohort) == num_samples



# Test the API static methods ------------------------------------------------------------------------------------------

def test_get_datasets():
    FreezableAPI.datasets()


def test_get_fullpath_files():
    FreezableAPI.datasets(fullpath=True)



def test_available_bool(simpleCohort):
    assert FreezableAPI.exists(dtype="Cohort", name="TestCohort") == True


def test_unavailable_bool(simpleCohort):
    assert FreezableAPI.exists(dtype="Cohort", name="ERROR") == False


def test_parse_slug():
    assert FreezableAPI.parse_slug("Project.foobar:v1") == ("Project", "foobar", "v1")


def test_parse_slug_no_tag():
    assert FreezableAPI.parse_slug("Project.foobar") == ("Project", "foobar", None)


def test_bad_freezable_name():
    with pytest.raises(FreezableNameInvalidError):
        FreezableAPI.parse_slug("Project/foobar")


def test_valid_freezable_name():
    valid = "foobar"
    assert FreezableAPI.validate_freezable_name(valid) == valid


def test_invalid_freezable_name():
    with pytest.raises(FreezableNameInvalidError):
        valid = "foobar."
        assert FreezableAPI.validate_freezable_name(valid) == valid


def test_test_tagname_invalid_when_none():
    with pytest.raises(TagInvalidError):
        assert FreezableAPI.validate_tagname(None)


def test_test_tagname_invalid_when_contains_colon():
    with pytest.raises(TagInvalidError):
        assert FreezableAPI.validate_tagname("test:test")
