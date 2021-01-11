import pytest
import tempfile

from minus80 import Accession, Cohort


def test_init(simpleCohort, RNACohort):
    x = simpleCohort
    assert isinstance(x, Cohort)


def test_init_different_rootdir():
    tmpdir = tempfile.TemporaryDirectory()
    x = Cohort("tmpCohort", rootdir=tmpdir.name)
    assert str(x.m80.basedir).startswith(tmpdir.name)


def test_repr(simpleCohort):
    repr(simpleCohort)


def test_get_AID_from_name(simpleCohort):
    assert simpleCohort._get_AID("Sample1") == 1


def test_get_AID(simpleCohort):
    simpleCohort._AID_mapping["Sample1"] == 1


def test_add_accession(simpleCohort):
    a = Accession("Sample4", files=["file1.txt", "file2.txt"], type="CHIP")
    if a in simpleCohort:
        del simpleCohort[a]
    start_len = len(simpleCohort)
    simpleCohort.add_accession(a)
    assert len(simpleCohort) == start_len + 1


def test_delitem(simpleCohort):
    a = Accession("TESTSAMPLE_IGNORE", files=["file1.txt", "file2.txt"], type="CHIP")
    if a not in simpleCohort:
        simpleCohort.add_accession(a)
    start_len = len(simpleCohort)
    del simpleCohort["TESTSAMPLE_IGNORE"]
    assert len(simpleCohort) == start_len - 1


def test_getitem(simpleCohort):
    x = simpleCohort["Sample1"]
    assert isinstance(x, Accession)


def test_len(simpleCohort):
    assert isinstance(len(simpleCohort), int)


def test_contains(simpleCohort):
    assert "Sample1" in simpleCohort


def test_iter(simpleCohort):
    for x in simpleCohort:
        assert isinstance(x, Accession)


def test_random_accession(simpleCohort):
    a = simpleCohort.random_accession()
    assert isinstance(a, Accession)


def test_random_accessions(simpleCohort):
    a = simpleCohort.random_accessions(n=2)
    assert all([isinstance(k, Accession) for k in a])


def test_random_too_many_accessions(simpleCohort):
    with pytest.raises(Exception):
        simpleCohort.random_accessions(n=200)


def test_random_accessions_replace(simpleCohort):
    a = simpleCohort.random_accessions(n=2, replace=True)
    assert all([isinstance(k, Accession) for k in a])


def test_from_accessions():
    a = Accession("Sample1", files=["file1.txt", "file2.txt"], type="WGS")
    b = Accession("Sample2", files=["file1.txt", "file2.txt"], type="WGS")
    c = Accession("Sample3", files=["file1.txt", "file2.txt"], type="CHIP")
    d = Accession("Sample4", files=["file1.txt", "file2.txt"], type="CHIP")
    Cohort.from_accessions("TestCohort", [a, b, c, d])


def test_get_columsn(simpleCohort):
    assert "type" in simpleCohort.columns


def test_get_names(simpleCohort):
    assert ["Sample1", "Sample2", "Sample3", "Sample4"] == simpleCohort.names


def test_get_raw_files(simpleCohort):
    assert ["file1.txt", "file2.txt"] == simpleCohort.raw_files


def test_ignored_files(simpleCohort):
    assert [] == simpleCohort.ignored_files


def test_num_files(simpleCohort):
    assert simpleCohort.num_files == 2


def test_as_DataFrame(simpleCohort):
    import pandas as pd

    assert isinstance(simpleCohort.as_DataFrame(), pd.DataFrame)


def test_get_fileinfo(simpleCohort):
    assert simpleCohort.get_fileinfo("file1.txt").url == "file1.txt"


def test_add_accession_by_df(simpleCohort):
    import pandas as pd

    df = pd.DataFrame(
        [["S1", 23, "O"], ["S2", 30, "O+"]], columns=["Name", "Age", "Type"]
    )
    simpleCohort.add_accessions_from_DataFrame(df, "Name")


def test_add_accession_by_df_bad_name(simpleCohort):
    import pandas as pd

    df = pd.DataFrame(
        [["S1", 23, "O"], ["S2", 30, "O+"]], columns=["Name", "Age", "Type"]
    )
    with pytest.raises(ValueError):
        simpleCohort.add_accessions_from_DataFrame(df, "BAD")


def test_add_accession_by_df_delete_missing_Data(simpleCohort):
    import pandas as pd
    import numpy as np

    # Minus80 will dele the Age value for S1
    df = pd.DataFrame(
        [["S1", np.nan, "O"], ["S2", 30, "O+"]], columns=["Name", "Age", "Type"]
    )
    simpleCohort.add_accessions_from_DataFrame(df, "Name")


def test_search_files(simpleCohort):
    assert simpleCohort.search_files("file1") == ["file1.txt"]


def test_search_accessions(simpleCohort):
    assert simpleCohort.search_accessions("Sample") == [
        "Sample1",
        "Sample2",
        "Sample3",
        "Sample4",
    ]


def test_search_metadata(simpleCohort):
    assert len(simpleCohort.search_metadata(type="WGS")) == 2


def test_add_raw_file(simpleCohort):
    simpleCohort.add_raw_file("test.txt")


def test_add_raw_file_relative_fails(simpleCohort):
    with pytest.raises(ValueError):
        simpleCohort.add_raw_file("./test.txt")


def test_get_name(simpleCohort):
    assert simpleCohort.get_name("Sample1") == "Sample1"


def test_get_name_from_AID(simpleCohort):
    assert simpleCohort.get_name(simpleCohort["Sample1"]["AID"]) == "Sample1"


def test_get_aliases(simpleCohort):
    assert simpleCohort.get_aliases("Sample1")
