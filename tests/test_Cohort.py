import pytest

from minus80 import Accession, Cohort


def test_init(simpleCohort, RNACohort):
    x = simpleCohort
    assert isinstance(x, Cohort)


def test_repr(simpleCohort):
    x = repr(simpleCohort)


def test_get_AID_from_name(simpleCohort):
    assert simpleCohort._get_AID("Sample1") == 1


def test_get_AID(simpleCohort):
    aid_map = simpleCohort._AID_mapping["Sample1"] == 1


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
    with pytest.raises(Exception) as e_info:
        a = simpleCohort.random_accessions(n=200)


def test_random_accessions_replace(simpleCohort):
    a = simpleCohort.random_accessions(n=2, replace=True)
    assert all([isinstance(k, Accession) for k in a])


def test_from_accessions():
    a = Accession("Sample1", files=["file1.txt", "file2.txt"], type="WGS")
    b = Accession("Sample2", files=["file1.txt", "file2.txt"], type="WGS")
    c = Accession("Sample3", files=["file1.txt", "file2.txt"], type="CHIP")
    d = Accession("Sample4", files=["file1.txt", "file2.txt"], type="CHIP")
    x = Cohort.from_accessions("TestCohort", [a, b, c, d])
