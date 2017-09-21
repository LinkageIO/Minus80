import pytest

from minus80 import Accession,Cohort

def test_init(simpleCohort):
    x = simpleCohort
    assert isinstance(x,Cohort)

def test_get_AID_from_name(simpleCohort):
    assert simpleCohort._get_AID('Sample1') == 1

def test_add_accession(simpleCohort):
    a = Accession('Sample4',files=['file1.txt','file2.txt'],type='CHIP')
    if a in simpleCohort:
        del simpleCohort[a]
    start_len = len(simpleCohort)
    simpleCohort.add_accession(a)
    assert len(simpleCohort) == start_len + 1

def test_delitem(simpleCohort):
    pass

def test_getitem(simpleCohort):
    pass

def test_len(simpleCohort):
    pass

def test_contains(simpleCohort):
    pass

def test_iter(simpleCohort):
    pass
