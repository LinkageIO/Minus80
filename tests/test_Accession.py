
import pytest


def test_accession_name(simpleAccession):
    assert simpleAccession.name == 'Sample1'

def test_accession_files(simpleAccession):
    assert 'file1.txt' in simpleAccession.files 
    assert 'file2.txt' in simpleAccession.files

def test_accession_metadata(simpleAccession):
    assert simpleAccession.metadata['type'] == 'sample'

def test_accession_getitem_overload(simpleAccession):
    assert simpleAccession['type'] == 'sample'
