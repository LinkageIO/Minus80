
import pytest


def test_accession_name(simpleAccession):
    assert simpleAccession.name == 'Sample1'

def test_accession_files(simpleAccession):
    assert 'file1.txt' in simpleAccession.files 
    assert 'file2.txt' in simpleAccession.files

def test_accession_metadata(simpleAccession):
    assert simpleAccession.metadata['type'] == 'sample'

def test_accession_getitem(simpleAccession):
    assert simpleAccession['type'] == 'sample'

def test_accession_setitem(simpleAccession):
    simpleAccession['added'] = True
    assert simpleAccession['added'] == True

def test_accession_add_file(simpleAccession):
    simpleAccession.add_file('/path/to/file.txt')
    assert '/path/to/file.txt' in simpleAccession.files

def test_accession_files_are_set(simpleAccession):
    simpleAccession.add_file('/path/to/file.txt')
    len_files = len(simpleAccession.files)
    simpleAccession.add_file('/path/to/file.txt')
    assert len(simpleAccession.files) == len_files

