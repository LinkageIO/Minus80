import pytest
from minus80 import Accession


def test_bare_accession():
    x = Accession("empty")
    assert isinstance(x, Accession)
    str(x)
    repr(x)


def test_add_relative_path():
    x = Accession("empty")
    x.add_file("./test.txt")


def test_add_files():
    x = Accession("empty")
    x.add_files(["./test.txt", "test2.txt", "test3.txt"])


def test_accession_name(simpleAccession):
    assert simpleAccession.name == "Sample1"


def test_accession_files(simpleAccession):
    assert "file1.txt" in simpleAccession.files
    assert "file2.txt" in simpleAccession.files


def test_accession_metadata(simpleAccession):
    assert simpleAccession.metadata["type"] == "sample"


def test_accession_getitem(simpleAccession):
    assert simpleAccession["type"] == "sample"


def test_accession_setitem(simpleAccession):
    simpleAccession["added"] = True
    assert simpleAccession["added"] == True


def test_accession_file_check(RNAAccession1):
    assert len(RNAAccession1.files) == 4


def test_accession_add_file_skip_check(simpleAccession):
    simpleAccession.add_file("ssh://test@examples.com/path/to/file.txt")
    assert "ssh://test@examples.com/path/to/file.txt" in simpleAccession.files


def test_accession_files_are_set(simpleAccession):
    simpleAccession.add_file("/path/to/file.txt")
    len_files = len(simpleAccession.files)
    simpleAccession.add_file("/path/to/file.txt")
    assert len(simpleAccession.files) == len_files
