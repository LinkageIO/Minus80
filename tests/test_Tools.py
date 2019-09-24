import pytest
import minus80.Tools as Tools


def test_get_files():
    Tools.get_files()


def test_get_fullpath_files():
    Tools.get_files(fullpath=True)


def test_available():
    Tools.available()


def test_available_bool(simpleCohort):
    assert Tools.available(dtype="Cohort", name="TestCohort") == True


def test_unavailable_bool(simpleCohort):
    assert Tools.available(dtype="Cohort", name="ERROR") == False
