import pytest
from minus80.Config import cf, Level


def test_get_attr():
    cf.test = "a"
    assert cf.test == "a"


def test_get_item():
    level = cf["options"]


def test_set_level_attr():
    cf.test = Level()
    cf.test.passed = True
    assert cf.test.passed


def test_get_cloud_creds():
    assert cf.gcp.bucket


def test_pprint():
    x = repr(cf)
    assert True
