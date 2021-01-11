from minus80.Config import cf, Level


def test_get_attr():
    cf.test = "a"
    assert cf.test == "a"


def test_get_item():
    assert cf["options"]


def test_set_level_attr():
    cf.test = Level()
    cf.test.passed = True
    assert cf.test.passed


def test_pprint():
    repr(cf)
    assert True
