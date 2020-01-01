import pytest
import minus80.Tools as Tools

from minus80.Exceptions import (
            TagInvalidError,
            FreezableNameInvalidError
        )

def test_get_files():
    Tools.get_files()

def test_get_fullpath_files():
    Tools.get_files(fullpath=True)

def test_available():
    Tools.available()

def test_available_with_tags():
    Tools.available(tags=True)

def test_available_bool(simpleCohort):
    assert Tools.available(dtype="Cohort", name="TestCohort") == True

def test_unavailable_bool(simpleCohort):
    assert Tools.available(dtype="Cohort", name="ERROR") == False

def test_parse_slug():
    assert Tools.parse_slug('Project.foobar:v1') == ('Project', 'foobar', 'v1')

def test_parse_slug_no_tag():
    assert Tools.parse_slug('Project.foobar') == ('Project', 'foobar', None)

def test_bad_freezable_name():
    with pytest.raises(FreezableNameInvalidError):
        Tools.parse_slug('Project/foobar') 

def test_valid_freezable_name():
    valid = 'foobar'
    assert Tools.validate_freezable_name(valid) == valid

def test_invalid_freezable_name():
    with pytest.raises(FreezableNameInvalidError):
        valid = 'foobar.'
        assert Tools.validate_freezable_name(valid) == valid

def test_test_tagname_invalid_when_none():
    with pytest.raises(TagInvalidError):
        assert Tools.validate_tagname(None)

def test_test_tagname_invalid_when_contains_colon():
    with pytest.raises(TagInvalidError):
        assert Tools.validate_tagname('test:test')
