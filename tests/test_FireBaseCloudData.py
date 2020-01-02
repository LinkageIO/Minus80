import pytest

def test_login(cloud):
    assert cloud.user is not None

def test_load_token_when_user_is_None(cloud):
    cloud._user = None
    # Load the token
    assert cloud.user is not None
