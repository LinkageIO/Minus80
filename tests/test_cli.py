import os
import pytest
import subprocess

def test_cli_dry_run():
    cmd = f"minus80 --help".split()
    subprocess.run(cmd)

def test_login():
    m80_username = os.environ['MINUS80_USERNAME']
    m80_password = os.environ['MINUS80_PASSWORD']

    cmd = f"minus80 cloud login --username {m80_username} --password {m80_password} --force".split()
    subprocess.run(cmd)

def test_project_init():
    import tempfile
    import pathlib
    tmpdir = tempfile.mkdtemp()
    cmd = f'minus80 init --path {tmpdir} foobar'.split()
    subprocess.run(cmd)

def test_project_list():
    cmd = f'minus80 list'.split()
    subprocess.run(cmd)

