import os
import string
import random
import tempfile
import subprocess
import pytest


import minus80 as m80

from click.testing import CliRunner
from minus80.Freezable import FreezableAPI
from minus80.cli import minus80 as cli


def test_project_init_cwd():
    tmpdir = tempfile.mkdtemp()
    os.chdir(f"{tmpdir}")
    run = CliRunner()
    run.invoke(cli.init, "foobar")


@pytest.mark.skipif(True, reason="no-cloud")
def test_login():
    run = CliRunner()
    x = run.invoke(cli.login)
    assert x.output == "Successfully logged in\n"


def test_project_init():
    tmpdir = tempfile.mkdtemp()
    run = CliRunner()
    x = run.invoke(cli.init, ["--path", f"{tmpdir}", "foobar"])
    assert os.path.exists(f"{tmpdir}/foobar")
    assert x.exit_code == 0


def test_project_double_init():
    tmpdir = tempfile.mkdtemp()
    run = CliRunner()
    x = run.invoke(cli.init, ["--path", f"{tmpdir}", "foobar"])
    assert os.path.exists(f"{tmpdir}/foobar")
    assert x.exit_code == 0
    # run again and check exit code
    x = run.invoke(cli.init, ["--path", f"{tmpdir}", "foobar"])
    assert x.exit_code == 1


def test_project_freeze():
    for _ in range(3):
        # Get the project
        chars = string.ascii_uppercase + string.digits
        random_tag = "".join(random.choice(chars) for _ in range(5))
        x = m80.Project("foobar")
        with open(x.m80.basedir / "thawed/data/random_numbers.txt", "w") as OUT:
            # print random number
            print(f"{random.randint(0,100)}", file=OUT)
        x = subprocess.run(
            f"minus80 freeze Project.foobar:{random_tag}".split(), capture_output=True
        )
        assert x.returncode == 0


def test_project_freeze_tag_exists():
    x = subprocess.run("minus80 freeze Project.foobar:v1".split(), capture_output=True)
    x = subprocess.run("minus80 freeze Project.foobar:v1".split(), capture_output=True)
    assert x.returncode == 1


def test_project_freeze_no_tag():
    x = subprocess.run("minus80 freeze Project.foobar".split(), capture_output=True)
    assert x.returncode == 1


def test_project_freeze_unavailable():
    x = subprocess.run("minus80 freeze Project.bizbaz:v1".split(), capture_output=True)
    assert x.returncode == 1


def test_project_freeze_not_valid_freezable_type():
    x = subprocess.run("minus80 freeze foobar.bizbaz:v1".split(), capture_output=True)
    assert x.returncode == 1


def test_project_thaw():
    foobar = m80.Project("foobar")
    rand_tag = random.sample(foobar.m80.tags, 1).pop()
    x = subprocess.run(f"minus80 thaw Project.foobar:{rand_tag}".split())
    assert x.returncode == 0


def test_project_thaw_no_tag():
    x = subprocess.run("minus80 thaw Project.foobar".split())
    assert x.returncode == 1


def test_project_thaw_missing():
    x = subprocess.run("minus80 thaw Project.bizbaz".split())
    assert x.returncode == 1


def test_project_thaw_missing_tag():
    x = subprocess.run("minus80 thaw Project.foobar:not_a_tag".split())
    assert x.returncode == 1


def test_project_list():
    x = subprocess.run("minus80 ls".split())
    assert x.returncode == 0


def test_project_list_with_tags():
    x = subprocess.run("minus80 ls --tags".split())
    assert x.returncode == 0


def test_delete_project():
    tmpdir = tempfile.mkdtemp()
    subprocess.run(f"minus80 init --path {tmpdir} bizbaz".split())
    assert FreezableAPI.exists("Project", "bizbaz")
    # Now delete the project
    subprocess.run("minus80 delete  Project.bizbaz --force".split())
    assert not FreezableAPI.exists("Project", "bizbaz")


def test_delete_bad_slug():
    x = subprocess.run("minus80 delete  Project|bizbaz --force".split())
    assert x.returncode == 1


def test_delete_missing_dataset():
    # bazbaz does not exist
    x = subprocess.run("minus80 delete  Project.bazbaz --force".split())
    assert x.returncode == 1


def test_delete_project_bad_tag():
    tmpdir = tempfile.mkdtemp()
    subprocess.run(f"minus80 init --path {tmpdir} bizbaz".split())
    assert FreezableAPI.exists("Project", "bizbaz")
    # Try to delete with tag
    subprocess.run("minus80 delete  Project.bizbaz:v1 --force".split())
    # Now delete the project for real
    subprocess.run("minus80 delete  Project.bizbaz --force".split())
    assert not FreezableAPI.exists("Project", "bizbaz")
