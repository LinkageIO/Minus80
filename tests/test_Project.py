import pytest
import shutil
import tempfile
import pathlib

import numpy as np

from pathlib import Path
from minus80 import Project
from minus80.Freezable import FreezableAPI

from minus80.Exceptions import (
    TagDoesNotExistError,
    TagExistsError,
    UnsavedChangesInThawedError,
)


@pytest.fixture
def simpleProject(scope="module"):
    tmpdir = tempfile.TemporaryDirectory()
    x = Project("simpleProject")
    x.create_link(Path(tmpdir.name) / "tmp")
    yield x
    FreezableAPI.delete(x.m80.dtype, x.m80.name)


def test_create_link_path_exists(simpleProject):
    tmpdir = tempfile.TemporaryDirectory()
    with pytest.raises(ValueError):
        simpleProject.create_link(tmpdir.name)


def test_init_project(simpleProject):
    assert simpleProject


def test_slug(simpleProject):
    assert simpleProject.m80.slug == "MINUS80.Project.simpleProject"


def test_thawed_dir(simpleProject):
    assert (simpleProject.m80.thawed_dir).exists


def test_frozen_dir(simpleProject):
    assert (simpleProject.m80.frozen_dir).exists


# Create a bunch of tags
def test_create_tag_v1(simpleProject, test_data_dir ):

    # copy a file into the project dir
    shutil.copyfile(
        src=test_data_dir / "Sample1_ATGTCA_L007_R1_001.fastq",
        dst=simpleProject.m80.thawed_dir / "Sample1_ATGTCA_L007_R1_001.fastq",
    )
    # The parent tag shouldnt exists until after the first freeze
    with pytest.raises(TagDoesNotExistError):
        simpleProject.m80.parent_tag
    assert simpleProject
    simpleProject.m80.freeze("v1")
    assert "v1" in simpleProject.m80.tags


def test_duplicate_tag(simpleProject):
    # try to create a duplicate tag
    simpleProject.m80.freeze("v1")
    with pytest.raises(TagExistsError):
        simpleProject.m80.freeze("v1")


def test_create_tag_v2(simpleProject, test_data_dir):
    # Create v1 tag
    shutil.copyfile(
        src=test_data_dir / "Sample1_ATGTCA_L007_R1_001.fastq",
        dst=simpleProject.m80.thawed_dir / "Sample1_ATGTCA_L007_R1_001.fastq",
    )
    simpleProject.m80.freeze("v1")
    # create v2 tag
    shutil.copyfile(
        src=test_data_dir / "Sample1_ATGTCA_L007_R2_001.fastq",
        dst=simpleProject.m80.thawed_dir / "Sample1_ATGTCA_L007_R2_001.fastq",
    )
    simpleProject.m80.freeze("v2")
    assert "v2" in simpleProject.m80.tags
    # check the the thawed tag is equal to the parent tag
    assert simpleProject.m80.thawed_tag["parent"] == simpleProject.m80.parent_tag["tag"]

    # Thaw back to v1
    simpleProject.m80.thaw("v1")
    assert "Sample1_ATGTCA_L007_R1_001.fastq" in simpleProject.m80.checksum["files"]
    assert "Sample1_ATGTCA_L007_R2_001.fastq" not in simpleProject.m80.checksum["files"]


def test_thaw_nonexistant(simpleProject):
    with pytest.raises(TagDoesNotExistError):
        simpleProject.m80.thaw("nope")


def test_thaw_with_unsaved_changes(simpleProject,test_data_dir):
    # Create v1 tag
    shutil.copyfile(
        src=test_data_dir / "Sample1_ATGTCA_L007_R1_001.fastq",
        dst=simpleProject.m80.thawed_dir / "Sample1_ATGTCA_L007_R1_001.fastq",
    )
    simpleProject.m80.freeze("v1")
    # create v2 tag
    shutil.copyfile(
        src=test_data_dir / "Sample1_ATGTCA_L007_R2_001.fastq",
        dst=simpleProject.m80.thawed_dir / "Sample1_ATGTCA_L007_R2_001.fastq",
    )
    simpleProject.m80.freeze("v2")
    # Delete the file
    import os

    # remove a file
    os.remove(simpleProject.m80.thawed_dir / "Sample1_ATGTCA_L007_R2_001.fastq")
    # add a new file
    shutil.copyfile(
        src=test_data_dir / "Sample2_ATGTCA_L005_R1_001.fastq",
        dst=simpleProject.m80.thawed_dir / "Sample2_ATGTCA_L005_R1_001.fastq",
    )
    # change a file
    simpleProject.m80.col["x"] = np.array([1, 2, 3, 4])
    with pytest.raises(UnsavedChangesInThawedError):
        simpleProject.m80.thaw("v1")
