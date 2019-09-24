import pytest
import minus80 as m80


def test_start_nuked(simpleCloudData):
    simpleCloudData.nuke()


def test_init(simpleCloudData):
    assert simpleCloudData


def test_bad_engine():
    with pytest.raises(Exception) as e_info:
        m80.CloudData(engine="error")


# ---------------
# Push


def test_push_raw(simpleCloudData):
    simpleCloudData.push(
        dtype="Fastq", name="data/Sample1_ATGTCA_L007_R1_001.fastq", raw=True
    )


def test_push_Cohort(simpleCloudData, simpleCohort):
    simpleCloudData.push(dtype="Cohort", name="TestCohort")


def test_push_missing_Cohort(simpleCloudData, simpleCohort):
    with pytest.raises(Exception) as e_info:
        simpleCloudData.push(dtype="Cohort", name="ERROR")


# ---------------
# List
def test_list(simpleCloudData):
    simpleCloudData.list()


def test_raw_list(simpleCloudData):
    simpleCloudData.list(raw=True)


def test_dtype_list(simpleCloudData):
    simpleCloudData.list(dtype="Cohort", raw=True)


def test_name_list(simpleCloudData):
    simpleCloudData.list(name="RNACohort", raw=True)


# ---------------
# Pull


def test_pull_raw(simpleCloudData):
    simpleCloudData.pull(
        dtype="Fastq",
        name="Sample1_ATGTCA_L007_R1_001.fastq",
        raw=True,
        output="data/Sample1_ATGTCA_L007_R1_001.fastq",
    )


def test_pull_Cohort(simpleCloudData, simpleCohort):
    simpleCloudData.pull(dtype="Cohort", name="TestCohort")


def test_pull_missing_Cohort(simpleCloudData, simpleCohort):
    with pytest.raises(Exception) as e_info:
        simpleCloudData.pull(dtype="Cohort", name="ERROR")


# ---------------
# Remove
def test_remove_Cohort(simpleCloudData):
    simpleCloudData.remove(dtype="Cohort", name="TestCohort")


def test_remove_raw(simpleCloudData):
    simpleCloudData.remove(
        dtype="Fastq", name="data/Sample1_ATGTCA_L007_R1_001.fastq", raw=True
    )


# --------------
# List empty
def test_nuke(simpleCloudData):
    simpleCloudData.nuke()


def test_list_empty(simpleCloudData):
    simpleCloudData.list()
