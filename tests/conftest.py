import os
import pytest
from minus80 import Accession
from minus80 import Cohort
from minus80 import Project

#from minus80 import CloudData
from minus80.Tools import *
from minus80.FireBaseCloudData import FireBaseCloudData

@pytest.fixture(scope="module")
def simpleAccession():
    # Create a simple Accession
    return Accession("Sample1", files=["file1.txt", "file2.txt"], type="sample")

@pytest.fixture(scope="module")
def RNAAccession1():
    a = Accession(
        "RNAAccession1",
        files=[
            "./data/Sample1_ATGTCA_L007_R1_001.fastq",
            "./data/Sample1_ATGTCA_L007_R2_001.fastq",
            "./data/Sample1_ATGTCA_L008_R1_001.fastq",
            "./data/Sample1_ATGTCA_L008_R2_001.fastq",
        ],
        type="RNASeq",
    )
    return a


@pytest.fixture(scope="module")
def RNAAccession2():
    a = Accession(
        "RNAAccession2",
        files=[
            "./data/Sample2_ATGTCA_L005_R1_001.fastq",
            "./data/Sample2_ATGTCA_L005_R2_001.fastq",
            "./data/Sample2_ATGTCA_L006_R1_001.fastq",
            "./data/Sample2_ATGTCA_L006_R2_001.fastq",
        ],
        type="RNASeq",
    )
    return a


@pytest.fixture(scope="module")
def RNACohort(RNAAccession1, RNAAccession2):
    delete("Cohort", "RNACohort")
    x = Cohort("RNACohort")
    x.add_accession(RNAAccession1)
    x.add_accession(RNAAccession2)
    yield x
    delete(x.m80.dtype,x.m80.name)

@pytest.fixture(scope="module")
def simpleCohort():
    delete("Cohort", "TestCohort")
    # Create the simple cohort
    a = Accession("Sample1", files=["file1.txt", "file2.txt"], type="WGS")
    b = Accession("Sample2", files=["file1.txt", "file2.txt"], type="WGS")
    c = Accession("Sample3", files=["file1.txt", "file2.txt"], type="CHIP")
    d = Accession("Sample4", files=["file1.txt", "file2.txt"], type="CHIP")

    x = Cohort("TestCohort")
    for acc in [a, b, c, d]:
        x.add_accession(acc)
    yield x
    delete(x.m80.dtype,x.m80.name)




@pytest.fixture(scope="module")
def cloud():
    m80_username = os.environ['MINUS80_USERNAME']
    m80_password = os.environ['MINUS80_PASSWORD']
    # Login
    cloud = FireBaseCloudData()
    cloud.login(m80_username, m80_password)
    return cloud

