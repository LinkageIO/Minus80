
import pytest
from minus80 import Accession
from minus80 import Cohort

@pytest.fixture(scope='module')
def simpleAccession():
    # Create a simple Accession
    return Accession('Sample1',files=['file1.txt','file2.txt'],type='sample')


@pytest.fixture(scope='module')
def RNAAccession1():
    a = Accession(
        'RNAAccession1',
        files = [
            './data/Sample1_ATGTCA_L007_R1_001.fastq',
            './data/Sample1_ATGTCA_L007_R2_001.fastq',
            './data/Sample1_ATGTCA_L008_R1_001.fastq',
            './data/Sample1_ATGTCA_L008_R2_001.fastq',
        ],
        type='RNASeq'
    )
    return a

@pytest.fixture(scope='module')
def RNAAccession2():
    a = Accession(
        'RNAAccession2',
        files = [
            './data/Sample2_ATGTCA_L005_R1_001.fastq',
            './data/Sample2_ATGTCA_L005_R2_001.fastq',
            './data/Sample2_ATGTCA_L006_R1_001.fastq',
            './data/Sample2_ATGTCA_L006_R2_001.fastq',
        ],
        type='RNASeq'
    )
    return a


@pytest.fixture(scope='module')
def RNACohort(RNAAccession1,RNAAccession2):
    x = Cohort('RNACohort')
    x.add_accession(RNAAccession1)
    x.add_accession(RNAAccession2)
    return x

@pytest.fixture(scope='module')
def simpleCohort():
    # Create the simple 
    a = Accession('Sample1',files=['file1.txt','file2.txt'],type='WGS')
    b = Accession('Sample2',files=['file1.txt','file2.txt'],type='WGS')
    c = Accession('Sample3',files=['file1.txt','file2.txt'],type='CHIP')
    d = Accession('Sample4',files=['file1.txt','file2.txt'],type='CHIP')

    x = Cohort('TestCohort')
    [x.add_accession(_) for _ in [a,b,c,d]]
    return x
