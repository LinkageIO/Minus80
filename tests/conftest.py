
import pytest
from minus80 import Accession
from minus80 import Cohort

@pytest.fixture(scope='module')
def simpleAccession():
    # Create a simple Accession
    return Accession('Sample1',files=['file1.txt','file2.txt'],type='sample')


@pytest.fixture(scope='module')
def RNAAccession():
    a = Accession(
        'RNAAccession',
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
def RNACohort(RNAAccession):
    x = Cohort('RNACohort')
    x.add_accession(RNAAccession)
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
