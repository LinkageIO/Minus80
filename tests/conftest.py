
import pytest
from minus80 import Accession
from minus80 import Accessions

@pytest.fixture(scope='module')
def simpleAccession():
    # Create a simple Accession
    return Accession('Sample1',files=['file1.txt','file2.txt'],type='sample')
