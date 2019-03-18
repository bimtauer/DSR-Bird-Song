from birdsong.data_preparation.management import DatabaseManager
from Testing_signal_noise import Slicer
import pytest
import os

@pytest.fixture(scope='session')
def test_dir(tmpdir_factory):
    return tmpdir_factory.mktemp("target_dir")
    
def test_slicer(test_dir):
    slicer = Slicer(test_dir)
    url = 'https://www.xeno-canto.org/461383/download'
    slicer(('461383', url))
    assert '461383_0.pkl' in os.listdir(test_dir)

def test_inventory(test_dir):
    dbm = DatabaseManager(test_dir)
    dbm.inventory()
    assert dbm.counts == {'461383': 2}
