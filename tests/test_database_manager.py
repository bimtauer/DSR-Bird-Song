from birdsong.datasets.management import DatabaseManager
import pytest

def test_inventory(tmpdir):
    test_dir = tmpdir.mkdir("subdir")
    test_file = test_dir.join('123_0')
    
    dbm = DatabaseManager(test_dir)
    counts = dbm.inventory()
    assert counts == {'123':1}
