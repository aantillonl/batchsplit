import unittest
import BatchSplit

# This test units are work in progress
class TestBatchSplit(unittest.TestCase):
    
    # Test for non existing file
    def test_file_not_exist(self):
        self.assertRaises(IOError,next,BatchSplit.BatchSplit('C://not_exist.txt'))
    
    # Test for longer than 5mb record
    def test_long_records(self):
    
    # Test for overflowed batch
    def test_overflow(self):
        
if __name__ == '__main__':
    unittest.main()# -*- coding: utf-8 -*-

