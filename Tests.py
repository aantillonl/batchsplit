import unittest
import numpy.testing as npt
import BatchSplit
import numpy as np

class TestBatchSplit(unittest.TestCase):
    
    # Test for non existing file
    def test_file_not_exist(self):
        self.assertRaises(IOError,next,BatchSplit.BatchSplitFromFile('C://does_not_exist.txt'))
    
    # Test for longer than max record size allowed
    def test_long_records(self):
        # Set test parameters
        max_records_per_batch = 10  
        max_record_size = 10
        
        # The first line of long_line.txt is 26 chars long (26 bytes), which is greater than the max_record_size
        # the rest are elementes named "item_n", n is consecutive
        file_path = 'test_files/long_line.txt'
        
        # The first batch must exclude the first (longer) line and contain items 0 through max_records_per_batch
        expected_output = np.array(['item_' + str(i) for i in range(max_records_per_batch)])
        
        # Instantiate the generator
        gen = BatchSplit.BatchSplitFromFile(
                                file_path = file_path,
                                max_record_size = max_record_size,
                                max_records_per_batch = max_records_per_batch
                            )
        
        # Get the first batch and test
        output = next(gen)
        npt.assert_array_equal(expected_output, output)
                        
    # Test for correct batch split
    def test_split(self):
        # Set test parameters
        num_of_batches = 10
        items_per_batch = 10
        # The file "long_array.txt" contains 10 batches of 10 items each named batch_b_item_i, where b and i are consecutives
        file_path = 'test_files/long_array.txt'
        
        # Instantiate the generator
        gen = BatchSplit.BatchSplitFromFile(
                                file_path = file_path,
                                max_records_per_batch = items_per_batch
                            )
        
        # Generate the expected and actual output
        first_10_expected = []
        first_10_output =[]
        for batch in range(num_of_batches):
            first_10_expected.append(np.array(['batch_' + str(batch) + '_item_' + str(item) for item in range(items_per_batch)]))
            first_10_output.append(next(gen))
        
        # Test first 10 batches
        npt.assert_array_equal(first_10_expected,first_10_output)
        
    # Test for batch overflow
    def test_overflow(self):
        # Batch is 1 item before reaching max_records_per_batch, but the batch has reached max capacity
        # The generator should return (yield) the batch as is, and append the next line into the next batch
        # Set test params
        records_per_batch = 10
        # Thte test file 'overflow_batch.txt' contains the records 'item_n' where n is consecutive
        file_path = 'test_files/overflow_batch.txt'
        # If n is one digit, the length of the "item_n" is 6 bytes. Set the batch size to hold 9 of such items
        max_batch_size = 6 * 9
    
        # Define expected batches: i.e. [item_0, .. item_n-1], [item_n]        
        first_batch_expected = np.array(['item_' + str(i) for i in range(records_per_batch - 1)])
        second_batch_expected = np.array(['item_' + str(records_per_batch - 1)])
        
        # Instantiate generator
        gen = BatchSplit.BatchSplitFromFile(
                                file_path = file_path,
                                max_batch_size = max_batch_size,
                                max_records_per_batch = records_per_batch
                            )
        
        # Test first 2 batches
        npt.assert_array_equal(first_batch_expected, next(gen))
        npt.assert_array_equal(second_batch_expected, next(gen))
    
if __name__ == '__main__':
    unittest.main()