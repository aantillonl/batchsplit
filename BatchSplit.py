"""BatchSplit.

This module allows to read a large array of records in batches using generators.
The advantage of using a generator is that it does not require to read the 
entire array of records into memory to produce the batches, batches are built 
one by one as they are required.
 

The module currently contains only the the generator BatchSplitFromFile, 
which implements batch spliting from a file on disk as data source.

Todo:
    * Create generators from other datasources. e.g stream, memory array, etc.
"""
import numpy as np
def BatchSplitFromFile(file_path, max_batch_size = 5000000, max_records_per_batch = 500, max_record_size = 1000000):    
    """BatchSplitFromFile
        
        This generator allows reading and generating batches from a file stored 
        on disk. The generator allows to set the limits to the size of the 
        batch, in bytes or in records, as well as a limit to the length of 
        allowed records.
    
        Note:
            The records should be stored one record per line in the file. 
            Batches will always respect the ``max_records_per_batch`` and 
            ``max_batch_size`` limits. Therefore a batch is yielded when it
            reaches either of these limits.
    
        Yields:
            numpy array: batch, as an array of records. A record is a variable length string.
            
        Args:
            file_path: Path to the file where the records are stored.
            max_batch_size: Maximum size of batch allowed in bytes. Default 
            value is 5000000 (5MB).
            
            max_records_per_batch: Maximum records allowed in a batch. Default
            value is 500 records.
            
            max_record_size: Maximum length in bytes of a single records. Records
            over this value are discarded. Default value is 1000000 (1MB)
        
        Example:
            gen = BatchSplit.BatchSplitFromFile()
            
            batch = next(gen)               
        
    """
    # Initialize batch container and counters
    batch = []
    record_counter = 0
    current_batch_size = 0
    discarded_records = 0    
    
    # Try to open the source file. Exception handled below
    try:
        # Open file in "read" mode
        with open(file_path, 'r') as f:
            # Iterate through the file until no data is left
            while True:
                # Read next line and remove the trailing new line char
                current_record = f.readline().rstrip('\n')
                # If there is no more data to read from file end the cycle
                if not current_record:
                    # In case the current batch has something yield the content before breaking
                    if len(batch) > 0 :
                        yield batch
                    break

                # Compute the size in bytes of the current record
                current_record_size = len(current_record.encode('utf-8'))
                
                # Handle cases when reading a new record                
                # Case 1: If the record is larger than the allowed max record size, skipt it.
                if current_record_size > max_record_size:
                    # However a count of skipped records is kept in case it is required later
                    discarded_records += 1
                    continue
            
                # Case 2: If the current record "fits" in the current batch without overflowing the max allowed batch size
                if record_counter < max_records_per_batch and current_batch_size + current_record_size <= max_batch_size:
                    # Append to batch and increase counters
                    batch.append(current_record)
                    record_counter += 1
                    current_batch_size += current_record_size
                
                # Case 3: If the current record overflows the current batch's capacity or the batch is already full
                else:
                    # Yield (return) current batch as is without the current line
                    yield np.array(batch)
                    # Clear batch and then include the current line in the new batch and update counters
                    batch.clear()
                    batch.append(current_record)
                    record_counter = 1
                    current_batch_size = current_record_size   
                    continue
    
    # Handle if file does not exist
    except IOError:
        raise