# BatchSplit module
#################################################################
# Description
# Generator that splits an array of records into an array of batches of records
# Batches are available calling next(gen). Where gen is an instance of BatchSplit
#################################################################
# Input
# The array of records is located on disk
# The records are strings of variable length, one per line.
# The records contain only ascii characters
#################################################################

def BatchSplit(file_path):    
    # Define maximum values allowed. batch and record size is in bytes
    max_batch_size = 5000000
    max_records_per_batch = 500
    max_record_size = 1000000
    
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
                # Read next line
                current_record = f.readline()
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
                if current_batch_size + current_record_size <= max_batch_size:
                    # Append to batch and increase counters
                    batch.append(current_record)
                    record_counter += 1
                    current_batch_size += current_record_size
                
                # Case 3: If the current record overflows the current batch's capacity
                else:
                    # Yield (return) current batch as is without the current line
                    yield batch
                    # Clear batch and then include the current line in the new batch and update counters
                    batch.clear()
                    batch.append(current_record)
                    record_counter = 1
                    current_batch_size = current_record_size                    
            
                # When the current batch is full in capacity or record count
                if record_counter >= max_records_per_batch or current_batch_size == max_batch_size:
                    yield batch
                    batch.clear()
                    record_counter = 0
                  
    
    # Handle if file does not exist
    except IOError:
        raise