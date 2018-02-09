# BatchSplit

A brief implementation of a library to read large datasets and split them into batches to be later consumed by another application. 
For example moved to a Kinesis stream.

The library uses a generator to lazily iterate through the dataset and *yield* the next batch as they are requested. This approach is
different from reading all the data and building all the batches at once. However the benefit is that it is more robust to overflows since 
not all the data is loaded at once. Similarily, most common cases would consume only one batch at the time.

All the parameters are declared at the beginning of the generator function for quick visualization and editing.
