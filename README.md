# BatchSplit

A brief implementation of a library to read large datasets and split them into batches to be later consumed by another application. 
For example moved to a Kinesis stream.

The library is a Python module which currently contains only a generator to read data from a file on disk. Specific details about the implementation are available as docstrings on the BatchSplit source code. Docstrings are the recommended method to document Python modules, more info in [this link](https://www.python.org/dev/peps/pep-0257/)

Future developments could include read from stream, cloud storage or other types of data sources.

## Usage
```
import BatchSplit

gen = BatchSplit.BatchSplitFromFile()
batch = next(gen)
```
