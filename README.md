# pyspark-core-utils

This small library gives you out of the box a ready to use PySpark application with the following features:

- PySpark session config built from provided configuration
- Configuration is loaded by configurations file and command line
- Supports multi environment
- Provides a Logging solution the app
- Provides multi steps for organising your steps (`init`, `run`, `cleanup`) 

## How to use

All the features are accessible simply extending the SparkApp class:

```
from pyspark_core_utils.apps import SparkApp

class Job(SparkApp):
    def init(self):
        pass

    def run(self):
        pass
    
    def cleanup(self):
        pass

def main():
    Job(__package__).execute()
```

## How to test
To run the tests, first install Python 3.7 with Homebrew (`make install-python`), then run the tests with `make test`. If you're not using Homebrew you can override the `PYTHON` variable as such: `make test PYTHON=/path/to/python3`.

## How to create a new release
Merge the required changes to main. Then create a new release in github to be able to refer to it. The release version is referenced in the data-pipeline-template.


## How to contribute

If you want to contribute, please submit a Pull Request! Thank you :)
