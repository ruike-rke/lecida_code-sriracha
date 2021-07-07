# sriracha: Lecida Learn common utilities

[![CircleCI](https://circleci.com/gh/lecida/sriracha/tree/master.svg?style=svg&circle-token=4cb60b88ea7313cc271d0ebf9b929e04aa5743ef)](https://circleci.com/gh/lecida/sriracha/tree/master)

Sriracha holds common utilities used by OPM and EDA, but can also be used by other repositories.

## Available utilities
* I/O utilities
* Time-series data utilities
* remote file / S3 utilities

## Configure
Run 

```bash
python -m venv env
. env/bin/activate
pip install pip-tools
pip-sync
pip install -e .
sriracha configure
```

to configure a local directory path to which S3 files will be synced when
using sriracha's `s3_to_local` functionality to sync remote directories and
files.

`s3_to_local` allows for the inputs of your scripts and functions to be S3
paths. Whenever you catch yourself committing a local path to a repository,
consider uploading the input files to S3 and using `s3_to_local` instead.
This makes experiments and scripts repeatable and transportable across filesystems.
