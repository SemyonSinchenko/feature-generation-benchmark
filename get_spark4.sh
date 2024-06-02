#!/usr/bin/bash

wget https://dist.apache.org/repos/dist/dev/spark/v4.0.0-preview1-rc3-bin/pyspark-4.0.0.dev1.tar.gz

tar -xzf pyspark-4.0.0.dev1.tar.gz

cd pyspark-4.0.0.dev1/
pip uninstall pyspark
python setup.py install
