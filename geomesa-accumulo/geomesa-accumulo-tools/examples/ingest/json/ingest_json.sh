#!/usr/bin/env bash

bin/geomesa-accumulo ingest -u user -p pass -i inst -z zoo -c catalog -s example-json -C example-json example.json

# this uses a file to define the converter
bin/geomesa-accumulo ingest -u user -p pass -i inst -z zoo -c catalog2 -s example-json -C example_multi_line.conf example_multi_line.json

