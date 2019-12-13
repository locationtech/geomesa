#!/usr/bin/env bash

bin/geomesa-gt ingest --param dbtype=postgis --param host=localhost --param port=5432 --param schema=public --param database=database --param user=postgres --param passwd=postgres -s example-json -C example-json example.json

# this uses a file to define the converter
bin/geomesa-gt ingest --param dbtype=postgis --param host=localhost --param port=5432 --param schema=public --param database=database --param user=postgres --param passwd=postgres -s example-json -C example_multi_line.conf example_multi_line.json

