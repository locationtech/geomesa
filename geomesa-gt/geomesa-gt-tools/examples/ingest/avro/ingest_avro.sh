#!/usr/bin/env bash

# type and converter example-xml is registered in $GEOMESA_HOME/conf/application.conf
bin/geomesa-gt ingest --param dbtype=postgis --param host=localhost --param port=5432 --param schema=public --param database=database --param user=postgres --param passwd=postgres -s example-avro -C example-avro-no-header example_no_header.avro

bin/geomesa-gt ingest --param dbtype=postgis --param host=localhost --param port=5432 --param schema=public --param database=database --param user=postgres --param passwd=postgres -s example-avro -C example-avro-header example_with_header.avro

