#!/usr/bin/env bash

# type and converter example-xml is registered in $GEOMESA_HOME/conf/application.conf
bin/geomesa-kudu ingest -c catalog -s example-avro -C example-avro-no-header example_no_header.avro

bin/geomesa-kudu ingest -c catalog -s example-avro -C example-avro-header example_with_header.avro
