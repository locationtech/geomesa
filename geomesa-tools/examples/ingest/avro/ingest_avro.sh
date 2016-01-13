#!/usr/bin/env bash

# type and converter example-xml is registered in $GEOMESA_HOME/conf/application.conf
geomesa ingest -u user -p password -i inst -z zoo -c catalog -s example-avro -C example-avro-noheader no-header.avro

geomesa ingest -u user -p password -i inst -z zoo -c catalog -s example-avro -C example-avro-header with-header.avro