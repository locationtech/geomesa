#!/usr/bin/env bash

# type and converter example-csv is registered in $GEOMESA_HOME/conf/application.conf
bin/geomesa-accumulo ingest -u user -p password -i inst -z zoo -c catalog -s example-csv -C example-csv example.csv