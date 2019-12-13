#!/usr/bin/env bash

# type and converter example-csv is registered in $GEOMESA_HOME/conf/application.conf
bin/geomesa-hbase ingest -c catalog -s example-csv -C example-csv example.csv
