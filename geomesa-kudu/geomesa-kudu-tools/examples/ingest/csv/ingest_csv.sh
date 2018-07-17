#!/usr/bin/env bash

# type and converter example-csv is registered in $GEOMESA_HOME/conf/application.conf
bin/geomesa-kudu ingest -c catalog -s example-csv -C example-csv example.csv
