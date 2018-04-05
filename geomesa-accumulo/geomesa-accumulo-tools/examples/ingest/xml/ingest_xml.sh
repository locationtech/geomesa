#!/usr/bin/env bash

# type and converter example-xml is registered in $GEOMESA_HOME/conf/application.conf
bin/geomesa-accumulo ingest -u user -p password -i inst -z zoo -c catalog -s example-xml -C example-xml example.xml