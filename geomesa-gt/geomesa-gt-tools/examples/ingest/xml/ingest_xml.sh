#!/usr/bin/env bash

# type and converter example-xml is registered in $GEOMESA_HOME/conf/application.conf
bin/geomesa-gt ingest --param dbtype=postgis --param host=localhost --param port=5432 --param schema=public --param database=database --param user=postgres --param passwd=postgres -s example-xml -C example-xml example.xml
