#!/usr/bin/env bash

# spec and converter for example_xml are registered in $GEOMESA_CASSANDRA_HOME/conf/application.conf
bin/geomesa-cassandra ingest \
    --contact-point 127.0.0.1:9042 \
    --key-space mykeyspace \
    --catalog mycatalog \
    --converter example-xml \
    --spec example-xml \
    examples/ingest/json/example.xml
