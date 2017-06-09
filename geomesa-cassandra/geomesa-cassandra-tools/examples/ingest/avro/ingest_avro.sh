#!/usr/bin/env bash

# type and converter example-xml is registered in $GEOMESA_HOME/conf/application.conf
bin/geomesa-cassandra ingest \
    --contact-point 127.0.0.1:9042 \
    --key-space mykeyspace \
    --catalog mycatalog \
    --converter example-avro-no-header \
    --spec example-avro \
    examples/ingest/avro/example_no_header.avro

bin/geomesa-cassandra ingest \
    --contact-point 127.0.0.1:9042 \
    --key-space mykeyspace \
    --catalog mycatalog \
    --converter example-avro-header \
    --spec example-avro \
    examples/ingest/avro/example_with_header.avro
