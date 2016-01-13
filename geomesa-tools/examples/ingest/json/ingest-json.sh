#!/usr/bin/env bash

bin/geomesa ingest -u user -p pass -i inst -z zoo -c catalog -s example-json -C example-json example.json

bin/geomesa ingest -u user -p pass -i inst -z zoo -c catalog2 -s example-json -C example-json multi-line.json

