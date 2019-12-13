#!/usr/bin/env bash
#
# Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

if [ -z "$GEOMESA_TEST_HOME" ]; then
  export GEOMESA_TEST_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

GMVER="%%geomesa.devel.version%%"
VER_SHORT="$(echo $GMVER | sed 's/[\.\-]\|SNAPSHOT//g')" # "130m3"
SCALAVER="%%scala.binary.version%%"
GMTMP="geomesa-test-tmp"
NS="gmtest${VER_SHORT}"

function test_local_ingest() {
    echo "Test Local Ingest"
    set -x
    $geomesaIngestCommand -c ${CATALOG} -s example-csv  -C example-csv                                      $GM_TOOLS/examples/ingest/csv/example.csv
    $geomesaIngestCommand -c ${CATALOG} -s example-json -C example-json                                     $GM_TOOLS/examples/ingest/json/example.json
    $geomesaIngestCommand -c ${CATALOG} -s example-json -C $GM_TOOLS/examples/ingest/json/example_multi_line.conf $GM_TOOLS/examples/ingest/json/example_multi_line.json
    $geomesaIngestCommand -c ${CATALOG} -s example-xml  -C example-xml                                      $GM_TOOLS/examples/ingest/xml/example.xml
    $geomesaIngestCommand -c ${CATALOG} -s example-xml  -C example-xml-multi                                $GM_TOOLS/examples/ingest/xml/example_multi_line.xml
    $geomesaIngestCommand -c ${CATALOG} -s example-avro -C example-avro-no-header                           $GM_TOOLS/examples/ingest/avro/example_no_header.avro
    #$geomesaIngest -c ${CATALOG} -s example-avro -C example-avro-header                             $GM_TOOLS/examples/ingest/avro/with_header.avro
    set +x
}

function test_hdfs_ingest() {
    echo "Test HDFS Ingest"
    set -x
    # Check hadoop is available
    if [[ `which hadoopCMD` ]]; then
        hadoopCMD fs -ls /user/$(whoami)

        hadoopCMD fs -put $GM_TOOLS/examples/ingest/csv/example.csv
        hadoopCMD fs -put $GM_TOOLS/examples/ingest/json/example.json
        hadoopCMD fs -put $GM_TOOLS/examples/ingest/json/example_multi_line.json
        hadoopCMD fs -put $GM_TOOLS/examples/ingest/xml/example.xml
        hadoopCMD fs -put $GM_TOOLS/examples/ingest/xml/example_multi_line.xml
        hadoopCMD fs -put $GM_TOOLS/examples/ingest/avro/example_no_header.avro
        #hadoopCMD fs -put $GM_TOOLS/examples/ingest/avro/with_header.avro

        $geomesaIngestCommand -c ${CATALOG} -s example-csv  -C example-csv                                      hdfs://localhost:9000/user/$(whoami)/example.csv
        $geomesaIngestCommand -c ${CATALOG} -s example-json -C example-json                                     hdfs://localhost:9000/user/$(whoami)/example.json
        $geomesaIngestCommand -c ${CATALOG} -s example-json -C $GM_TOOLS/examples/ingest/json/example_multi_line.conf hdfs://localhost:9000/user/$(whoami)/example_multi_line.json
        $geomesaIngestCommand -c ${CATALOG} -s example-xml  -C example-xml                                      hdfs://localhost:9000/user/$(whoami)/example.xml
        $geomesaIngestCommand -c ${CATALOG} -s example-xml  -C example-xml-multi                                hdfs://localhost:9000/user/$(whoami)/example_multi_line.xml
        $geomesaIngestCommand -c ${CATALOG} -s example-avro -C example-avro-no-header                           hdfs://localhost:9000/user/$(whoami)/example_no_header.avro
        #$geomesaIngestCommand -c ${CATALOG} -s example-avro -C example-avro-header                             hdfs://localhost:9000/user/$(whoami)/with_header.avro
    else
        ERROR=1
    fi
    set +x
}

function test_s3() {
    echo "Test S3"
    set -x
    # uses gmdata jar in tools lib
    # s3n
    $geomesaIngestCommand -c ${CATALOG} -s geolife -C geolife s3n://ahulbert-test/geolife/Data/000/Trajectory/20081023025304.plt s3n://ahulbert-test/geolife/Data/000/Trajectory/20081024020959.plt
    # s3a
    $geomesaIngestCommand -c ${CATALOG} -s geolife -C geolife s3a://ahulbert-test/geolife/Data/000/Trajectory/20081023025304.plt s3a://ahulbert-test/geolife/Data/000/Trajectory/20081024020959.plt
    set +x
}

function differ() {
  local f=$1
  echo "diff $f"
  diff "${target}" "$f"
  echo "done"
}

function test_export() {
    echo "Test Export"
    if [[ -d "$GMTMP" ]]; then
      rm "/tmp/${GMTMP}" -rf
    fi

    t="/tmp/${GMTMP}"
    if [[ ! -d $t ]]; then
      mkdir $t
    fi

    # Export some formats
    $geomesaExportCommand -c ${CATALOG} -f example-csv -F csv   > "$t/e.csv"
    $geomesaExportCommand -c ${CATALOG} -f example-csv -F avro  > "$t/e.avro"
    $geomesaExportCommand -c ${CATALOG} -f example-csv -F tsv   > "$t/e.tsv"
    $geomesaExportCommand -c ${CATALOG} -f example-csv -F arrow > "$t/e.arrow"

    # Reingest automatically those formats
    $geomesaIngestCommand -c ${CATALOG} -f re-avro "$t/e.avro"
    $geomesaIngestCommand -c ${CATALOG} -f re-csv  "$t/e.csv"
    $geomesaIngestCommand -c ${CATALOG} -f re-tsv  "$t/e.tsv"

    # compare output of reimported tsv,csv,avro to standard export
    $geomesaExportCommand -c ${CATALOG} -f re-avro -F csv  | sort > "$t/re.avro.export"
    $geomesaExportCommand -c ${CATALOG} -f re-csv  -F csv  | sort > "$t/re.csv.export"
    $geomesaExportCommand -c ${CATALOG} -f re-tsv  -F csv  | sort > "$t/re.tsv.export"

    target="$t/e.csv.sorted"
    cat "$t/e.csv" | sort > "$target"

    differ "$t/re.avro.export"
    differ "$t/re.csv.export"
    differ "$t/re.tsv.export"
}

function test_hdfs_export() {
    echo "Test HDFS Export"
    set -x

    if [[ `which hadoop` ]]; then
        if [[ -d "$GMTMP" ]]; then
          rm "/tmp/${GMTMP}" -rf
        fi

        t="/tmp/${GMTMP}"
        if [[ ! -d $t ]]; then
          mkdir $t
        fi

        # Export some formats
        $geomesaExportCommand -c ${CATALOG} -f example-csv -F csv  > "$t/e.csv"
        $geomesaExportCommand -c ${CATALOG} -f example-csv -F avro > "$t/e.avro"
        $geomesaExportCommand -c ${CATALOG} -f example-csv -F tsv  > "$t/e.tsv"

        hadoopCMD fs -put "$t/e.avro"
        hadoopCMD fs -put "$t/e.csv"
        hadoopCMD fs -put "$t/e.tsv"

        $geomesaIngestCommand -c ${CATALOG} -f re-avro-hdfs hdfs://localhost:9000/user/$(whoami)/e.avro
        $geomesaIngestCommand -c ${CATALOG} -f re-csv-hdfs  hdfs://localhost:9000/user/$(whoami)/e.csv
        $geomesaIngestCommand -c ${CATALOG} -f re-tsv-hdfs  hdfs://localhost:9000/user/$(whoami)/e.tsv

        # compare output of reimported tsv,csv,avro to standard export
        $geomesaExportCommand -c ${CATALOG} -f re-avro-hdfs -F csv | sort > "$t/re.avro.hdfs.export"
        $geomesaExportCommand -c ${CATALOG} -f re-csv-hdfs  -F csv | sort > "$t/re.csv.hdfs.export"
        $geomesaExportCommand -c ${CATALOG} -f re-tsv-hdfs  -F csv | sort > "$t/re.tsv.hdfs.export"

        target="$t/e.csv.sorted"
        cat "$t/e.csv" | sort > "$target"

        differ "$t/re.avro.hdfs.export"
        differ "$t/re.csv.hdfs.export"
        differ "$t/re.tsv.hdfs.export"
    else
        ERROR=1
    fi
    set +x
}

function test_twitter() {
    echo "Test Twitter"
    # number of expected records to ingest correctly
    expected=97045 && \
    $geomesaIngestCommand -c ${CATALOG} -s twitter -C twitter-place-centroid ${TWITTER_FILE} && \
    $geomesaIngestCommand -c ${CATALOG} -s twitter-polygon -C twitter-polygon ${TWITTER_FILE} && \
    c1=$($geomesaExportCommand -c ${CATALOG} -f twitter -a user_id --no-header | wc -l) && \
    c2=$($geomesaExportCommand -c ${CATALOG} -f twitter-polygon -a user_id --no-header | wc -l) && \
    test $c1 -eq $c2 && \
    test $c1 -eq $expected
}
