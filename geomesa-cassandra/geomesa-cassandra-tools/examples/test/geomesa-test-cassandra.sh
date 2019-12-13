#!/usr/bin/env bash
#
# Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# Resolve test home
if [[ -z "${GEOMESA_TEST_HOME}" ]]; then
  export GEOMESA_TEST_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# Load common functions and setup
. "${GEOMESA_TEST_HOME}"/bin/common-functions.sh

usage="usage: ./geomesa-test-cassandra.sh -d /gm-cassandra/tools/dist.tar.gz -b /path/to/cassandra/ [<OPTIONS>] "

function help() {
  echo ""
  echo "Installs a GeoMesa distributed runtime JAR into HDFS and sets up a corresponding Accumulo namespace"
  echo ""
  echo "${usage}"
  echo ""
  echo "Required:"
  echo "  -d    Path to the cassandra tools dist tar"
  echo "  -b    Path to CASSANDRA_HOME; provide param or in environment"
  echo "  -p    Cassandra contact point (address of a Cassandra node). Must be IP:Port eg: 127.0.0.1:9042"
  echo ""
  echo "Optional: If these parameters are not provided the respective tests will be skipped."
  echo "  -k    Keyspace to ingest test tables into. Default: gmtest"
  echo "  -t    Twitter file (json) to test with"
  echo ""
  exit 0
}

#bin/geomesa-test-cassandra.sh \
# -d /opt/devel/geomesa/geomesa-cassandra/geomesa-cassandra-dist/target/geomesa-cassandra_2.11-1.3.2-SNAPSHOT-bin.tar.gz \
# -b /opt/devel/cassandra-standalone/cassandra-3.10/ \
# -t /opt/data/twitter/20160831/20160831-143209.json \
# -p localhost

# Arg resolver
while getopts ":d:b:p:k:t:help" opt; do
  case $opt in
    d)
      TOOLS_DIST_PATH=$OPTARG
      ;;
    b)
      export CASSANDRA_HOME=$OPTARG
      ;;
    p)
      CONTACT_POINT=$OPTARG
      ;;
    k)
      KEYSPACE=$OPTARG
      ;;
    t)
      TWITTER_FILE=$OPTARG
      ;;
    help)
      help
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      echo "${usage}"
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      echo "${usage}"
      exit 1
      ;;
  esac
done

if [[ -z "${KEYSPACE}" ]]; then
    KEYSPACE="gmtest"
fi

if [[ -z "${CONTACT_POINT}" ]]; then
    echo "Contact point (-p) is required."
    exit 1
fi


if [[ ! -d $CASSANDRA_HOME ]]; then
    echo "$CASSANDRA_HOME not valid; CASSANDRA_HOME is required."
    exit 1
elif [[ ! -x ${CASSANDRA_HOME}/bin/cassandra ]]; then
    echo "${CASSANDRA_HOME}/bin/cqlsh does not exist or is not executable."
    exit 1
fi

if [[ "$TWITTER_FILE" ]]; then
    if [[ -r "$TWITTER_FILE" ]]; then
        TWITTER_ENABLED=1
    else
        echo "Can't find or read twitter file $TWITTER_FILE"
        exit -1
    fi
else
    TWITTER_ENABLED=0
fi

GM_TOOLS="/tmp/geomesa-cassandra_${SCALAVER}-${GMVER}"
geomesaIngestCommand="${GM_TOOLS}/bin/geomesa-cassandra ingest -k ${KEYSPACE} -P ${CONTACT_POINT}"
geomesaExportCommand="${GM_TOOLS}/bin/geomesa-cassandra export -k ${KEYSPACE} -P ${CONTACT_POINT}"
export GEOMESA_CASSANDRA_HOME=${GM_TOOLS}

# Deploy tools
if [[ ! -r $TOOLS_DIST_PATH ]]; then
    echo "Cannot find or read tools dists"
    exit 1
else
    if [[ -d $GM_TOOLS ]]; then
      rm -rf $GM_TOOLS
    fi
    cp $TOOLS_DIST_PATH /tmp/
    TOOLS_NAME="$(basename $TOOLS_DIST_PATH)"
    pushd /tmp/
    tar -xf "/tmp/${TOOLS_NAME}"
    popd
    if [[ ! -d $GM_TOOLS ]]; then
        echo "GeoMesa HBase directory $GM_TOOLS does not exist or is not directory"
        exit 1
    fi
fi

function CassandraRun() {
    ${CASSANDRA_HOME}/bin/cqlsh -e "$@"
}

function test_cassandra_vis() {
    # TODO
    echo "TODO"
}

CATALOG="gmtest1"
# Make the keyspace
CassandraRun "CREATE KEYSPACE ${KEYSPACE} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};"

# Test Sequence
test_local_ingest
test_export

if [[ $TWITTER_ENABLED == 1 ]]; then
    test_twitter
fi

test_cassandra_vis

echo "Done, check output for errors"
