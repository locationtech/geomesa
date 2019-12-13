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

usage="usage: ./geomesa-test-hbase.sh -d /gm-hbase/tools/dist.tar.gz -b /path/to/hbase/ [<OPTIONS>] "

function help() {
  echo ""
  echo "Installs a GeoMesa distributed runtime JAR into HDFS and sets up a corresponding Accumulo namespace"
  echo ""
  echo "${usage}"
  echo ""
  echo "Required:"
  echo "  -d    Path to the hbase tools dist tar"
  echo "  -b    Path to HBASE_HOME; provide param or in environment"
  echo ""
  echo "Optional: If these parameters are not provided the respective tests will be skipped."
  echo "  -h    Path to HADOOP_HOME; provide param or in environment"
  echo "  -s    GeoLife file in s3 to test with"
  echo "  -t    Twitter file (json) to test with"
  echo ""
  exit 0
}

#bin/geomesa-test-hbase.sh -d /opt/devel/geomesa/geomesa-hbase/geomesa-hbase-dist/target/geomesa-hbase_2.11-1.3.2-SNAPSHOT-bin.tar.gz \
# -t /opt/data/twitter/20160831/20160831-143209.json \
# -b /opt/devel/hbase-standalone/hbase-1.3.1

# Arg resolver
while getopts ":d:t:h:b:s:help" opt; do
  case $opt in
    d)
      TOOLS_DIST_PATH=$OPTARG
      ;;
    t)
      TWITTER_FILE=$OPTARG
      ;;
    h)
      export HADOOP_HOME=$OPTARG
      ;;
    b)
      export HBASE_HOME=$OPTARG
      ;;
    s)
      S3URL=$OPTARG
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

GM_TOOLS="/tmp/geomesa-hbase_${SCALAVER}-${GMVER}"
geomesaIngestCommand="${GM_TOOLS}/bin/geomesa-hbase ingest"
geomesaExportCommand="${GM_TOOLS}/bin/geomesa-hbase export"
export GEOMESA_HBASE_HOME=${GM_TOOLS}

if [[ ! -d $HBASE_HOME ]]; then
    echo "$HBASE_HOME not valid; HBASE_HOME is required."
    exit 1
elif [[ ! -x ${HBASE_HOME}/bin/hbase ]]; then
    echo "${HBASE_HOME}/bin/hbase does not exist or is not executable."
    exit 1
fi

if [[ -n "$HADOOP_HOME" ]]; then
    if [[ ! -d $HADOOP_HOME ]]; then
        echo "$HADOOP_HOME not valid; HADOOP_HOME is required."
        exit 1
    elif [[ ! -x ${HADOOP_HOME}/bin/hadoop ]]; then
        echo "${HADOOP_HOME}/bin/hadoop does not exist or is not executable."
        exit 1
    else
        hadoopCMD="${HADOOP_HOME}/bin/hadoop"
        HADOOP_ENABLED=1
    fi
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

if [[ "$S3URL" ]]; then
    S3_ENABLED=1
else
    S3_ENABLED=0
fi

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


function HBaseRun() {
    ${HBASE_HOME}/bin/hbase shell hbaseCMDs.txt
}

function test_hbase_vis() {
    # TODO
    echo "TODO"
}

CATALOG="${NS}.gmtest1"

# Test Sequence
test_local_ingest
test_export

if [[ "$HDFS_ENABLED" == 1 ]]; then
    test_hdfs_ingest
    test_hdfs_export
fi

if [[ $TWITTER_ENABLED == 1 ]]; then
    test_twitter
fi

if [[ $S3_ENABLED == 1 ]]; then
    test_s3
fi

test_hbase_vis

echo "Done, check output for errors"
