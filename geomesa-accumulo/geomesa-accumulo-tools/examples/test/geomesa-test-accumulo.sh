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

usage="usage: ./geomesa-test-accumulo.sh -d /gm-accumulo/tools/dist.tar.gz  -u user -p password [<OPTIONS>] "

function help() {
  echo ""
  echo "Installs a GeoMesa distributed runtime JAR into HDFS and sets up a corresponding Accumulo namespace"
  echo ""
  echo "${usage}"
  echo ""
  echo "Required:"
  echo "  -d    Path to the hbase tools dist tar"
  echo "  -u    Accumulo username"
  echo "  -p    Accumulo password"
  echo "  -a    Path to ACCUMULO_HOME; provide param or in environment"
  echo "  -h    Path to HADOOP_HOME; provide param or in environment"
  echo ""
  echo "Optional: If these parameters are not provided the respective tests will be skipped."
  echo "  -s    GeoLife file in s3 to test with"
  echo "  -t    Twitter file (json) to test with"
  echo ""
  exit 0
}

#bin/geomesa-test-accumulo.sh -d /opt/devel/geomesa/geomesa-accumulo/geomesa-accumulo-dist/target/geomesa-accumulo_2.11-1.3.2-SNAPSHOT-bin.tar.gz \
# -t /opt/data/twitter/20160831/20160831-143209.json \
# -a /opt/devel/cloud-local-accumulo/accumulo-1.7.2 \
# -u root -p secret \
# -h /opt/devel/cloud-local-accumulo/hadoop-2.7.3

# Arg resolver
while getopts ":d:t:u:p:h:a:s:help" opt; do
  case $opt in
    d)
      TOOLS_DIST_PATH=$OPTARG
      ;;
    t)
      TWITTER_FILE=$OPTARG
      ;;
    u)
      ACC_USER=$OPTARG
      ;;
    p)
      ACC_PASSWORD=$OPTARG
      ;;
    h)
      HADOOP_HOME=$OPTARG
      ;;
    a)
      ACCUMULO_HOME=$OPTARG
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

GM_TOOLS="/tmp/geomesa-accumulo_${SCALAVER}-${GMVER}"
export GEOMESA_ACCUMULO_HOME=${GM_TOOLS}

if [[ ! -d $ACCUMULO_HOME ]]; then
    echo "$ACCUMULO_HOME not valid; ACCUMULO_HOME is required."
    exit 1
elif [[ ! -x ${ACCUMULO_HOME}/bin/accumulo ]]; then
    echo "${ACCUMULO_HOME}/bin/accumulo does not exist or is not executable."
    exit 1
elif [[ -z "$ACC_USER" ]]; then
    echo "Accumulo username is required."
    exit 1
elif [[ -z "$ACC_PASSWORD" ]]; then
    echo "Accumulo password is required."
    exit 1
else
    geomesaIngestCommand="${GM_TOOLS}/bin/geomesa ingest -u ${ACC_USER} -p ${ACC_PASSWORD}"
    geomesaExportCommand="${GM_TOOLS}/bin/geomesa export -u ${ACC_USER} -p ${ACC_PASSWORD}"
fi

if [[ ! -d $ACCUMULO_HOME ]]; then
    echo "$ACCUMULO_HOME not valid; ACCUMULO_HOME is required."
    exit 1
fi

if [[ "$HADOOP_HOME" ]]; then
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
        echo "GeoMesa Accumulo directory $GM_TOOLS does not exist or is not directory"
        exit 1
    fi
fi

function AccumuloRun() {
    ${ACCUMULO_HOME}/bin/accumulo shell -u root -p secret -e "$1"
}

function setupAccumulo()  {
    set -x
    echo "Placing iter in hdfs" && \
    itrdir="/geomesa/iter/${NS}" && \
    itrfile="geomesa-accumulo-distributed-runtime_${SCALAVER}-${GMVER}.jar" && \
    (hadoopCMD fs -test -d $itrdir && hadoopCMD fs -rm -r $itrdir); \
    hadoopCMD fs -mkdir -p $itrdir && \
    hadoopCMD fs -put ${GM}/dist/accumulo/${itrfile} ${itrdir}/${itrfile} && \

    echo "configuring namespaces" && \
    (test "$(AccumuloRun "namespaces" | grep $NS | wc -l)" == "1" && AccumuloRun "deletenamespace ${NS} -f"); \
    AccumuloRun "createnamespace ${NS}" && \
    (test "$(AccumuloRun "config -np" | grep "general.vfs.context.classpath.${NS}" | wc -l)" == "1" && AccumuloRun "config -d general.vfs.context.classpath.${NS}"); \
    AccumuloRun "config -s general.vfs.context.classpath.${NS}=hdfs://localhost:9000${itrdir}/${itrfile}" && \
    AccumuloRun "config -ns ${NS} -s table.classpath.context=${NS}"
    set +x
}

function test_accumulo_vis() {
   echo "Test Accumulo Visibilities"
   $geomesaIngestCommand -c ${CATALOG} -s example-csv -f viscsv  -C example-csv-with-visibilities $GM_TOOLS/examples/ingest/csv/example.csv

   # no auths gets no data
   accumulo shell -u root -p secret -e "setauths -u root -s ''"
   res=$($geomesaExportCommand -c ${CATALOG} -f viscsv | wc -l)
   if [[ "${res}" -ne "1" ]]; then
     echo "error vis should be 1"
     exit 1
   fi

   # no auths gets no data
   accumulo shell -u root -p secret -e "setauths -u root -s user"
   res=$($geomesaExportCommand -c ${CATALOG} -f viscsv | wc -l)
   if [[ "${res}" -ne "3" ]]; then
     echo "error vis should be 3"
     exit 2
   fi

   # no auths gets no data
   accumulo shell -u root -p secret -e "setauths -u root -s user,admin"
   res=$($geomesaExportCommand -c ${CATALOG} -f viscsv | wc -l)
   if [[ "${res}" -ne "4" ]]; then
     echo "error vis should be 4"
     exit 3
   fi
}

CATALOG="${NS}.gmtest1"

# Test Sequence
test_local_ingest
test_export

if [[ $HDFS_ENABLED == 1 ]]; then
  test_hdfs_ingest
  test_hdfs_export
fi

if [[ $TWITTER_ENABLED == 1 ]]; then
  test_twitter
fi

if [[ $S3_ENABLED == 1 ]]; then
    test_s3
fi

test_accumulo_vis

echo "Done, check output for errors"

