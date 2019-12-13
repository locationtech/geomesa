#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# Portions Crown Copyright (c) 2017-%%copyright.year%% Dstl
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# Installs a GeoMesa distributed runtime JAR into HDFS and sets up a corresponding Accumulo namespace

usage="usage: ./setup-namespace.sh -u username -n namespace [<options>]"

function help() {
  echo ""
  echo "Installs a GeoMesa distributed runtime JAR into HDFS and sets up a corresponding Accumulo namespace"
  echo ""
  echo "${usage}"
  echo ""
  echo "Required:"
  echo "  -u    Accumulo username for your instance"
  echo "  -n    Accumulo namespace to use"
  echo ""
  echo "Optional:"
  echo "  -p    Accumulo password for the provided username"
  echo "  -t    Login using an existing Kerberos token, else will prompt for password"
  echo "  -g    Path of the GeoMesa distributed runtime JAR, with or without raster support."
  echo "  -d    Directory to create namespace in, default: /accumulo/classpath"
  echo "  -h    HDFS URI e.g. hdfs://localhost:9000"
  echo ""
  exit 0
}

while getopts ":u:p:tn:g:h:d:help" opt; do
  case $opt in
    u)
      ACCUMULO_USER=$OPTARG
      ;;
    p)
      ACCUMULO_PASSWORD=$OPTARG
      ;;
    t)
      USING_TOKEN=1
      ;;
    n)
      ACCUMULO_NAMESPACE=$OPTARG
      ;;
    g)
      GEOMESA_JAR=$OPTARG
      ;;
    d)
      NAMESPACE_DIR=$OPTARG
      ;;
    h)
      HDFS_URI=$OPTARG
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

if [[ -z "$ACCUMULO_USER" ]]; then
    echo "Accumulo username parameter is required: -u" >&2
    ERROR=1
fi

if [[ -z "$ACCUMULO_NAMESPACE" ]]; then
    echo "Accumulo namespace parameter is required: -n" >&2
    ERROR=1
fi

if [[ -z "$GEOMESA_JAR" ]]; then
    # look for distributed runtime jar in dist
    # double dirnames takes the parent of the parent of the script
    # which should be the GM home if this script is in "bin"
    if [ -z "${GEOMESA_ACCUMULO_HOME}" ]; then
        GEOMESA_ACCUMULO_HOME="$(cd "`dirname "$0"`"/..; pwd)"
    fi
    GEOMESA_JAR=$(find -L ${GEOMESA_ACCUMULO_HOME}/dist/accumulo -name "geomesa-accumulo-distributed-runtime*" | grep -v "raster")
    if [[ "x$GEOMESA_JAR" == "x" ]]; then
        echo "Could not find GeoMesa distributed runtime JAR - please specify the JAR using the '-g' flag"
        ERROR=1
    else
        echo "Using GeoMesa JAR: $GEOMESA_JAR"
    fi
fi

if [[ -z "$NAMESPACE_DIR" ]]; then
    NAMESPACE_DIR="/accumulo/classpath"
    echo "Using namespace directory: $NAMESPACE_DIR"
fi

if [[ -z "$HDFS_URI" ]]; then
    HDFS_URI=`hdfs getconf -confKey fs.defaultFS`
fi

if [[ $HDFS_URI == hdfs* ]]; then
    echo "Using HDFS URI: $HDFS_URI"
else
    echo "Invalid HDFS URI discovered: $HDFS_URI"
    ERROR=1
fi

if [[ -z "$ERROR" && -z "$ACCUMULO_PASSWORD" && -z "$USING_TOKEN" ]]; then
    read -s -p "Enter Accumulo password for user $ACCUMULO_USER: " ACCUMULO_PASSWORD
    echo
fi

if [[ -n "$ERROR" ]]; then
    help
    exit 1
fi

echo "Copying GeoMesa JAR for Accumulo namespace $ACCUMULO_NAMESPACE..."
hadoop fs -mkdir -p "${NAMESPACE_DIR}/${ACCUMULO_NAMESPACE}"
hadoop fs -copyFromLocal -f "$GEOMESA_JAR" "${NAMESPACE_DIR}/${ACCUMULO_NAMESPACE}/"

if hadoop fs -ls "${NAMESPACE_DIR}/${ACCUMULO_NAMESPACE}/geomesa*.jar" > /dev/null 2>&1
then
    if [[ -z "$USING_TOKEN" ]]; then
        ACCUMULO_SHELL_CMD="accumulo shell -u $ACCUMULO_USER -p $ACCUMULO_PASSWORD"
    else
        ACCUMULO_SHELL_CMD="accumulo shell -u $ACCUMULO_USER"
    fi
    echo -e "createnamespace ${ACCUMULO_NAMESPACE}\n" \
      "grant NameSpace.CREATE_TABLE -ns ${ACCUMULO_NAMESPACE} -u $ACCUMULO_USER\n" \
      "config -s general.vfs.context.classpath.${ACCUMULO_NAMESPACE}=${HDFS_URI}${NAMESPACE_DIR}/${ACCUMULO_NAMESPACE}/.*.jar\n" \
      "config -ns ${ACCUMULO_NAMESPACE} -s table.classpath.context=${ACCUMULO_NAMESPACE}\n" \
      | ${ACCUMULO_SHELL_CMD}

    if [[ $? -eq 1 ]]; then
        echo "Error encountered executing Accumulo shell commands, check above output for errors."
    else
        echo "Successfully installed GeoMesa distributed runtime JAR."
    fi
else
  echo "No GeoMesa JAR found in HDFS. Please check HDFS (permissions?) and try again."
fi
