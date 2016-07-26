#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# Installs a GeoMesa distributed runtime JAR into an Accumulo namespace

while getopts ":u:p:n:g:h:" opt; do
  case $opt in
    u)
      ACCUMULO_USER=$OPTARG
      ;;
    p)
      ACCUMULO_PASSWORD=$OPTARG
      ;;
    n)
      ACCUMULO_NAMESPACE=$OPTARG
      ;;
    g)
      GEOMESA_JAR=$OPTARG
      ;;
    h)
      HDFS_URI=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

if [ -z "$ACCUMULO_USER" ]; then
    echo "Accumulo username parameter is required: -u" >&2
    ERROR=1
fi

if [ -z "$ACCUMULO_NAMESPACE" ]; then
    echo "Accumulo namespace parameter is required: -n" >&2
    ERROR=1
fi

if [ -z "$GEOMESA_JAR" ]; then
    # cd to directory of script and look for GeoMesa JAR
    cd "$( dirname "${BASH_SOURCE[0]}" )"
    GEOMESA_JAR=$(ls | grep geomesa-accumulo-distributed-runtime)
    if [ -z "$GEOMESA_JAR" ]; then
        echo "Null GeoMesa JAR parameter encountered and could not find a GeoMesa distributed runtime JAR"
        ERROR=1
    else
        echo "Null GeoMesa JAR parameter encountered, using $GEOMESA_JAR"
    fi
fi

if [ -z "$HDFS_URI" ]; then
    HDFS_URI=`hdfs getconf -confKey fs.defaultFS`
    echo "Null HDFS URI parameter encountered, using $HDFS_URI"
fi

if [[ -z $ERROR && -z "$ACCUMULO_PASSWORD" ]]; then
    read -s -p "Enter Accumulo Password: " ACCUMULO_PASSWORD
fi

if [ -n "$ERROR" ]; then
    echo -e "\nRequired parameters:\n\t" \
      "-u (Accumulo username)\n\t" \
      "-n (Accumulo namespace)"
    echo -e "Optional parameters:\n\t" \
      "-p (Accumulo password)\n\t" \
      "-g (Path of GeoMesa distributed runtime JAR)\n\t" \
      "-h (HDFS URI e.g. hdfs://localhost:54310)"
    exit 1
fi

echo "Copying GeoMesa JAR for Accumulo namespace $ACCUMULO_NAMESPACE ..."
hadoop fs -mkdir -p /accumulo/classpath/${ACCUMULO_NAMESPACE}
hadoop fs -copyFromLocal -f $GEOMESA_JAR /accumulo/classpath/${ACCUMULO_NAMESPACE}/

if hadoop fs -ls /accumulo/classpath/${ACCUMULO_NAMESPACE}/geomesa*.jar > /dev/null 2>&1
then
    echo -e "createnamespace ${ACCUMULO_NAMESPACE}\n" \
      "grant NameSpace.CREATE_TABLE -ns ${ACCUMULO_NAMESPACE} -u root\n" \
      "config -s general.vfs.context.classpath.${ACCUMULO_NAMESPACE}=${HDFS_URI}/accumulo/classpath/${ACCUMULO_NAMESPACE}/.*.jar\n" \
      "config -ns ${ACCUMULO_NAMESPACE} -s table.classpath.context=${ACCUMULO_NAMESPACE}\n" \
      | accumulo shell -u $ACCUMULO_USER -p $ACCUMULO_PASSWORD
    echo -e "Finished installing GeoMesa distributed runtime JAR."
else
  echo "No GeoMesa JAR found in HDFS. Please check HDFS (permissions?) and try again."
fi