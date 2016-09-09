#!/usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for hadoop and accumulo
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

accumulo_version="${accumulo.version.recommended}"
hadoop_version="${hadoop.version.recommended}"
zookeeper_version="${zookeeper.version.recommended}"
thrift_version="${thrift.version}"

# for hadoop 2.5 and 2.6 to work we need these
guava_version="11.0.2"
com_log_version="1.1.3"
commons_vfs2_version="2.0"

# for Accumulo 1.7+ to work we also need the following
if [[ "$accumulo_version" == "1.7"* ]]; then
  htrace_core_version="3.1.0-incubating"
  commons_vfs2_version="2.1"
fi

base_url="https://search.maven.org/remotecontent?filepath="

if [[ (-z "$1") ]]; then
  echo "Error: Provide one arg which is the target directory (e.g. /opt/jboss/standalone/deployments/geoserver.war/WEB-INF/lib)"
  exit
else
  install_dir=$1
  NL=$'\n'
  read -r -p "Install accumulo and hadoop dependencies to ${install_dir}?${NL}Confirm? [Y/n]" confirm
  confirm=${confirm,,} #lowercasing
  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    # get stuff
    declare -a urls=(
      "${base_url}org/apache/accumulo/accumulo-core/${accumulo_version}/accumulo-core-${accumulo_version}.jar"
      "${base_url}org/apache/accumulo/accumulo-fate/${accumulo_version}/accumulo-fate-${accumulo_version}.jar"
      "${base_url}org/apache/accumulo/accumulo-trace/${accumulo_version}/accumulo-trace-${accumulo_version}.jar"
      "${base_url}org/apache/accumulo/accumulo-server-base/${accumulo_version}/accumulo-server-base-${accumulo_version}.jar"
      "${base_url}org/apache/accumulo/accumulo-start/${accumulo_version}/accumulo-start-${accumulo_version}.jar"
      "${base_url}org/apache/thrift/libthrift/${thrift_version}/libthrift-${thrift_version}.jar"
      "${base_url}org/apache/zookeeper/zookeeper/${zookeeper_version}/zookeeper-${zookeeper_version}.jar"
      "${base_url}commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar"
      "${base_url}org/apache/hadoop/hadoop-auth/${hadoop_version}/hadoop-auth-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-client/${hadoop_version}/hadoop-client-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-common/${hadoop_version}/hadoop-common-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-hdfs/${hadoop_version}/hadoop-hdfs-${hadoop_version}.jar"
      "${base_url}commons-logging/commons-logging/${com_log_version}/commons-logging-${com_log_version}.jar"
      "${base_url}com/google/guava/guava/${guava_version}/guava-${guava_version}.jar"
      "${base_url}org/apache/commons/commons-vfs2/${commons_vfs2_version}/commons-vfs2-${commons_vfs2_version}.jar"
    )

    if [[ "$accumulo_version" == "1.7"* ]]; then
      urls=("${urls[@]}" "${base_url}org/apache/htrace/htrace-core/${htrace_core_version}/htrace-core-${htrace_core_version}.jar")
    fi

    for x in "${urls[@]}"; do
      fname=$(basename "$x");
      echo "fetching ${x}";
      wget -O "${1}/${fname}" "$x" || { rm -f "${1}/${fname}"; echo "Error downloading dependency: ${fname}"; \
        errorList="${errorList} ${x} ${NL}"; };
    done
    if [[ -n "${errorList}" ]]; then
      echo "Failed to download: ${NL} ${errorList}";
    fi
  else
    echo "Installation cancelled"
  fi
fi
