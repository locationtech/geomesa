#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for hadoop and accumulo
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

accumulo_version="%%accumulo.version.recommended%%"
hadoop_version="%%hadoop.version.recommended%%"
zookeeper_version="%%zookeeper.version.recommended%%"
thrift_version="%%thrift.version%%"

# htrace 3 required for hadoop before 2.8, accumulo up to 1.9.2
# htrace 4 required for hadoop 2.8 and later
# since they have separate package names, should be safe to install both
htrace3_core_version="3.1.0-incubating"
htrace4_core_version="4.1.0-incubating"

# required for hadoop - make sure they correspond to the hadoop installed version
guava_version="%%guava.version%%"
com_log_version="1.1.3"
# required for accumulo
commons_vfs2_version="2.3"

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}org/apache/accumulo/accumulo-core/${accumulo_version}/accumulo-core-${accumulo_version}.jar"
  "${base_url}org/apache/accumulo/accumulo-server-base/${accumulo_version}/accumulo-server-base-${accumulo_version}.jar"
  "${base_url}org/apache/accumulo/accumulo-start/${accumulo_version}/accumulo-start-${accumulo_version}.jar"
  "${base_url}org/apache/thrift/libthrift/${thrift_version}/libthrift-${thrift_version}.jar"
  "${base_url}org/apache/zookeeper/zookeeper/${zookeeper_version}/zookeeper-${zookeeper_version}.jar"
  "${base_url}commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar"
  "${base_url}org/apache/commons/commons-configuration2/2.5/commons-configuration2-2.5.jar"
  "${base_url}org/apache/commons/commons-text/1.6/commons-text-1.6.jar"
  "${base_url}org/apache/hadoop/hadoop-auth/${hadoop_version}/hadoop-auth-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-client/${hadoop_version}/hadoop-client-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-common/${hadoop_version}/hadoop-common-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-hdfs/${hadoop_version}/hadoop-hdfs-${hadoop_version}.jar"
  "${base_url}commons-logging/commons-logging/${com_log_version}/commons-logging-${com_log_version}.jar"
  "${base_url}org/apache/commons/commons-vfs2/${commons_vfs2_version}/commons-vfs2-${commons_vfs2_version}.jar"
  "${base_url}org/apache/htrace/htrace-core/${htrace3_core_version}/htrace-core-${htrace3_core_version}.jar"
  "${base_url}org/apache/htrace/htrace-core4/${htrace4_core_version}/htrace-core4-${htrace4_core_version}.jar"
)

# add accumulo 1.x jars if needed
accumulo_maj_ver="$(expr match "$accumulo_version" '\([0-9][0-9]*\)\.')"
if [[ "$accumulo_maj_ver" -lt 2 ]]; then
  urls+=(
    "${base_url}org/apache/accumulo/accumulo-fate/${accumulo_version}/accumulo-fate-${accumulo_version}.jar"
    "${base_url}org/apache/accumulo/accumulo-trace/${accumulo_version}/accumulo-trace-${accumulo_version}.jar"
  )
fi

# add hadoop 3+ jars if needed
hadoop_maj_ver="$(expr match "$hadoop_version" '\([0-9][0-9]*\)\.')"
if [[ "$hadoop_maj_ver" -ge 3 ]]; then
  urls+=(
    "${base_url}org/apache/hadoop/hadoop-client-api/${hadoop_version}/hadoop-client-api-${hadoop_version}.jar"
    "${base_url}org/apache/hadoop/hadoop-client-runtime/${hadoop_version}/hadoop-client-runtime-${hadoop_version}.jar"
  )
fi

zk_maj_ver="$(expr match "$zookeeper_version" '\([0-9][0-9]*\)\.')"
zk_min_ver="$(expr match "$zookeeper_version" '[0-9][0-9]*\.\([0-9][0-9]*\)')"
zk_bug_ver="$(expr match "$zookeeper_version" '[0-9][0-9]*\.[0-9][0-9]*\.\([0-9][0-9]*\)')"

# compare the version of zookeeper to determine if we need zookeeper-jute (version >= 3.5.5)
if [[ "$zk_maj_ver" -ge 3 && "$zk_min_ver" -ge 5 && "$zk_bug_ver" -ge 5 ]]; then
  urls+=("${base_url}org/apache/zookeeper/zookeeper-jute/$zookeeper_version/zookeeper-jute-$zookeeper_version.jar")
fi

# if there's already a guava jar (e.g. geoserver) don't install guava to avoid conflicts
if [ -z "$(find -L $install_dir -maxdepth 1 -name 'guava-*' -print -quit)" ]; then
  urls+=("${base_url}com/google/guava/guava/${guava_version}/guava-${guava_version}.jar")
fi

downloadUrls "$install_dir" urls[@]

error=$?
if [[ "$error" != "0" ]]; then
  exit $error
elif [[ -f "$install_dir/commons-text-1.4.jar" ]]; then
  # remove older version of commons-text if it's present (i.e. in geoserver)
  read -r -p "Found conflicting JAR $install_dir/commons-text-1.4.jar. Remove it? (y/n) " confirm
  confirm=${confirm,,} # lower-casing
  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    rm -f "$install_dir/commons-text-1.4.jar"
  fi
fi
