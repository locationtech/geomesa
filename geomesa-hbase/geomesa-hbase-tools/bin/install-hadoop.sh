#!/bin/bash

#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for hadoop (for GeoMesa HBase)
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

hadoop_version="%%hadoop.version.recommended%%"
zookeeper_version="%%zookeeper.version.recommended%%"

# this version required for hadoop 2.8, earlier hadoop versions use 3.1.0-incubating
htrace_core_version="4.1.0-incubating"

# These are needed for Hadoop and to work
# These will depend on the specific hadoop  versions
guava_version="%%hbase.guava.version%%"
com_log_version="1.1.3"
netty3_version="3.6.2.Final"
netty4_version="%%netty.version%%"

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}org/apache/zookeeper/zookeeper/${zookeeper_version}/zookeeper-${zookeeper_version}.jar"
  "${base_url}commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar"
  "${base_url}org/apache/hadoop/hadoop-auth/${hadoop_version}/hadoop-auth-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-client/${hadoop_version}/hadoop-client-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-common/${hadoop_version}/hadoop-common-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-hdfs/${hadoop_version}/hadoop-hdfs-${hadoop_version}.jar"
  "${base_url}commons-logging/commons-logging/${com_log_version}/commons-logging-${com_log_version}.jar"
  "${base_url}commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
  "${base_url}commons-io/commons-io/2.5/commons-io-2.5.jar"
  "${base_url}javax/servlet/servlet-api/2.4/servlet-api-2.4.jar"
  "${base_url}io/netty/netty-all/${netty4_version}/netty-all-${netty4_version}.jar"
  "${base_url}io/netty/netty/${netty3_version}/netty-${netty3_version}.jar"
  "${base_url}com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar"
)

# compare the first digit of htrace core version to determine the artifact name
if [[ "${htrace_core_version%%.*}" -lt 4 ]]; then
  urls+=("${base_url}org/apache/htrace/htrace-core/${htrace_core_version}/htrace-core-${htrace_core_version}.jar")
else
  urls+=("${base_url}org/apache/htrace/htrace-core4/${htrace_core_version}/htrace-core4-${htrace_core_version}.jar")
fi

# if there's already a guava jar (e.g. geoserver) don't install guava to avoid conflicts
if [ -z "$(find -L $install_dir -maxdepth 1 -name 'guava-*' -print -quit)" ]; then
  urls+=("${base_url}com/google/guava/guava/${guava_version}/guava-${guava_version}.jar")
fi

downloadUrls "$install_dir" urls[@]
