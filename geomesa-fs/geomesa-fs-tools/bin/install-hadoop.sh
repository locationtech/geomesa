#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for hadoop
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

hadoop_version="%%hadoop.version.recommended%%"

# htrace 3 required for hadoop before 2.8
# htrace 4 required for hadoop 2.8 and later
# since they have separate package names, should be safe to install both
htrace3_core_version="3.1.0-incubating"
htrace4_core_version="4.1.0-incubating"

# required for hadoop - make sure they correspond to the hadoop installed version
guava_version="%%guava.version%%"
com_log_version="1.1.3"
commons_config_version="1.6"

# this should match the parquet desired version
snappy_version="1.1.1.6"

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}org/apache/hadoop/hadoop-auth/${hadoop_version}/hadoop-auth-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-client/${hadoop_version}/hadoop-client-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-common/${hadoop_version}/hadoop-common-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-hdfs/${hadoop_version}/hadoop-hdfs-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-hdfs-client/${hadoop_version}/hadoop-hdfs-client-${hadoop_version}.jar"
  "${base_url}org/xerial/snappy/snappy-java/${snappy_version}/snappy-java-${snappy_version}.jar"
  "${base_url}commons-configuration/commons-configuration/${commons_config_version}/commons-configuration-${commons_config_version}.jar"
  "${base_url}commons-logging/commons-logging/${com_log_version}/commons-logging-${com_log_version}.jar"
  "${base_url}commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
  "${base_url}com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar"
  "${base_url}commons-io/commons-io/2.5/commons-io-2.5.jar"
  "${base_url}org/apache/htrace/htrace-core/${htrace3_core_version}/htrace-core-${htrace3_core_version}.jar"
  "${base_url}org/apache/htrace/htrace-core4/${htrace4_core_version}/htrace-core4-${htrace4_core_version}.jar"
)

# add hadoop 3+ jars if needed
hadoop_maj_ver="$(expr match "$hadoop_version" '\([0-9][0-9]*\)\.')"
if [[ "$hadoop_maj_ver" -ge 3 ]]; then
  urls+=(
    "${base_url}org/apache/hadoop/hadoop-client-api/${hadoop_version}/hadoop-client-api-${hadoop_version}.jar"
    "${base_url}org/apache/hadoop/hadoop-client-runtime/${hadoop_version}/hadoop-client-runtime-${hadoop_version}.jar"
  )
fi

# if there's already a guava jar (e.g. geoserver) don't install guava to avoid conflicts
if [ -z "$(find -L $install_dir -maxdepth 1 -name 'guava-*' -print -quit)" ]; then
  urls+=("${base_url}com/google/guava/guava/${guava_version}/guava-${guava_version}.jar")
fi

downloadUrls "$install_dir" urls[@]
