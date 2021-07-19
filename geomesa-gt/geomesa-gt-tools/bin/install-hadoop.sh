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

hadoop_version="%%hadoop.version.recommended%%"
zookeeper_version="%%zookeeper.version.recommended%%"

# accumulo up to 1.9.2 are using this
# it's possible hadoop uses it too ... def 2.7.x does
htrace3_core_version="3.1.0-incubating"

# this version required for hadoop 2.8 and older but has separate package names
# so we install it should be safe.
htrace4_core_version="4.1.0-incubating"

# for hadoop 2.5 and 2.6 to work we need these
guava_version="%%guava.version%%"
com_log_version="1.1.3"

# Load common functions and setup
if [ -z "${%%tools.dist.name%%_HOME}" ]; then
  export %%tools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%tools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%tools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar"
  "${base_url}org/apache/hadoop/hadoop-auth/${hadoop_version}/hadoop-auth-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-common/${hadoop_version}/hadoop-common-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-hdfs/${hadoop_version}/hadoop-hdfs-${hadoop_version}.jar"
  "${base_url}commons-logging/commons-logging/${com_log_version}/commons-logging-${com_log_version}.jar"
  "${base_url}org/apache/htrace/htrace-core/${htrace3_core_version}/htrace-core-${htrace3_core_version}.jar"
  "${base_url}org/apache/htrace/htrace-core4/${htrace4_core_version}/htrace-core4-${htrace4_core_version}.jar"
  "${base_url}org/apache/zookeeper/zookeeper/${zookeeper_version}/zookeeper-${zookeeper_version}.jar"
)

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
