#!/usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#
zookeeper_version="%%zookeeper.version.recommended%%"

# kafka versions
guava_version="%%guava.version%%"
kafka_version="%%kafka.version%%"
zkclient_version="%%zkclient.version%%"
jopt_version="%%kafka.jopt.version%%"

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}org/apache/kafka/kafka_2.11/$kafka_version/kafka_2.11-$kafka_version.jar"
  "${base_url}org/apache/kafka/kafka-clients/$kafka_version/kafka-clients-$kafka_version.jar"
  "${base_url}org/apache/zookeeper/zookeeper/$zookeeper_version/zookeeper-$zookeeper_version.jar"
  "${base_url}com/101tec/zkclient/$zkclient_version/zkclient-$zkclient_version.jar"
  "${base_url}net/sf/jopt-simple/jopt-simple/$jopt_version/jopt-simple-$jopt_version.jar"
  "${base_url}com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar"
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
