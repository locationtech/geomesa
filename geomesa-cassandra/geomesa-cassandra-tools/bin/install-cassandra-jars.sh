#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

cassandra_version="%%cassandra.server.version.recommended%%"
driver_version="%%cassandra.driver.version.recommended%%"
netty_version="%%netty.version%%"
metrics_version="%%metrics.version%%"
guava_version="%%cassandra.guava.version%%"

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}org/apache/cassandra/cassandra-all/${cassandra_version}/cassandra-all-${cassandra_version}.jar"
  "${base_url}com/datastax/cassandra/cassandra-driver-core/${driver_version}/cassandra-driver-core-${driver_version}.jar"
  "${base_url}com/datastax/cassandra/cassandra-driver-mapping/${driver_version}/cassandra-driver-mapping-${driver_version}.jar"
  "${base_url}io/netty/netty-all/${netty_version}/netty-all-${netty_version}.jar"
  "${base_url}io/dropwizard/metrics/metrics-core/${metrics_version}/metrics-core-${metrics_version}.jar"
  "${base_url}ch/qos/logback/logback-core/1.1.3/logback-core-1.1.3.jar"
  "${base_url}ch/qos/logback/logback-classic/1.1.3/logback-classic-1.1.3.jar"
)

# if there's already a guava jar (e.g. geoserver) don't install guava to avoid conflicts
if [ -z "$(find -L $install_dir -maxdepth 1 -name 'guava-*' -print -quit)" ]; then
  urls+=("${base_url}com/google/guava/guava/${guava_version}/guava-${guava_version}.jar")
fi

downloadUrls "$install_dir" urls[@]
