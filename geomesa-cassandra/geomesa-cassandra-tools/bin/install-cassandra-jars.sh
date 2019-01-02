#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

cassandra_version=3.0.11
driver_version=3.0.0

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
  "${base_url}io/netty/netty-all/4.0.33.Final/netty-all-4.0.33.Final.jar"
  "${base_url}io/dropwizard/metrics/metrics-core/3.1.2/metrics-core-3.1.2.jar"
)

downloadUrls "$install_dir" urls[@]
