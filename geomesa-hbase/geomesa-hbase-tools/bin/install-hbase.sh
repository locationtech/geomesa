#!/bin/bash

#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for HBase
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

hbase_version="%%hbase.version.recommended%%"

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}org/apache/hbase/hbase-common/${hbase_version}/hbase-common-${hbase_version}.jar"
  "${base_url}org/apache/hbase/hbase-protocol/${hbase_version}/hbase-protocol-${hbase_version}.jar"
  "${base_url}org/apache/hbase/hbase-client/${hbase_version}/hbase-client-${hbase_version}.jar"
  "${base_url}org/apache/hbase/hbase-server/${hbase_version}/hbase-server-${hbase_version}.jar"
  "${base_url}org/apache/hbase/hbase-hadoop-compat/${hbase_version}/hbase-hadoop-compat-${hbase_version}.jar"
  "${base_url}com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar"
)

downloadUrls "$install_dir" urls[@]
