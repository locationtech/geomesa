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

# this version required for hadoop 2.7, later hadoop versions use 4.1.0-incubating
htrace_core_version="3.1.0-incubating"

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}org/apache/hbase/hbase-shaded-client/${hbase_version}/hbase-shaded-client-${hbase_version}.jar"
)

# compare the first digit of htrace core version to determine the artifact name
if [[ "${htrace_core_version%%.*}" -lt 4 ]]; then
  urls+=("${base_url}org/apache/htrace/htrace-core/${htrace_core_version}/htrace-core-${htrace_core_version}.jar")
else
  urls+=("${base_url}org/apache/htrace/htrace-core4/${htrace_core_version}/htrace-core4-${htrace_core_version}.jar")
fi

downloadUrls "$install_dir" urls[@]
