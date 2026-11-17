#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# https://www.apache.org/licenses/LICENSE-2.0
#

# This file lists the dependencies required for running the geomesa-cassandra command-line tools
# and the GeoServer plugin. These are the runtime dependencies of the Apache Cassandra Java driver 4.x,
# which is not bundled in the binary distribution. Note that the JARs under $CASSANDRA_HOME/lib
# can't be used, as Cassandra ships with an older, incompatible version of the driver.
# Update the versions as required to match the target environment.

driver_install_version="%%cassandra.driver.version.recommended%%"

# gets the dependencies for this module
# args:
#   $1 - current classpath
function dependencies() {
  local classpath="$1"

  local driver_version="$driver_install_version"

  if [[ -n "$classpath" ]]; then
    driver_version="$(get_classpath_version java-driver-core "$classpath" "$driver_version")"
  fi

  # native-protocol, java-driver-shaded-guava and reactive-streams versions correspond to java-driver-core
  declare -a gavs=(
    "com.datastax.oss:java-driver-core:${driver_version}:jar"
    "com.datastax.oss:java-driver-query-builder:${driver_version}:jar"
    "com.datastax.oss:native-protocol:1.5.1:jar"
    "com.datastax.oss:java-driver-shaded-guava:25.1-jre-graal-sub-1:jar"
    "org.reactivestreams:reactive-streams:1.0.3:jar"
    "io.netty:netty-buffer:%%netty.version%%:jar"
    "io.netty:netty-codec-base:%%netty.version%%:jar"
    "io.netty:netty-common:%%netty.version%%:jar"
    "io.netty:netty-handler:%%netty.version%%:jar"
    "io.netty:netty-resolver:%%netty.version%%:jar"
    "io.netty:netty-transport:%%netty.version%%:jar"
    "io.netty:netty-transport-native-unix-common:%%netty.version%%:jar"
    "io.dropwizard.metrics:metrics-core:%%metrics.version%%:jar"
    "com.google.guava:guava:%%guava.version%%:jar"
  )

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

# gets any dependencies that should be removed from the classpath for this module
# args:
#   $1 - current classpath
function exclude_dependencies() {
  echo ""
}
