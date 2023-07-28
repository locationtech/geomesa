#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for running the geomesa-cassandra command-line tools.
# Usually these dependencies will be provided by the environment (e.g. CASSANDRA_HOME).
# Update the versions as required to match the target environment.

cassandra_install_version="%%cassandra.server.version.recommended%%"
driver_install_version="%%cassandra.driver.version.recommended%%"

# gets the dependencies for this module
# args:
#   $1 - current classpath
function dependencies() {
  local classpath="$1"

  local cassandra_version="$cassandra_install_version"
  local driver_version="$driver_install_version"

  if [[ -n "$classpath" ]]; then
    # check for both cassandra-all and apache-cassandra
    cassandra_version="$(get_classpath_version cassandra-all "$classpath" "$cassandra_version")"
    cassandra_version="$(get_classpath_version apache-cassandra "$classpath" "$cassandra_version")"
    driver_version="$(get_classpath_version cassandra-driver-core "$classpath" "$driver_version")"
  fi

  declare -a gavs=(
    "com.datastax.cassandra:cassandra-driver-core:${driver_version}:jar"
    "io.netty:netty-buffer:%%netty.version%%:jar"
    "io.netty:netty-resolver:%%netty.version%%:jar"
    "io.netty:netty-codec:%%netty.version%%:jar"
    "io.netty:netty-transport:%%netty.version%%:jar"
    "io.netty:netty-common:%%netty.version%%:jar"
    "io.netty:netty-transport-native-epoll:%%netty.version%%:jar"
    "io.netty:netty-handler:%%netty.version%%:jar"
    "io.netty:netty-transport-native-unix-common:%%netty.version%%:jar"
    "io.dropwizard.metrics:metrics-core:%%metrics.version%%:jar"
    "ch.qos.logback:logback-core:1.1.3:jar"
    "ch.qos.logback:logback-classic:1.1.3:jar"
    "com.google.guava:guava:%%cassandra.guava.version%%:jar"
  )

  # the cassandra install bundles the cassandra-all.jar as apache-cassandra.jar
  if [[ -z "$([[ "$classpath" =~ .*apache-cassandra-([^:/][^:/]*)\.jar.* ]] && echo "${BASH_REMATCH[1]}")" ]]; then
    gavs+=("org.apache.cassandra:cassandra-all:${cassandra_version}:jar")
  fi

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

# gets any dependencies that should be removed from the classpath for this module
# args:
#   $1 - current classpath
function exclude_dependencies() {
  echo ""
}
