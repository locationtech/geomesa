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
netty_install_version="%%netty.version%%"
metrics_install_version="%%metrics.version%%"
guava_install_version="%%cassandra.guava.version%%"

function dependencies() {
  local classpath="$1"

  local cassandra_version="$cassandra_install_version"
  local driver_version="$driver_install_version"

  if [[ -n "$classpath" ]]; then
    # check for both cassandra-all and apache-cassandra
    cassandra_version="$(get_classpath_version cassandra-all $classpath $cassandra_version)"
    cassandra_version="$(get_classpath_version apache-cassandra $classpath $cassandra_version)"
    driver_version="$(get_classpath_version cassandra-driver-core $classpath $driver_version)"
  fi

  declare -a gavs=(
    "com.datastax.cassandra:cassandra-driver-core:${driver_version}:jar"
    "io.netty:netty-all:${netty_install_version}:jar"
    "io.dropwizard.metrics:metrics-core:${metrics_install_version}:jar"
    "ch.qos.logback:logback-core:1.1.3:jar"
    "ch.qos.logback:logback-classic:1.1.3:jar"
    "com.google.guava:guava:${guava_install_version}:jar"
  )

  # the cassandra install bundles the cassandra-all.jar as apache-cassandra.jar
  if [[ -z "$(expr match "$classpath" ".*apache-cassandra-\([^:/][^:/]*\)\.jar.*")" ]]; then
    gavs+=("org.apache.cassandra:cassandra-all:${cassandra_version}:jar")
  fi

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

function exclude_dependencies() {
  # local classpath="$1"
  echo ""
}
