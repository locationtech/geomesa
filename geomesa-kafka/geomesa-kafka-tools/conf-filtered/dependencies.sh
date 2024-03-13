#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for running the geomesa-kafka command-line tools.
# Usually these dependencies will be provided by the environment (e.g. KAFKA_HOME).
# Update the versions as required to match the target environment.

kafka_install_version="%%kafka.version%%"
zookeeper_install_version="%%zookeeper.version.recommended%%"

function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

# gets the dependencies for this module
# args:
#   $1 - current classpath
function dependencies() {
  local classpath="$1"

  local kafka_version="$kafka_install_version"
  local zk_version="$zookeeper_install_version"

  if [[ -n "$classpath" ]]; then
    kafka_version="$(get_classpath_version kafka-clients "$classpath" $kafka_version)"
    zk_version="$(get_classpath_version zookeeper "$classpath" $zk_version)"
  fi

  declare -a gavs=(
    "org.apache.kafka:kafka-clients:${kafka_version}:jar"
    "org.apache.zookeeper:zookeeper:${zk_version}:jar"
    "net.sf.jopt-simple:jopt-simple:5.0.4:jar"
    "com.yammer.metrics:metrics-core:2.2.0:jar"
    "io.netty:netty-codec:%%netty.version%%:jar"
    "io.netty:netty-handler:%%netty.version%%:jar"
    "io.netty:netty-resolver:%%netty.version%%:jar"
    "io.netty:netty-transport:%%netty.version%%:jar"
    "io.netty:netty-transport-classes-epoll:%%netty.version%%:jar"
    "io.netty:netty-transport-native-epoll:%%netty.version%%:jar:linux-x86_64"
    "io.netty:netty-transport-native-unix-common:%%netty.version%%:jar"
  )

  # compare the version of zookeeper to determine if we need zookeeper-jute (version >= 3.5.5)
  JUTE_FROM_VERSION="3.5.5"
  if version_ge "$zk_version" $JUTE_FROM_VERSION; then
    gavs+=(
      "org.apache.zookeeper:zookeeper-jute:${zk_version}:jar"
    )
  fi

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

# gets any dependencies that should be removed from the classpath for this module
# args:
#   $1 - current classpath
function exclude_dependencies() {
  echo ""
}
