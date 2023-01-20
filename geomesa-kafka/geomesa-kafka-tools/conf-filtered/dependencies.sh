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
zkclient_install_version="%%zkclient.version%%"
zookeeper_install_version="%%zookeeper.version.recommended%%"
jopt_install_version="%%kafka.jopt.version%%"

function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

function dependencies() {
  local classpath="$1"

  local kafka_version="$kafka_install_version"
  local zkclient_version="$zkclient_install_version"
  local zk_version="$zookeeper_install_version"

  if [[ -n "$classpath" ]]; then
    kafka_version="$(get_classpath_version kafka-clients $classpath $kafka_version)"
    zkclient_version="$(get_classpath_version zkclient $classpath $zkclient_version)"
    zk_version="$(get_classpath_version zookeeper $classpath $zk_version)"
  fi

  declare -a gavs=(
    "org.apache.kafka:kafka_%%scala.binary.version%%:${kafka_version}:jar"
    "org.apache.kafka:kafka-clients:${kafka_version}:jar"
    "org.apache.zookeeper:zookeeper:${zk_version}:jar"
    "com.101tec:zkclient:${zkclient_version}:jar"
    "net.sf.jopt-simple:jopt-simple:${jopt_install_version}:jar"
    "com.yammer.metrics:metrics-core:2.2.0:jar"
  )

  # compare the version of zookeeper to determine if we need zookeeper-jute (version >= 3.5.5)
  JUTE_FROM_VERSION="3.5.5"
  if version_ge ${zk_version} $JUTE_FROM_VERSION; then
    gavs+=(
      "org.apache.zookeeper:zookeeper-jute:${zk_version}:jar"
    )
  fi

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

function exclude_dependencies() {
  # local classpath="$1"
  echo ""
}
