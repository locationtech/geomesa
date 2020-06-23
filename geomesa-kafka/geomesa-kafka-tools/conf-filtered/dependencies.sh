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
guava_install_version="%%guava.version%%"
jopt_install_version="%%kafka.jopt.version%%"

function dependencies() {
  local classpath="$1"

  local kafka_version="$kafka_install_version"
  local zk_client_version="$zkclient_install_version"
  local zk_version="$zookeeper_install_version"

  if [[ -n "$classpath" ]]; then
    kafka_version="$(get_classpath_version kafka-clients $classpath $kafka_version)"
    zk_client_version="$(get_classpath_version zkclient $classpath $zk_client_version)"
    zk_version="$(get_classpath_version zookeeper $classpath $zk_version)"
  fi

  declare -a gavs=(
    "org.apache.kafka:kafka_2.11:${kafka_version}:jar"
    "org.apache.kafka:kafka-clients:${kafka_version}:jar"
    "org.apache.zookeeper:zookeeper:${zookeeper_version}:jar"
    "com.101tec:zkclient:${zkclient_version}:jar"
    "net.sf.jopt-simple:jopt-simple:${jopt_install_version}:jar"
    "com.yammer.metrics:metrics-core:2.2.0:jar"
    "com.google.guava:guava:${guava_install_version}:jar"
    "org.slf4j:slf4j-log4j12:1.7.25:jar"
    "log4j:log4j:1.2.17:jar"
  )

  local zk_maj_ver="$(expr match "$zk_version" '\([0-9][0-9]*\)\.')"
  local zk_min_ver="$(expr match "$zk_version" '[0-9][0-9]*\.\([0-9][0-9]*\)')"
  local zk_bug_ver="$(expr match "$zk_version" '[0-9][0-9]*\.[0-9][0-9]*\.\([0-9][0-9]*\)')"

  # compare the version of zookeeper to determine if we need zookeeper-jute (version >= 3.5.5)
  if [[ "$zk_maj_ver" -ge 3 && "$zk_min_ver" -ge 5 && "$zk_bug_ver" -ge 5 ]]; then
    gavs+=(
      "org.apache.zookeeper:zookeeper-jute:$zk_version:jar"
    )
  fi

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

function exclude_dependencies() {
  # local classpath="$1"
  echo ""
}
