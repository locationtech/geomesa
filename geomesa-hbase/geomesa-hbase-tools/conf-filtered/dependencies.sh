#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for running the geomesa-accumulo command-line tools.
# Usually these dependencies will be provided by the environment (e.g. ACCUMULO_HOME).
# Update the versions as required to match the target environment.

hbase_install_version="%%hbase.version.recommended%%"
hbase_thirdparty_install_version="%%hbase.thirdparty.version.recommended%%"
hadoop_install_version="%%hadoop.version%%"
zookeeper_install_version="%%zookeeper.version.recommended%%"
# required for hadoop - make sure it corresponds to the hadoop installed version
guava_install_version="%%hbase.guava.version%%"
snappy_install_version="1.1.10.4"

# gets the dependencies for this module
# args:
#   $1 - current classpath
function dependencies() {
  local classpath="$1"

  local hbase_version="$hbase_install_version"
  local hbase_thirdparty_version="$hbase_thirdparty_install_version"
  local hadoop_version="$hadoop_install_version"
  local zk_version="$zookeeper_install_version"
  local snappy_version="$snappy_install_version"

  if [[ -n "$classpath" ]]; then
    hbase_version="$(get_classpath_version hbase-client "$classpath" "$hbase_version")"
    hbase_thirdparty_version="$(get_classpath_version hbase-shaded-protobuf "$classpath" "$hbase_thirdparty_version")"
    hadoop_version="$(get_classpath_version hadoop-common "$classpath" "$hadoop_version")"
    hadoop_version="$(get_classpath_version hadoop-client-api "$classpath" "$hadoop_version")"
    zk_version="$(get_classpath_version zookeeper "$classpath" "$zk_version")"
    snappy_version="$(get_classpath_version snappy-java "$classpath" "$snappy_version")"
  fi

  if [[ "$hadoop_version" == "3.2.3" ]]; then
    echo >&2 "WARNING Updating Hadoop version from 3.2.3 to 3.2.4 due to invalid client-api Maven artifacts"
    hadoop_version="3.2.4"
  fi

  declare -a gavs=(
    "org.apache.hbase:hbase-client:${hbase_version}:jar"
    "org.apache.hbase:hbase-common:${hbase_version}:jar"
    "org.apache.hbase:hbase-hadoop-compat:${hbase_version}:jar"
    "org.apache.hbase:hbase-protocol:${hbase_version}:jar"
    "com.google.protobuf:protobuf-java:%%hbase.protobuf.version%%:jar"
    "org.apache.zookeeper:zookeeper:${zk_version}:jar"
    "org.apache.hadoop:hadoop-client-api:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-client-runtime:${hadoop_version}:jar"
    "org.xerial.snappy:snappy-java:${snappy_version}:jar"
    "commons-logging:commons-logging:1.3.3:jar"
    "org.apache.httpcomponents:httpclient:4.5.13:jar"
    "org.apache.httpcomponents:httpcore:4.4.13:jar"
    "commons-cli:commons-cli:1.5.0:jar"
    "commons-io:commons-io:2.11.0:jar"
    "io.netty:netty-buffer:%%netty.version%%:jar"
    "io.netty:netty-codec:%%netty.version%%:jar"
    "io.netty:netty-common:%%netty.version%%:jar"
    "io.netty:netty-handler:%%netty.version%%:jar"
    "io.netty:netty-resolver:%%netty.version%%:jar"
    "io.netty:netty-transport:%%netty.version%%:jar"
    "io.netty:netty-transport-classes-epoll:%%netty.version%%:jar"
    "io.netty:netty-transport-native-epoll:%%netty.version%%:jar:linux-x86_64"
    "io.netty:netty-transport-native-unix-common:%%netty.version%%:jar"
    "io.netty:netty:3.10.6.Final:jar"
    "io.dropwizard.metrics:metrics-core:3.2.6:jar"
    "com.google.guava:guava:${guava_install_version}:jar"
  )

  # additional dependencies that depend on the major version
  if version_ge "${hbase_version}" 2.0.0; then
    gavs+=(
      "org.apache.hbase:hbase-mapreduce:${hbase_version}:jar"
      "org.apache.hbase:hbase-protocol-shaded:${hbase_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-miscellaneous:${hbase_thirdparty_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-netty:${hbase_thirdparty_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-netty-tcnative:${hbase_thirdparty_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-protobuf:${hbase_thirdparty_version}:jar"
      "org.apache.hbase.thirdparty:hbase-unsafe:${hbase_thirdparty_version}:jar"
      "io.opentelemetry:opentelemetry-api:1.15.0:jar"
      "io.opentelemetry:opentelemetry-context:1.15.0:jar"
      "io.opentelemetry:opentelemetry-semconv:1.15.0-alpha:jar"
    )
  else
    gavs+=(
      "com.yammer.metrics:metrics-core:2.2.0:jar"
    )
  fi

  if ! version_ge "${hadoop_version}" 3.3.0; then
    gavs+=(
      "org.apache.htrace:htrace-core4:4.1.0-incubating:jar"
    )
  fi

  # compare the version of zookeeper to determine if we need zookeeper-jute (version >= 3.5.5)
  JUTE_FROM_VERSION="3.5.5"
  if version_ge "${zk_version}" $JUTE_FROM_VERSION; then
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
