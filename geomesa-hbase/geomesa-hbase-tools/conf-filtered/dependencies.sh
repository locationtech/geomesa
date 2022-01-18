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
hadoop_install_version="%%hadoop.version.recommended%%"
zookeeper_install_version="%%zookeeper.version.recommended%%"
# required for hadoop - make sure it corresponds to the hadoop installed version
guava_install_version="%%guava.version%%"

function dependencies() {
  local classpath="$1"

  local hbase_version="$hbase_install_version"
  local hbase_thirdparty_version="$hbase_thirdparty_install_version"
  local hadoop_version="$hadoop_install_version"
  local zk_version="$zookeeper_install_version"

  if [[ -n "$classpath" ]]; then
    hbase_version="$(get_classpath_version hbase-client $classpath $hbase_version)"
    hbase_thirdparty_version="$(get_classpath_version hbase-shaded-protobuf $classpath $hbase_thirdparty_version)"
    hadoop_version="$(get_classpath_version hadoop-common $classpath $hadoop_version)"
    zk_version="$(get_classpath_version zookeeper $classpath $zk_version)"
  fi

  declare -a gavs=(
    "org.apache.hbase:hbase-client:${hbase_version}:jar"
    "org.apache.hbase:hbase-common:${hbase_version}:jar"
    "org.apache.hbase:hbase-hadoop-compat:${hbase_version}:jar"
    "org.apache.hbase:hbase-protocol:${hbase_version}:jar"
    "com.google.protobuf:protobuf-java:2.5.0:jar"
    "org.apache.zookeeper:zookeeper:${zk_version}:jar"
    "org.apache.hadoop:hadoop-auth:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-common:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs-client:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-mapreduce-client-core:${hadoop_version}:jar"
    "commons-configuration:commons-configuration:1.6:jar"
    "commons-logging:commons-logging:1.1.3:jar"
    "commons-cli:commons-cli:1.2:jar"
    "commons-io:commons-io:2.5:jar"
    "javax.servlet:servlet-api:2.4:jar"
    "io.netty:netty-all:%%netty.version%%:jar"
    "io.netty:netty:3.6.2.Final:jar"
    # htrace 3 required for hadoop before 2.8
    # htrace 4 required for hadoop 2.8 and later
    # since they have separate package names, should be safe to install both
    "org.apache.htrace:htrace-core:3.1.0-incubating:jar"
    "org.apache.htrace:htrace-core4:4.1.0-incubating:jar"
    "com.google.guava:guava:${guava_install_version}:jar"
  )

  local hbase_maj_ver="$(expr match "$hbase_version" '\([0-9][0-9]*\)\.')"

  # additional dependencies that depend on the major version
  if [[ "hbase_maj_ver" -ge 2 ]]; then
    gavs+=(
      "org.apache.hbase:hbase-mapreduce:${hbase_version}:jar"
      "org.apache.hbase:hbase-protocol-shaded:${hbase_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-miscellaneous:${hbase_thirdparty_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-netty:${hbase_thirdparty_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-protobuf:${hbase_thirdparty_version}:jar"
      "io.dropwizard.metrics:metrics-core:3.2.6:jar"
    )
  else
    gavs+=(
      "com.yammer.metrics:metrics-core:2.2.0:jar"
    )
  fi

  # add hadoop 3+ jars if needed
  local hadoop_maj_ver="$(expr match "$hadoop_version" '\([0-9][0-9]*\)\.')"
  if [[ "$hadoop_maj_ver" -ge 3 ]]; then
    gavs+=(
      "org.apache.hadoop:hadoop-client-api:${hadoop_version}:jar"
      "org.apache.hadoop:hadoop-client-runtime:${hadoop_version}:jar"
    )
  fi

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
