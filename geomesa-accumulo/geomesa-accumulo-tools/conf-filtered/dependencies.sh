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

accumulo_install_version="%%accumulo.version.recommended%%"
hadoop_install_version="%%hadoop.version.recommended%%"
zookeeper_install_version="%%zookeeper.version.recommended%%"
thrift_install_version="%%thrift.version%%"
# required for hadoop - make sure it corresponds to the hadoop installed version
guava_install_version="%%guava.version%%"

function dependencies() {
  local classpath="$1"

  local accumulo_version="$accumulo_install_version"
  local hadoop_version="$hadoop_install_version"
  local zk_version="$zookeeper_install_version"

  if [[ -n "$classpath" ]]; then
    accumulo_version="$(get_classpath_version accumulo-core $classpath $accumulo_version)"
    hadoop_version="$(get_classpath_version hadoop-common $classpath $hadoop_version)"
    zk_version="$(get_classpath_version zookeeper $classpath $zk_version)"
  fi

  declare -a gavs=(
    "org.apache.accumulo:accumulo-core:${accumulo_version}:jar"
    "org.apache.accumulo:accumulo-server-base:${accumulo_version}:jar"
    "org.apache.accumulo:accumulo-start:${accumulo_version}:jar"
    "org.apache.thrift:libthrift:${thrift_install_version}:jar"
    "org.apache.zookeeper:zookeeper:${zk_version}:jar"
    "commons-configuration:commons-configuration:1.6:jar"
    "org.apache.commons:commons-configuration2:2.5:jar"
    "org.apache.commons:commons-text:1.6:jar"
    "org.apache.hadoop:hadoop-auth:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-common:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs:${hadoop_version}:jar"
    "commons-logging:commons-logging:1.1.3:jar"
    "org.apache.commons:commons-vfs2:2.3:jar"
    # htrace 3 required for hadoop before 2.8, accumulo up to 1.9.2
    # htrace 4 required for hadoop 2.8 and later
    # since they have separate package names, should be safe to install both
    "org.apache.htrace:htrace-core:3.1.0-incubating:jar"
    "org.apache.htrace:htrace-core4:4.1.0-incubating:jar"
    "com.google.guava:guava:${guava_install_version}:jar"
    "org.slf4j:slf4j-log4j12:1.7.25:jar"
  )

  # add accumulo 1.x jars if needed
  local accumulo_maj_ver="$(expr match "$accumulo_version" '\([0-9][0-9]*\)\.')"
  if [[ "$accumulo_maj_ver" -lt 2 ]]; then
    gavs+=(
      "org.apache.accumulo:accumulo-fate:${accumulo_version}:jar"
      "org.apache.accumulo:accumulo-trace:${accumulo_version}:jar"
    )
  else
    gavs+=(
      "org.apache.commons:commons-collections4:4.3:jar"
      "org.apache.accumulo:accumulo-hadoop-mapreduce:${accumulo_version}:jar"
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
  echo "commons-text-1.4.jar"
}
