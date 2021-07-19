#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for running the geomesa-bigtable command-line tools.
# Usually these dependencies will be provided by the environment (e.g. HADOOP_HOME).
# Update the versions as required to match the target environment.

hbase_install_version="%%hbase.version%%"
hadoop_install_version="%%hadoop.version.recommended%%"

function dependencies() {
  local classpath="$1"

  local hbase_version="$hbase_install_version"
  local hadoop_version="$hadoop_install_version"

  if [[ -n "$classpath" ]]; then
    hbase_version="$(get_classpath_version hbase-client $classpath $hbase_version)"
    hadoop_version="$(get_classpath_version hadoop-common $classpath $hadoop_version)"
  fi

  declare -a gavs=(
    "org.apache.hbase:hbase-annotations:${hbase_version}:jar"
    "org.apache.hbase:hbase-client:${hbase_version}:jar"
    "org.apache.hbase:hbase-common:${hbase_version}:jar"
    "org.apache.hbase:hbase-hadoop2-compat:${hbase_version}:jar"
    "org.apache.hbase:hbase-hadoop-compat:${hbase_version}:jar"
    "org.apache.hbase:hbase-prefix-tree:${hbase_version}:jar"
    "org.apache.hbase:hbase-procedure:${hbase_version}:jar"
    "org.apache.hbase:hbase-protocol:${hbase_version}:jar"
    "org.apache.hbase:hbase-server:${hbase_version}:jar"
    "commons-configuration:commons-configuration:1.6:jar"
    "org.apache.hadoop:hadoop-annotations:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-auth:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-common:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs:${hadoop_version}:jar"
  )

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

function exclude_dependencies() {
  # local classpath="$1"
  echo ""
}
