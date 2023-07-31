#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for running the geomesa-fs command-line tools.
# Usually these dependencies will be provided by the environment (e.g. HADOOP_HOME).
# Update the versions as required to match the target environment.

hadoop_install_version="%%hadoop.version.recommended%%"
# required for hadoop - make sure it corresponds to the hadoop installed version
guava_install_version="%%geotools.guava.version%%"

# gets the dependencies for this module
# args:
#   $1 - current classpath
function dependencies() {
  local classpath="$1"

  local hadoop_version="$hadoop_install_version"

  if [[ -n "$classpath" ]]; then
    hadoop_version="$(get_classpath_version hadoop-common "$classpath" "$hadoop_version")"
  fi

  declare -a gavs=(
    "org.apache.hadoop:hadoop-auth:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-common:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs-client:${hadoop_version}:jar"
    "commons-configuration:commons-configuration:1.6:jar"
    "commons-logging:commons-logging:1.1.3:jar"
    # htrace 3 required for hadoop before 2.8
    # htrace 4 required for hadoop 2.8 and later
    # since they have separate package names, should be safe to install both
    "org.apache.htrace:htrace-core:3.1.0-incubating:jar"
    "org.apache.htrace:htrace-core4:4.1.0-incubating:jar"
    "com.google.guava:guava:${guava_install_version}:jar"
  )

  # add hadoop 3+ jars if needed
  local hadoop_maj_ver
  hadoop_maj_ver="$([[ "$hadoop_version" =~ ([0-9][0-9]*)\. ]] && echo "${BASH_REMATCH[1]}")"
  if [[ "$hadoop_maj_ver" -ge 3 ]]; then
    gavs+=(
      "org.apache.hadoop:hadoop-client-api:${hadoop_version}:jar"
      "org.apache.hadoop:hadoop-client-runtime:${hadoop_version}:jar"
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
