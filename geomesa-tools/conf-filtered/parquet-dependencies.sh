#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for using geomesa-convert-parquet.
# Update the versions as required to match the target environment.

hadoop_install_version="%%hadoop.version.recommended%%"

function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

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
    "org.apache.hadoop:hadoop-common:${hadoop_version}:jar"
    "com.fasterxml.woodstox:woodstox-core:5.3.0:jar"
    "org.codehaus.woodstox:stax2-api:4.2.1:jar"
    "org.apache.commons:commons-configuration2:2.8.0:jar"
    "commons-collections:commons-collections:3.2.2:jar"
    "commons-logging:commons-logging:1.2:jar"
  )

  # add hadoop 3+ jars if needed
  if version_ge "${hadoop_version}" 3.0.0; then
    gavs+=(
      "org.apache.hadoop:hadoop-client-api:${hadoop_version}:jar"
      "org.apache.hadoop:hadoop-client-runtime:${hadoop_version}:jar"
    )
  else
    gavs+=(
      "commons-configuration:commons-configuration:1.6:jar"
    )
  fi

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

function exclude_dependencies() {
  # local classpath="$1"
  echo ""
}
