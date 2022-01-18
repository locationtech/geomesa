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
guava_install_version="%%guava.version%%"
aws_sdk_install_version="1.10.6"
# this should match the parquet desired version
snappy_install_version="1.1.1.6"

function dependencies() {
  local classpath="$1"

  local hadoop_version="$hadoop_install_version"
  local aws_sdk_version="$aws_sdk_install_version"
  local snappy_version="$snappy_install_version"

  if [[ -n "$classpath" ]]; then
    hadoop_version="$(get_classpath_version hadoop-common $classpath $hadoop_version)"
    aws_sdk_version="$(get_classpath_version aws-java-sdk-core $classpath $aws_sdk_version)"
    snappy_version="$(get_classpath_version snappy-java $classpath $snappy_version)"
  fi

  declare -a gavs=(
    "org.apache.hadoop:hadoop-auth:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-client:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-common:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs-client:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-aws:${hadoop_version}:jar"
    "org.xerial.snappy:snappy-java:${snappy_version}:jar"
    "commons-configuration:commons-configuration:1.6:jar"
    "commons-logging:commons-logging:1.1.3:jar"
    "commons-cli:commons-cli:1.2:jar"
    "commons-io:commons-io:2.5:jar"
    "com.google.protobuf:protobuf-java:2.5.0:jar"
    # htrace 3 required for hadoop before 2.8
    # htrace 4 required for hadoop 2.8 and later
    # since they have separate package names, should be safe to install both
    "org.apache.htrace:htrace-core:3.1.0-incubating:jar"
    "org.apache.htrace:htrace-core4:4.1.0-incubating:jar"
    "com.amazonaws:aws-java-sdk-core:${aws_sdk_version}:jar"
    "com.amazonaws:aws-java-sdk-s3:${aws_sdk_version}:jar"
    # joda-time required for aws sdk
    "joda-time:joda-time:2.8.1:jar"
    # these are the versions used by hadoop 2.8 and 3.1
    "org.apache.httpcomponents:httpclient:4.5.2:jar"
    "org.apache.httpcomponents:httpcore:4.4.4:jar"
    "commons-httpclient:commons-httpclient:3.1:jar"
    "com.google.guava:guava:${guava_install_version}:jar"
  )

  # add hadoop 3+ jars if needed
  local hadoop_maj_ver="$(expr match "$hadoop_version" '\([0-9][0-9]*\)\.')"
  if [[ "$hadoop_maj_ver" -ge 3 ]]; then
    gavs+=(
      "org.apache.hadoop:hadoop-client-api:${hadoop_version}:jar"
      "org.apache.hadoop:hadoop-client-runtime:${hadoop_version}:jar"
    )
  fi

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

function exclude_dependencies() {
  # local classpath="$1"
  echo ""
}
