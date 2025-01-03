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
aws_sdk_v1_install_version="1.12.780" # latest version as of 2025/01
aws_sdk_v2_install_version="2.29.44" # latest version as of 2025/01
aws_crt_install_version="0.33.7"
# this should match the parquet desired version
snappy_install_version="1.1.1.6"

# gets the dependencies for this module
# args:
#   $1 - current classpath
function dependencies() {
  local classpath="$1"

  local hadoop_version="$hadoop_install_version"
  local aws_sdk_v1_version="$aws_sdk_v1_install_version"
  local aws_sdk_v2_version="$aws_sdk_v2_install_version"
  local snappy_version="$snappy_install_version"

  if [[ -n "$classpath" ]]; then
    hadoop_version="$(get_classpath_version hadoop-common "$classpath" "$hadoop_version")"
    hadoop_version="$(get_classpath_version hadoop-client-api "$classpath" "$hadoop_version")"
    aws_sdk_v1_version="$(get_classpath_version aws-java-sdk-core "$classpath" "$aws_sdk_v1_version")"
    aws_sdk_v2_version="$(get_classpath_version aws-core "$classpath" "$aws_sdk_v2_version")"
    snappy_version="$(get_classpath_version snappy-java "$classpath" "$snappy_version")"
  fi

  if [[ "$hadoop_version" == "3.2.3" ]]; then
    echo >&2 "WARNING Updating Hadoop version from 3.2.3 to 3.2.4 due to invalid client-api Maven artifacts"
    hadoop_version="3.2.4"
  fi

  declare -a gavs=(
    "org.apache.hadoop:hadoop-client-api:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-client-runtime:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-aws:${hadoop_version}:jar"
    "org.xerial.snappy:snappy-java:${snappy_version}:jar"
    "commons-logging:commons-logging:1.3.3:jar"
    "org.apache.httpcomponents:httpclient:4.5.13:jar"
    "org.apache.httpcomponents:httpcore:4.4.13:jar"
  )

  # aws sdk
  if version_ge "${hadoop_version}" 3.4.0; then
    gavs+=(
      "software.amazon.awssdk:annotations:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:apache-client:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:arns:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:auth:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:aws-core:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:aws-query-protocol:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:aws-xml-protocol:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:checksums:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:checksums-spi:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:crt-core:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:endpoints-spi:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:http-auth:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:http-auth-aws:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:http-auth-spi:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:http-client-spi:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:identity-spi:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:json-utils:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:metrics-spi:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:netty-nio-client:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:profiles:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:protocol-core:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:regions:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:retries:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:retries-spi:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:s3:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:s3-transfer-manager:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:sdk-core:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:third-party-jackson-core:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk:utils:${aws_sdk_v2_version}:jar"
      "software.amazon.awssdk.crt:aws-crt:${aws_crt_install_version}:jar"
      "software.amazon.eventstream:eventstream:1.0.1:jar"
      "org.reactivestreams:reactive-streams:1.0.4:jar"
    )
  else
    gavs+=(
      "com.amazonaws:aws-java-sdk-core:${aws_sdk_v1_version}:jar"
      "com.amazonaws:aws-java-sdk-s3:${aws_sdk_v1_version}:jar"
      "com.amazonaws:aws-java-sdk-dynamodb:${aws_sdk_v1_version}:jar"
      "org.apache.htrace:htrace-core4:4.1.0-incubating:jar"
      "com.google.guava:guava:27.0-jre:jar"
      "joda-time:joda-time:2.8.1:jar"
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
