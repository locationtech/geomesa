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
<<<<<<< HEAD
aws_sdk_v1_install_version="1.12.735" # latest version as of 2024/06
aws_sdk_v2_install_version="2.25.64" # latest version as of 2024/06
aws_crt_install_version="0.29.18"
=======
<<<<<<< HEAD
aws_sdk_install_version="1.12.385" # latest version as of 2023/01
=======
# required for hadoop - make sure it corresponds to the hadoop installed version
guava_install_version="%%hadoop.guava.version%%"
aws_sdk_install_version="1.10.6"
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> eca30cb68af (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5b52d906cba (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 53570f0ec79 (GEOMESA-3254 Add Bloop build support)
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
    aws_sdk_v1_version="$(get_classpath_version aws-java-sdk-core "$classpath" "$aws_sdk_v1_version")"
    aws_sdk_v2_version="$(get_classpath_version aws-core "$classpath" "$aws_sdk_v2_version")"
    snappy_version="$(get_classpath_version snappy-java "$classpath" "$snappy_version")"
  fi

  declare -a gavs=(
    "org.apache.hadoop:hadoop-auth:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-common:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs-client:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-mapreduce-client-core:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-aws:${hadoop_version}:jar"
    "org.xerial.snappy:snappy-java:${snappy_version}:jar"
    "com.fasterxml.woodstox:woodstox-core:5.3.0:jar"
    "org.codehaus.woodstox:stax2-api:4.2.1:jar"
    "org.apache.commons:commons-configuration2:2.8.0:jar"
    "commons-configuration:commons-configuration:1.6:jar"
    "commons-collections:commons-collections:3.2.2:jar"
    "commons-lang:commons-lang:2.6:jar"
    "commons-logging:commons-logging:1.1.3:jar"
    "commons-cli:commons-cli:1.2:jar"
    "commons-io:commons-io:2.5:jar"
    "com.google.protobuf:protobuf-java:2.5.0:jar"
    "org.apache.htrace:htrace-core:3.1.0-incubating:jar"
    "org.apache.htrace:htrace-core4:4.1.0-incubating:jar"
    # these are the versions used by hadoop 2.8 and 3.1
    "org.apache.httpcomponents:httpclient:4.5.2:jar"
    "org.apache.httpcomponents:httpcore:4.4.4:jar"
    "commons-httpclient:commons-httpclient:3.1:jar"
  )

  # add hadoop 3+ jars if needed
  if version_ge "${hadoop_version}" 3.0.0; then
    gavs+=(
      "org.apache.hadoop:hadoop-client-api:${hadoop_version}:jar"
      "org.apache.hadoop:hadoop-client-runtime:${hadoop_version}:jar"
      "com.google.guava:guava:27.0-jre:jar"
    )
  else
    gavs+=(
      "com.google.guava:guava:11.0.2:jar"
    )
  fi

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
