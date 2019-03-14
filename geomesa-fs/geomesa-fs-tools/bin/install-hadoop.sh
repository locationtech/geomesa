#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for hadoop
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

hadoop_version="%%hadoop.version.recommended%%"
hadoop_version_min="%%hadoop.version.minimum%%"

# this version required for hadoop 2.8, earlier hadoop versions use 3.1.0-incubating
htrace_core_version="4.1.0-incubating"

# for hadoop 2.5 and 2.6 to work we need these
# These should match up to what the hadoop version desires
guava_version="11.0.2"
com_log_version="1.1.3"
aws_sdk_version="1.10.6"
commons_config_version="1.6"

# this should match the parquet desired version
snappy_version="1.1.1.6"

#joda-time required for aws sdk
joda_time_version="2.8.1"

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}org/apache/hadoop/hadoop-auth/${hadoop_version}/hadoop-auth-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-client/${hadoop_version}/hadoop-client-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-common/${hadoop_version}/hadoop-common-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-hdfs/${hadoop_version}/hadoop-hdfs-${hadoop_version}.jar"
  "${base_url}org/apache/hadoop/hadoop-aws/${hadoop_version}/hadoop-aws-${hadoop_version}.jar"
  "${base_url}com/amazonaws/aws-java-sdk-core/${aws_sdk_version}/aws-java-sdk-core-${aws_sdk_version}.jar"
  "${base_url}com/amazonaws/aws-java-sdk-s3/${aws_sdk_version}/aws-java-sdk-s3-${aws_sdk_version}.jar"
  "${base_url}joda-time/joda-time/${joda_time_version}/joda-time-${joda_time_version}.jar"
  "${base_url}org/xerial/snappy/snappy-java/${snappy_version}/snappy-java-${snappy_version}.jar"
  "${base_url}commons-configuration/commons-configuration/${commons_config_version}/commons-configuration-${commons_config_version}.jar"
  "${base_url}commons-logging/commons-logging/${com_log_version}/commons-logging-${com_log_version}.jar"
  "${base_url}commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
  "${base_url}com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar"
  "${base_url}commons-io/commons-io/2.5/commons-io-2.5.jar"
  "${base_url}org/apache/httpcomponents/httpclient/4.3.4/httpclient-4.3.4.jar"
  "${base_url}org/apache/httpcomponents/httpcore/4.3.3/httpcore-4.3.3.jar"
  "${base_url}commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar"
)

# compare the first digit of htrace core version to determine the artifact name
if [[ "${htrace_core_version%%.*}" -lt 4 ]]; then
  urls+=("${base_url}org/apache/htrace/htrace-core/${htrace_core_version}/htrace-core-${htrace_core_version}.jar")
else
  urls+=("${base_url}org/apache/htrace/htrace-core4/${htrace_core_version}/htrace-core4-${htrace_core_version}.jar")
fi

# if there's already a guava jar (e.g. geoserver) don't install guava to avoid conflicts
if [ -z "$(find -L $install_dir -maxdepth 1 -name 'guava-*' -print -quit)" ]; then
  urls+=("${base_url}com/google/guava/guava/${guava_version}/guava-${guava_version}.jar")
fi

downloadUrls "$install_dir" urls[@]
