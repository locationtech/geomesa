#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for hadoop aws s3 communication
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

hadoop_version="%%hadoop.version.recommended%%"

aws_sdk_version="1.10.6"

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
  "${base_url}org/apache/hadoop/hadoop-aws/${hadoop_version}/hadoop-aws-${hadoop_version}.jar"
  "${base_url}com/amazonaws/aws-java-sdk-core/${aws_sdk_version}/aws-java-sdk-core-${aws_sdk_version}.jar"
  "${base_url}com/amazonaws/aws-java-sdk-s3/${aws_sdk_version}/aws-java-sdk-s3-${aws_sdk_version}.jar"
  "${base_url}joda-time/joda-time/${joda_time_version}/joda-time-${joda_time_version}.jar"
  "${base_url}org/apache/httpcomponents/httpclient/4.3.4/httpclient-4.3.4.jar"
  "${base_url}org/apache/httpcomponents/httpcore/4.3.3/httpcore-4.3.3.jar"
  "${base_url}commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar"
)

downloadUrls "$install_dir" urls[@]
