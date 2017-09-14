#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
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

# for hadoop 2.5 and 2.6 to work we need these
# These should match up to what the hadoop version desires
guava_version="11.0.2"
com_log_version="1.1.3"
aws_sdk_version="1.7.4"
commons_config_version="1.6"
htrace_version="3.1.0-incubating"

# this should match the parquet desired version
snappy_version="1.1.1.6"

# Resource download location
base_url="https://search.maven.org/remotecontent?filepath="

function splitVersion() {
  # Split a version on '.' and return space separated list for an array
  split=($(echo $1 | sed -e 's/\./ /g'))
  echo "${split[@]}"
}

function compareVersions() {
  # usage: compareVersions [version_1] [version_2]
  # returns 0 (true) if version_1 > version_2
  # Note this function only works if the version numbers are the same length.
  version_1_split=($(splitVersion $1))
  version_2_split=($(splitVersion $2))

  for v in "${!version_1_split[@]}"; do
    if [[ "${version_1_split[$v]}" -gt "${version_2_split[$v]}" ]]; then
      return 0
    elif [[ "${version_1_split[$v]}" -eq "${version_2_split[$v]}" ]]; then
      continue
    elif [[ "${version_1_split[$v]}" -lt "${version_2_split[$v]}" ]]; then
      return 1
    fi
  done

  # Here only if $1 == $2
  return 1
}

function printVersions() {
  # usage: getVersions [url] [min_version]

  content=$(wget $1 -q -O -)
  # basic xml parsing for version numbers
  versions=$(echo "${content}" | grep -oE ">[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}\/<" | grep -oE "[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}")
  versionArray=($(echo "$versions" | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/ /g'))

  # Filter out version numbers that are older than min_version
  for version in "${versionArray[@]}"; do
    if compareVersions "${2}" "${version}"; then
      newVersionArray=()
      for j in "${versionArray[@]}"; do
        if [[ "${j}" != "${version}" ]]; then
          newVersionArray=("${newVersionArray[@]}" "${j}")
        fi
      done
      versionArray=("${newVersionArray[@]}")
    fi
  done

  # Remove empty elements
  versionArray=($( echo "${versionArray[@]}" | sed -e 's/  / /g'))

  # Print results
  size=${#versionArray[@]}
  i=0
  while [[ $i -lt $size ]]; do
    echo -e "${versionArray[i]}\t${versionArray[i+1]}\t${versionArray[i+2]}\t${versionArray[i+3]}"
    i=$(expr $i + 4)
  done
  echo ""
}

# Command Line Help
NL=$'\n'
usage="usage: ./install-hadoop.sh [[target dir] [<version(s)>]] | [-l|--list-versions] | [--help]"

# Parse command line options
if [[ "$1" == "--help" || "$1" == "-help" ]]; then
  echo "${usage}"
  echo "${NL}"
  echo "All versions are detected automatically at compile time."
  echo "These parameters are for situations where this may need overwritten."
  echo "${NL}"
  echo "Options:"
  echo "  -h,--hadoop-version       Manually set Hadoop version"
  echo "  -l,--list-versions        Print out available version numbers."
  echo "${NL}"
  echo "Example:"
  echo "./install-hadoop.sh /opt/jboss/standalone/deployments/geoserver.war/WEB-INF/lib -a 1.7.1 -h 2.7.3"
  echo "${NL}"
  exit 0
elif [[ "$1" == "-l" || "$1" == "--list-versions" ]]; then
  hadoop_version_url="${base_url}org/apache/hadoop/hadoop-main/"

  echo ""
  echo "Available Hadoop Versions"
  printVersions "${hadoop_version_url}" "${hadoop_version_min}"

  exit 0
else
  install_dir=$1
  shift
fi

while [[ $# -gt 1 ]]; do
  key="$1"

  case $key in
    -h|--hadoop-version)
      hadoop_version="$2"
      shift
    ;;
    *)
      echo "Unknown parameter $1"
      echo "${usage}"
      exit 1
    ;;
  esac

  shift
done

# Check for any incomplete parameters or mistypes e.g. "-a" without a version
if [[ -n "$1" ]]; then
  echo "Unknown or incomplete parameter $1"
  exit 1
fi

# Check for short version numbers. The version checker only works for version number of the same length.
hadoop_version_split=($(splitVersion "${hadoop_version}"))

if [[ "${#hadoop_version_split[@]}" != "3" ]]; then
  echo "Error: Version numbers must be specified in the format X.X.X"
  exit 1
fi

if [[ -z "${install_dir}" ]]; then
  echo "Error: Provide one arg which is the target directory (e.g. /opt/geoserver-2.9.1/webapps/geoserver/WEB-INF/lib)"
  echo "${usage}"
  exit 1
else
  read -r -p "Install Hadoop, Amazon AWS, and Snappy to ${install_dir}?${NL}Confirm? [Y/n]" confirm
  confirm=${confirm,,} # Lowercasing
  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    # Setup download URLs
    declare -a urls=(
      "${base_url}org/apache/hadoop/hadoop-auth/${hadoop_version}/hadoop-auth-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-client/${hadoop_version}/hadoop-client-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-common/${hadoop_version}/hadoop-common-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-hdfs/${hadoop_version}/hadoop-hdfs-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-aws/${hadoop_version}/hadoop-aws-${hadoop_version}.jar"
      "${base_url}org/apache/htrace/htrace-core/${htrace_version}/htrace-core-${htrace_version}.jar"
      "${base_url}com/amazonaws/aws-java-sdk/${aws_sdk_version}/aws-java-sdk-${aws_sdk_version}.jar"
      "${base_url}org/xerial/snappy/snappy-java/${snappy_version}/snappy-java-${snappy_version}.jar"
      "${base_url}commons-configuration/commons-configuration/${commons_config_version}/commons-configuration-${commons_config_version}.jar"
      "${base_url}commons-logging/commons-logging/${com_log_version}/commons-logging-${com_log_version}.jar"
      "${base_url}com/google/guava/guava/${guava_version}/guava-${guava_version}.jar"
      "${base_url}commons-cli/commons-cli/1.2/commons-cli-1.2.jar"
      "${base_url}com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar"
      "${base_url}commons-io/commons-io/2.5/commons-io-2.5.jar"
      "${base_url}org/apache/httpcomponents/httpclient/4.3.4/httpclient-4.3.4.jar"
      "${base_url}org/apache/httpcomponents/httpcore/4.3.3/httpcore-4.3.3.jar"
      "${base_url}commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar"
    )

    # Download dependencies
    for x in "${urls[@]}"; do
      fname=$(basename "$x");
      echo "fetching ${x}";
      wget -O "${install_dir}/${fname}" "$x" || { rm -f "${install_dir}/${fname}"; echo "Error downloading dependency: ${fname}"; \
        errorList="${errorList[@]} ${x} ${NL}"; };
    done
    if [[ -n "${errorList}" ]]; then
      echo "Failed to download: ${NL} ${errorList[@]}";
    fi
  else
    echo "Installation cancelled"
  fi
fi
