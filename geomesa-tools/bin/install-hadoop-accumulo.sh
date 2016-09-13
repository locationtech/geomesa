#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for hadoop and accumulo
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

accumulo_version="${accumulo.version.recommended}"
hadoop_version="${hadoop.version.recommended}"
zookeeper_version="${zookeeper.version.recommended}"
thrift_version="${thrift.version}"

# for hadoop 2.5 and 2.6 to work we need these
guava_version="11.0.2"
com_log_version="1.1.3"
commons_vfs2_version="2.0"

# Resource download location
base_url="https://search.maven.org/remotecontent?filepath="

# Print versions available for download
function printVersions() {
  # usage: getVersions [url]
  content=$(wget $1 -q -O -)
  versions=$(echo "${content}" | grep -oP ">[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}\/<" | grep -oP "[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}")
  versionArray=($(echo "$versions" | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/ /g'))
  size=${#versionArray[@]}
  i=0
  while [[ $i -lt $size ]]; do
    echo -e "${versionArray[i]}\t${versionArray[i+1]}\t${versionArray[i+2]}\t${versionArray[i+3]}"
    i=$(expr $i + 4)
  done
}

# Command Line Help
NL=$'\n'
usage="usage: ./install-hadoop-accumulo.sh [[target dir] [<version(s)>]] | [-g|--get-versions] | [-h|--help]"

# Parse command line options
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
	echo "${usage}"
	echo "${NL}"
	echo "All versions are detected automatically at compile time."
	echo "These parameters are for situations where this may need overwritten."
	echo "${NL}"
	echo "Options:"
	echo "  -a,--accumulo-version     Manually set Accumulo version"
	echo "  -h,--hadoop-version       Manually set Hadoop version"
	echo "  -z,--zookeeper-version    Manually set Zookeeper version"
	echo "  -t,--thrift-version       Manually set Thrift version"
	echo "  -g,--get-versions         Print out available version numbers."
	echo "${NL}"
	echo "Example:"
	echo "./install-hadoop-accumulo.sh /opt/jboss/standalone/deployments/geoserver.war/WEB-INF/lib -a 1.7.1 -h 2.7.3"
	echo "${NL}"
	exit
elif [[ "$1" == "-g" || "$1" == "--get-versions" ]]; then
  accumulo_version_url="${base_url}org/apache/accumulo/accumulo/"
  hadoop_version_url="${base_url}org/apache/hadoop/hadoop-main/"
  zookeeper_version_url="${base_url}org/apache/zookeeper/zookeeper/"
  thrift_version_url="${base_url}org/apache/thrift/libthrift/"

  echo "Available Accumulo Versions"
  printVersions "${accumulo_version_url}"
  echo "Available Hadoop Versions"
  printVersions "${hadoop_version_url}"
  echo "Available Zookeeper Versions"
  printVersions "${zookeeper_version_url}"
  echo "Available Thrift Versions"
  printVersions "${thrift_version_url}"

  exit
else
  install_dir=$1
  shift
fi

while [[ $# -gt 1 ]]; do
	key="$1"

	case $key in
		-a|--accumulo-version)
			accumulo_version="$2"
			shift
		;;
		-h|--hadoop-version)
			hadoop_version="$2"
			shift
		;;
		-z|--zookeeper-version)
			zookeeper_version="$2"
			shift
		;;
		-t|--thrift-version)
			thrift_version="$2"
			shift
		;;
		*)
			echo "Unknown parameter $1"
			echo "${usage}"
			exit
		;;
	esac

	shift
done

# Check for any incomplete parameters or mistypes e.g. "-a" without a version
if [[ -n "$1" ]]; then
  echo "Unknown or incomplete parameter $1"
  exit
fi

# for Accumulo 1.7+ to work we also need the following
if [[ "${accumulo_version}" == "1.7"* ]]; then
  htrace_core_version="3.1.0-incubating"
  commons_vfs2_version="2.1"
fi

if [[ (-z "${install_dir}") ]]; then
  echo "Error: Provide one arg which is the target directory (e.g. /opt/jboss/standalone/deployments/geoserver.war/WEB-INF/lib)"
  echo "${usage}"
  exit
else
  read -r -p "Install accumulo and hadoop dependencies to ${install_dir}?${NL}Confirm? [Y/n]" confirm
  confirm=${confirm,,} #lowercasing
  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    # get stuff
    declare -a urls=(
      "${base_url}org/apache/accumulo/accumulo-core/${accumulo_version}/accumulo-core-${accumulo_version}.jar"
      "${base_url}org/apache/accumulo/accumulo-fate/${accumulo_version}/accumulo-fate-${accumulo_version}.jar"
      "${base_url}org/apache/accumulo/accumulo-trace/${accumulo_version}/accumulo-trace-${accumulo_version}.jar"
      "${base_url}org/apache/accumulo/accumulo-server-base/${accumulo_version}/accumulo-server-base-${accumulo_version}.jar"
      "${base_url}org/apache/accumulo/accumulo-start/${accumulo_version}/accumulo-start-${accumulo_version}.jar"
      "${base_url}org/apache/thrift/libthrift/${thrift_version}/libthrift-${thrift_version}.jar"
      "${base_url}org/apache/zookeeper/zookeeper/${zookeeper_version}/zookeeper-${zookeeper_version}.jar"
      "${base_url}commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar"
      "${base_url}org/apache/hadoop/hadoop-auth/${hadoop_version}/hadoop-auth-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-client/${hadoop_version}/hadoop-client-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-common/${hadoop_version}/hadoop-common-${hadoop_version}.jar"
      "${base_url}org/apache/hadoop/hadoop-hdfs/${hadoop_version}/hadoop-hdfs-${hadoop_version}.jar"
      "${base_url}commons-logging/commons-logging/${com_log_version}/commons-logging-${com_log_version}.jar"
      "${base_url}com/google/guava/guava/${guava_version}/guava-${guava_version}.jar"
      "${base_url}org/apache/commons/commons-vfs2/${commons_vfs2_version}/commons-vfs2-${commons_vfs2_version}.jar"
    )

    if [[ "${accumulo_version}" == "1.7"* ]]; then
      urls=("${urls[@]}" "${base_url}org/apache/htrace/htrace-core/${htrace_core_version}/htrace-core-${htrace_core_version}.jar")
    fi

    for x in "${urls[@]}"; do
      fname=$(basename "$x");
      echo "fetching ${x}";
      wget -O "${install_dir}/${fname}" "$x" || { rm -f "${install_dir}/${fname}"; echo "Error downloading dependency: ${fname}"; \
        errorList="${errorList} ${x} ${NL}"; };
    done
    if [[ -n "${errorList}" ]]; then
      echo "Failed to download: ${NL} ${errorList}";
    fi
  else
    echo "Installation cancelled"
  fi
fi
