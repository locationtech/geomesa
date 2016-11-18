#!/usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

kafka_major_version="%%kafka.maj.version%%"

if [[ (-z "$1") ]]; then
  echo "Error: Provide one arg which is the target directory (e.g. /opt/jboss/standalone/deployments/geoserver.war/WEB-INF/lib)"
  exit
fi

install_dir=$1
NL=$'\n'
read -r -p "Install Kafka ${kafka_major_version} DataStore dependencies to ${install_dir}?${NL}Confirm? [Y/n]" confirm
confirm=${confirm,,} #lowercasing
if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
  # get stuff

  if [[ $kafka_major_version = "08" ]]; then
    ZK_CLIENT_VERSION="0.3"
  elif [[ $kafka_major_version = "09" ]]; then
    ZK_CLIENT_VERSION="0.7"
  elif [[ $kafka_major_version = "10" ]]; then
    ZK_CLIENT_VERSION="0.8"
  else
    echo "Error: Kafka version ${kafka_major_version} is not supported."
    exit
  fi

  declare -a urls=(
    "https://search.maven.org/remotecontent?filepath=org/apache/kafka/kafka-clients/%%kafka.min.version%%/kafka-clients-%%kafka.min.version%%.jar"
    "https://search.maven.org/remotecontent?filepath=org/apache/kafka/kafka_2.11/%%kafka.min.version%%/kafka_2.11-%%kafka.min.version%%.jar"
    "https://search.maven.org/remotecontent?filepath=org/apache/zookeeper/zookeeper/%%zookeeper.version%%/zookeeper-%%zookeeper.version%%.jar"
    "https://search.maven.org/remotecontent?filepath=com/101tec/zkclient/${ZK_CLIENT_VERSION}/zkclient-${ZK_CLIENT_VERSION}.jar"
    "https://search.maven.org/remotecontent?filepath=com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar"
  )

  for x in "${urls[@]}"; do
    fname=$(basename "$x");
    echo "fetching ${x}";
    wget -O "${1}/${fname}" "$x" || { rm -f "${1}/${fname}"; echo "Failed to download: ${x}"; \
      errorList="${errorList} ${x} ${NL}"; };
  done

  if [[ -n "${errorList}" ]]; then
    echo "Failed to download: ${NL} ${errorList}";
  fi
else
  echo "Installation cancelled"
fi
