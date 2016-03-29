#!/usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

if [[ (-z "$1") ]]; then
    echo "Error: Provide one arg which is the target directory (e.g. /opt/jboss/standalone/deployments/geoserver.war/WEB-INF/lib)"
    exit
else
    install_dir=$1
    NL=$'\n'
    read -r -p "Install Kafka DataStore dependencies to ${install_dir}?${NL}Confirm? [Y/n]" confirm
    confirm=${confirm,,} #lowercasing
    if [[ $confirm =~ ^(yes|y) ]]; then
        # get stuff
        declare -a urls=(
            "https://search.maven.org/remotecontent?filepath=org/apache/kafka/kafka-clients/${kafka.version}/kafka-clients-${kafka.version}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/kafka/kafka_2.11/${kafka.version}/kafka_2.11-${kafka.version}.jar"
            "https://search.maven.org/remotecontent?filepath=com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar"
            "https://search.maven.org/remotecontent?filepath=com/101tec/zkclient/0.3/zkclient-0.3.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/zookeeper/zookeeper/${zookeeper.version}/zookeeper-${zookeeper.version}.jar"
            )

        for x in "${urls[@]}"; do
            fname=$(basename "$x");
            echo "fetching ${x}";
            wget -O "${1}/${fname}" "$x";
         done
    else
        echo "Installation cancelled"
    fi
fi
