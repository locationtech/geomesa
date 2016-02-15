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
    read -r -p "Install accumulo and hadoop dependencies to ${install_dir}?${NL}Confirm? [Y/n]" confirm
    confirm=${confirm,,} #lowercasing
    if [[ $confirm =~ ^(yes|y) ]]; then
        # get stuff
        declare -a urls=(
            "https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-core/${accumulo.version}/accumulo-core-${accumulo.version}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-fate/${accumulo.version}/accumulo-fate-${accumulo.version}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-trace/${accumulo.version}/accumulo-trace-${accumulo.version}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/thrift/libthrift/${thrift.version}/libthrift-${thrift.version}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/zookeeper/zookeeper/${zookeeper.version}/zookeeper-${zookeeper.version}.jar"
            "https://search.maven.org/remotecontent?filepath=commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-auth/${hadoop.version}/hadoop-auth-${hadoop.version}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-client/${hadoop.version}/hadoop-client-${hadoop.version}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-common/${hadoop.version}/hadoop-common-${hadoop.version}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-hdfs/${hadoop.version}/hadoop-hdfs-${hadoop.version}.jar"
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
