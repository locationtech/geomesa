#!/usr/bin/env bash
#
# Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
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
        hadoop_ver="2.2.0"
        acc_ver="1.5.1"
        thrift_ver="0.9.1"
        zk_ver="3.4.5"
        declare -a urls=("https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-core/${acc_ver}/accumulo-core-${acc_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-fate/${acc_ver}/accumulo-fate-${acc_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-trace/${acc_ver}/accumulo-trace-${acc_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/zookeeper/zookeeper/${zk_ver}/zookeeper-${zk_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-auth/${hadoop_ver}/hadoop-auth-${hadoop_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-client/${hadoop_ver}/hadoop-client-${hadoop_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-common/${hadoop_ver}/hadoop-common-${hadoop_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-hdfs/${hadoop_ver}/hadoop-hdfs-${hadoop_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-app/${hadoop_ver}/hadoop-mapreduce-client-app-${hadoop_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-common/${hadoop_ver}/hadoop-mapreduce-client-common-${hadoop_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-core/${hadoop_ver}/hadoop-mapreduce-client-core-${hadoop_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-jobclient/${hadoop_ver}/hadoop-mapreduce-client-jobclient-${hadoop_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-shuffle/${hadoop_ver}/hadoop-mapreduce-client-shuffle-${hadoop_ver}.jar"
            "https://search.maven.org/remotecontent?filepath=org/apache/thrift/libthrift/${thrift_ver}/libthrift-${thrift_ver}.jar")

        for x in "${urls[@]}"; do
            fname=$(basename "$x");
            echo "fetching ${x}";
            wget -O "${1}/${fname}" "$x";
         done
    else
        echo "Installation cancelled"
    fi
fi
