#!/usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script provides configuration of resource locations and settings

# ==================================================================
# Hadoop Environment Variables
# ==================================================================

# set the classpath directly, which will skip all other processing
# otherwise, HADOOP_HOME, etc will be used to build up the classpath
# export GEOMESA_HADOOP_CLASSPATH=${GEOMESA_HADOOP_CLASSPATH:-$(hadoop classpath)}"

# Hadoop home
# export HADOOP_HOME="${HADOOP_HOME:-/path/to/hadoop}"

# Hadoop conf directory
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"

# Hadoop CDH configuration
# Setting this variable to "1" will configure classpath settings for Hadoop
# CDH. HADOOP_HOME and HADOOP_CONF_DIR are still used.
hadoopCDH="0"

# Hadoop CDH classpath variables
# Depending on your installation configuration these may not be needed.
# These are all loaded into the classpath. slf4j jars will be excluded.

# export HADOOP_COMMON_HOME="${HADOOP_COMMON_HOME:-/path/to/hadoop/common/home}"
# export HADOOP_HDFS_HOME="${HADOOP_HDFS_HOME:-/path/to/hadoop/hdfs/home}"
# export YARN_HOME="${YARN_HOME:-/path/to/yarn/home}"
# export HADOOP_MAPRED_HOME="${HADOOP_MAPRED_HOME:-/path/to/map/reduce/home}"
# export HADOOP_CUSTOM_CP="${HADOOP_CUSTOM_CP:-/path/to/jars:/path/to/jars}"

# get hadoop classpath
function get_hadoop_classpath() {
  if [[ -n "$GEOMESA_HADOOP_CLASSPATH" ]]; then
    echo "$GEOMESA_HADOOP_CLASSPATH"
  else
    # Copied from accumulo classpath
    if [[ "$hadoopCDH" == "1" ]]; then
      # Hadoop CDH configuration
      hadoopDirs=(
        "$HADOOP_HOME"
        "$HADOOP_CONF_DIR"
        "$HADOOP_COMMON_HOME"
        "$HADOOP_HDFS_HOME"
        "$YARN_HOME"
        "$HADOOP_MAPRED_HOME"
        "$HADOOP_CUSTOM_CP"
      )
    else
      hadoopDirs=(
        # Hadoop 2 requirements
        "$HADOOP_HOME/share/hadoop/common/"
        "$HADOOP_HOME/share/hadoop/hdfs/"
        "$HADOOP_HOME/share/hadoop/mapreduce/"
        "$HADOOP_HOME/share/hadoop/tools/lib/"
        "$HADOOP_HOME/share/hadoop/yarn/"
        # Hadoop 3 requirements
        "$HADOOP_HOME/share/hadoop/client/"
        "$HADOOP_HOME/share/hadoop/common/lib/"
        "$HADOOP_HOME/share/hadoop/yarn/lib/"
        # HDP 2.0 requirements
        /usr/lib/hadoop/
        /usr/lib/hadoop-hdfs/
        /usr/lib/hadoop-mapreduce/
        /usr/lib/hadoop-yarn/
        # HDP 2.2 requirements
        /usr/hdp/current/hadoop-client/
        /usr/hdp/current/hadoop-hdfs-client/
        /usr/hdp/current/hadoop-mapreduce-client/
        /usr/hdp/current/hadoop-yarn-client/
        # IOP 4.1 requirements
        /usr/iop/current/hadoop-client/
        /usr/iop/current/hadoop-hdfs-client/
        /usr/iop/current/hadoop-mapreduce-client/
        /usr/iop/current/hadoop-yarn-client/
      )
    fi

    local HADOOP_CP=""

    for home in ${hadoopDirs[*]}; do
      tmp="$(find_jars "$home" true)"
      if [[ -n "$tmp" ]]; then
        HADOOP_CP="$HADOOP_CP:$tmp"
      fi
    done

    if [[ -n "${HADOOP_CP}" && -d "${HADOOP_CONF_DIR}" ]]; then
      HADOOP_CP="${HADOOP_CP}:${HADOOP_CONF_DIR}"
    fi

    # if didn't find anything, attempt to cheat by stealing the classpath from the hadoop command
    if [[ -z "${HADOOP_CP}" && -n "$(command -v hadoop)" ]]; then
      HADOOP_CP="$(hadoop classpath)"
      HADOOP_CP="$(expand_classpath "$HADOOP_CP")"
    fi
    echo "$HADOOP_CP"
  fi
}

get_hadoop_classpath
