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
# HBase Environment Variables
# ==================================================================

# set the classpath directly, which will skip all other processing
# otherwise, HBASE_HOME, etc will be used to build up the classpath
# export GEOMESA_HBASE_CLASSPATH=

# HBase home directory
# export HBASE_HOME="${HBASE_HOME:-/path/to/hbase}"

# HBase lib directory, default to $HBASE_HOME/lib
# export HBASE_LIB="${HBASE_LIB:-$HBASE_HOME/lib}"

# HBase conf directory, default to $HBASE_HOME/conf
# export HBASE_CONF_DIR="${HBASE_CONF_DIR:-$HBASE_HOME/conf}"

# ==================================================================
# Zookeeper Environment Variables
# ==================================================================

# Zookeeper home
# export ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-/path/to/zookeeper}"

# get hbase classpath
function get_hbase_classpath() {
  if [[ -n "$GEOMESA_HBASE_CLASSPATH" ]]; then
    echo "$GEOMESA_HBASE_CLASSPATH"
  else
    if [[ -z "$HBASE_CONF_DIR" && -n "$HBASE_HOME" ]]; then
      HBASE_CONF_DIR="$HBASE_HOME/conf"
    fi
    if [[ -z "$HBASE_LIB" && -n "$HBASE_HOME" ]]; then
      HBASE_LIB="$HBASE_HOME/lib"
    fi
    local hbase_cp=""
    if [[ -d "$HBASE_CONF_DIR" ]]; then
      hbase_cp="$HBASE_CONF_DIR"
    fi
    if [[ -d "$HBASE_LIB" ]]; then
      hbase_find_jars="$(find_jars "$HBASE_LIB" true)"
      if [[ -n "$hbase_find_jars" ]]; then
        hbase_cp="$hbase_cp:$hbase_find_jars"
      fi
    fi
    # If no hbase env vars try the hbase classpath directly (works on AWS EMR)
    if [[ -z "${hbase_cp}" && -n "$(command -v hbase)" ]]; then
      hbase_cp=$(hbase classpath)
    fi
    # for zookeeper only include the single root jar
    if [[ -d "${ZOOKEEPER_HOME}" ]]; then
      ZOOKEEPER_JAR="$(find -L "$ZOOKEEPER_HOME" -maxdepth 1 -type f -name "*zookeeper*jar" | head -n 1)"
      hbase_cp="$hbase_cp:${ZOOKEEPER_JAR}"
    fi

    # if there's a geomesa runtime jar in hbase, exclude it from the classpath
    echo "$hbase_cp" | sed -E 's/[^:]*geomesa-hbase-distributed-runtime[^:]*jar//'
  fi
}

get_hbase_classpath

