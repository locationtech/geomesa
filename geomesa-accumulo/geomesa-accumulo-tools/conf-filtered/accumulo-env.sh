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
# Accumulo Environment Variables
# ==================================================================

# set the classpath directly, which will skip all other processing
# otherwise, ACCUMULO_HOME, etc will be used to build up the classpath
# export GEOMESA_ACCUMULO_CLASSPATH=

# Accumulo home directory
# export ACCUMULO_HOME="${ACCUMULO_HOME:-/path/to/accumulo}"

if [[ -n "$ACCUMULO_HOME" ]]; then
  # Accumulo lib directory, default to $ACCUMULO_HOME/lib
  export ACCUMULO_LIB="${ACCUMULO_LIB:-$ACCUMULO_HOME/lib}"
  # Accumulo conf directory, default to $ACCUMULO_HOME/conf
  export ACCUMULO_CONF_DIR="${ACCUMULO_CONF_DIR:-$ACCUMULO_HOME/conf}"
fi

# ==================================================================
# Zookeeper Environment Variables
# ==================================================================

# Zookeeper home
# export ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-/path/to/zookeeper}"

# get accumulo classpath
function get_accumulo_classpath() {
  if [[ -n "$GEOMESA_ACCUMULO_CLASSPATH" ]]; then
    echo "$GEOMESA_ACCUMULO_CLASSPATH"
  else
    local accumulo_cp=""
    if [[ -d "$ACCUMULO_CONF_DIR" ]]; then
      accumulo_cp="$ACCUMULO_CONF_DIR"
    fi
    if [[ -d "$ACCUMULO_LIB" ]]; then
      accumulo_cp="$accumulo_cp:$(find_jars "$ACCUMULO_LIB" true)"
    fi
    if [[ -d "${ZOOKEEPER_HOME}" ]]; then
      ZK_JARS="$(find -L "$ZOOKEEPER_HOME" -maxdepth 2 -name 'zookeeper*.jar' | paste -sd: -)"
      accumulo_cp="$accumulo_cp:${ZK_JARS}"
    fi
    # if there's a geomesa runtime jar in accumulo, exclude it from the classpath
    echo "$accumulo_cp" | sed -E 's/[^:]*geomesa-accumulo-distributed-runtime[^:]*jar//'
  fi
}

get_accumulo_classpath
