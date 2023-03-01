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
# Kafka Environment Variables
# ==================================================================

# set the classpath directly, which will skip all other processing
# otherwise, KAFKA_HOME, etc will be used to build up the classpath
# export GEOMESA_KAFKA_CLASSPATH=

# Kafka home directory
# export KAFKA_HOME="${KAFKA_HOME:-/path/to/kafka}"

# Kafka lib directory, default to $KAFKA_HOME/lib
export KAFKA_LIB="${KAFKA_LIB:-$KAFKA_HOME/libs}"

# ==================================================================
# Zookeeper Environment Variables
# ==================================================================

# Zookeeper home
# export ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-/path/to/zookeeper}"

# get kafka classpath
function get_kafka_classpath() {
  if [[ -n "$GEOMESA_KAFKA_CLASSPATH" ]]; then
    echo "$GEOMESA_KAFKA_CLASSPATH"
  else
    local kafka_cp=""
    if [[ -d "$KAFKA_LIB" ]]; then
      kafka_cp="$(find_jars "$KAFKA_LIB" true)"
    fi
    echo "$kafka_cp"
  fi
}

get_kafka_classpath

