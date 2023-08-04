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
# Cassandra Environment Variables
# ==================================================================

# set the classpath directly, which will skip all other processing
# otherwise, CASSANDRA_HOME, etc will be used to build up the classpath
# export GEOMESA_CASSANDRA_CLASSPATH=

# Cassandra home directory
# export CASSANDRA_HOME="${CASSANDRA_HOME:-/path/to/cassandra}"

# get cassandra classpath
function get_cassandra_classpath() {
  if [[ -n "$GEOMESA_CASSANDRA_CLASSPATH" ]]; then
    echo "$GEOMESA_CASSANDRA_CLASSPATH"
  else
    local classpath=""
    if [[ -d "$CASSANDRA_HOME/lib" ]]; then
      classpath="$(find_jars "$CASSANDRA_HOME"/lib true)"
      classpath="$(remove_slf4j_from_classpath "$classpath")"
    fi
    echo "$classpath"
  fi
}

get_cassandra_classpath

