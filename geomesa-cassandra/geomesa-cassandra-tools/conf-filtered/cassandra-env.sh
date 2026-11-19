#!/usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# https://www.apache.org/licenses/LICENSE-2.0
#

# This script provides configuration of resource locations and settings

# ==================================================================
# Cassandra Environment Variables
# ==================================================================

# set additional classpath entries for connecting to Cassandra
# note: the JARs under $CASSANDRA_HOME/lib are not used, as Cassandra ships with an older, incompatible version
# of the Java driver - instead, the required driver JARs are bundled in the GeoMesa lib directory
# export GEOMESA_CASSANDRA_CLASSPATH=

# get cassandra classpath
function get_cassandra_classpath() {
  echo "$GEOMESA_CASSANDRA_CLASSPATH"
}

get_cassandra_classpath

