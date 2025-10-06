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
# General Environment Variables
# ==================================================================

# Maven repository, used for downloading jar dependencies when required
export GEOMESA_MAVEN_URL="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

# check for missing dependencies and offer to download them
export GEOMESA_CHECK_DEPENDENCIES="${GEOMESA_CHECK_DEPENDENCIES:-true}"

# ==================================================================
# GeoMesa Environment Variables
# ==================================================================

# set the classpath explicitly - no other checks or variables will be used
# export GEOMESA_CLASSPATH="/path/to/jar1.jar:/path/to/jar2.jar:/path/to/conf"

# lib directory
export GEOMESA_LIB_DIR="${%%tools.dist.name%%_HOME}/lib"
if [[ -n "$GEOMESA_LIB" ]] && [[ "$GEOMESA_LIB" != "$GEOMESA_LIB_DIR" ]]; then
  echo >&2 "WARNING The GEOMESA_LIB environment variable is deprecated and will be removed in a future release"
  export GEOMESA_LIB_DIR="$GEOMESA_LIB"
fi

# logs directory, by default $%%tools.dist.name%%_HOME/logs
# this directory needs to be writable - in multi-tenant environments, each user should specify this differently
export GEOMESA_LOG_DIR="${GEOMESA_LOG_DIR:-$%%tools.dist.name%%_HOME/logs}"

# debug options passed to the java process when invoked with 'debug'
export GEOMESA_DEBUG_OPTS="-Xmx8192m -XX:-UseGCOverheadLimit -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:9898"
# ==================================================================
# Java Environment Variables
# ==================================================================

# add additional resources to the classpath, including the distributed classpath for m/r jobs
# export GEOMESA_EXTRA_CLASSPATHS="${GEOMESA_EXTRA_CLASSPATHS:-/some/dir/:/another/dir/}"

# additional java process options, includes JAVA_OPTS by default
export CUSTOM_JAVA_OPTS="${CUSTOM_JAVA_OPTS} $JAVA_OPTS"

# add additional native libraries to the java classpath (LD_LIBRARY_PATH)
# export JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH:-/path/to/java/native/library}"

# ==================================================================
# GeoMesa Nailgun Server Variables
# ==================================================================

# enable or disable a nailgun server for faster commands
export GEOMESA_NG_ENABLED="${GEOMESA_NG_ENABLED:-false}"
# host and port used to run nailgun server
export GEOMESA_NG_SERVER="${GEOMESA_NG_SERVER:-$NAILGUN_SERVER}"
export GEOMESA_NG_PORT="${GEOMESA_NG_PORT:-$NAILGUN_PORT}"
# timeout for ng client heartbeats
export GEOMESA_NG_TIMEOUT="${GEOMESA_NG_TIMEOUT:-}"
# time before the nailgun server is shut down due to inactivity
export GEOMESA_NG_IDLE="${GEOMESA_NG_IDLE:-1 hour}"
# size of the thread pool used for handling requests
export GEOMESA_NG_POOL_SIZE="${GEOMESA_NG_POOL_SIZE:-}"

# ==================================================================
# Export the variables configured above as needed
# This section shouldn't need to be modified
# ==================================================================

# export the above nailgun options for use by the ng client
if [[ -n "${GEOMESA_NG_SERVER}" ]]; then
  export NAILGUN_SERVER="${GEOMESA_NG_SERVER}"
fi
if [[ -n "${GEOMESA_NG_PORT}" ]]; then
  export NAILGUN_PORT="${GEOMESA_NG_PORT}"
fi

# set java opts
GEOMESA_OPTS="-Duser.timezone=UTC -DEPSG-HSQL.directory=/tmp/$(whoami) -Djava.awt.headless=true\
 -Dlog4j.configuration=file://${GEOMESA_CONF_DIR}/log4j.properties -Dgeomesa.home=${%%tools.dist.name%%_HOME}\
 -Dgeomesa.log.dir=${GEOMESA_LOG_DIR}"
if [[ -f "${GEOMESA_CONF_DIR}/java-logging.properties" ]]; then
  # we have to include java.util.logging override file, slf4j can't handle it automatically
  GEOMESA_OPTS="${GEOMESA_OPTS} -Djava.util.logging.config.file=${GEOMESA_CONF_DIR}/java-logging.properties"
fi

# configure java library path
# TODO JAVA_LIBRARY_PATH not being set, and this is being called before setting the classpath, which can modify JAVA_LIBRARY_PATH
if [[ -n "$JAVA_LIBRARY_PATH" ]]; then
  GEOMESA_OPTS="${GEOMESA_OPTS} -Djava.library.path=${JAVA_LIBRARY_PATH}"
  export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$JAVA_LIBRARY_PATH"
fi

export GEOMESA_OPTS="$CUSTOM_JAVA_OPTS $GEOMESA_OPTS"
