#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}com/google/guava/guava/17.0/guava-17.0.jar"
  "${base_url}ch/qos/logback/logback-classic/1.1.3/logback-classic-1.1.3.jar"
  "${base_url}ch/qos/logback/logback-core/1.1.3/logback-core-1.1.3.jar"
)

downloadUrls "$install_dir" urls[@]
