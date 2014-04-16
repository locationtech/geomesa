#!/bin/bash

# Copyright 2013 Commonwealth Computer Research, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a utility script that will allow you to fire up a Scala REPL
# on the command-line using only the JARs distributed as part of the
# GeoMesa project.
#
# Caveats:
# - You will want to place your Hadoop configuration directory on the
#   path before any of the JARs we list below, otherwise you will end
#   up using the core-site.xml and mapred.xml that are included.
# - This script is meant to be run from the "dev" sub-directory where
#   the distribution .tar.gz file was unpacked.

export CONF=`find . -name geomesa-site.xml`
export CONF_DIR=`dirname $CONF`
export CLASSPATH="$CONF_DIR:"`find . -iname "*.jar" | tr "\n" ":"`
export SCALA_HOME="lib"

java \
  $JAVA_OPTS \
  -cp "${CLASSPATH}" \
  -Dscala.home="$SCALA_HOME" \
  -Dscala.usejavacp=true \
  scala.tools.nsc.MainGenericRunner  "$@"
reset
