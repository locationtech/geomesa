#!/bin/bash

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

export CLASSPATH="./:"`find . -iname "*.jar" | tr "\n" ":"`
export SCALA_HOME=`find . -iname "*core*dependencies.jar"`

java \
  $JAVA_OPTS \
  -cp "${CLASSPATH}" \
  -Dscala.home="$SCALA_HOME" \
  -Dscala.usejavacp=true \
  scala.tools.nsc.MainGenericRunner  "$@"
