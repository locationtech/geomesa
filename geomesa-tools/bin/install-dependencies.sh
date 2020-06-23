#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for the data store
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

# configure HOME and CONF_DIR, then load geomesa-env.sh
export %%gmtools.dist.name%%_HOME="${%%gmtools.dist.name%%_HOME:-$(cd "`dirname "$0"`"/..; pwd)}"
export GEOMESA_CONF_DIR="${GEOMESA_CONF_DIR:-$%%gmtools.dist.name%%_HOME/conf}"

if [[ -f "${GEOMESA_CONF_DIR}/geomesa-env.sh" ]]; then
  . "${GEOMESA_CONF_DIR}/geomesa-env.sh"
else
  echo >&2 "ERROR: could not read '${GEOMESA_CONF_DIR}/geomesa-env.sh', aborting script"
  exit 1
fi

if [[ -f "${GEOMESA_CONF_DIR}/dependencies.sh" ]]; then
  . "${GEOMESA_CONF_DIR}/dependencies.sh"
else
  echo >&2 "ERROR: could not read '${GEOMESA_CONF_DIR}/dependencies.sh', aborting script"
  exit 2
fi

install_dir="${%%gmtools.dist.name%%_HOME}/lib"

if [[ -n "$1" && "$1" != "--no-prompt" ]]; then
  install_dir="$1"
  shift
fi

includes="$(dependencies)"
gavs=()
for gav in $includes; do
  gavs+=($gav)
done
download_maven "$install_dir" gavs[@] "Preparing to install the following artifacts into $install_dir:${newline}" $1

error=$?
if [[ $error -ne 0 ]]; then
  exit $error
fi

excludes="$(exclude_dependencies)"
if [[ -n "$excludes" ]]; then
  jars=()
  for jar in $excludes; do
    if [[ -f "$install_dir/$jar" ]]; then
      jars+=("$jar")
    fi
  done
  if [[ ${#jars[@]} -gt 0 ]]; then
    read -r -p "Found conflicting JAR(s):$newline  $(echo "${jars[@]}" | sed 's/ /\n  /g')${newline}Remove them? (y/n) " confirm
    confirm=${confirm,,} # lower-casing
    if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
      for jar in "${jars[@]}"; do
        rm -f "$install_dir/$jar"
      done
    fi
  fi
fi
