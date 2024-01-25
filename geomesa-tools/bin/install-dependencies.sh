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
export %%tools.dist.name%%_HOME="${%%tools.dist.name%%_HOME:-$(cd "$(dirname "$0")"/.. || exit; pwd)}"
export GEOMESA_CONF_DIR="${GEOMESA_CONF_DIR:-$%%tools.dist.name%%_HOME/conf}"
export GEOMESA_DEPENDENCIES="${GEOMESA_DEPENDENCIES:-dependencies.sh}"

if [[ -f "${GEOMESA_CONF_DIR}/geomesa-env.sh" ]]; then
  . "${GEOMESA_CONF_DIR}/geomesa-env.sh"
else
  echo >&2 "ERROR: could not read '${GEOMESA_CONF_DIR}/geomesa-env.sh', aborting script"
  exit 1
fi

if [[ -f "${GEOMESA_CONF_DIR}/${GEOMESA_DEPENDENCIES}" ]]; then
  echo >&2 "Reading dependencies from ${GEOMESA_CONF_DIR}/${GEOMESA_DEPENDENCIES}"
  . "${GEOMESA_CONF_DIR}/${GEOMESA_DEPENDENCIES}"
else
  echo >&2 "ERROR: could not read '${GEOMESA_CONF_DIR}/${GEOMESA_DEPENDENCIES}', aborting script"
  exit 2
fi

function download_dependencies() {
  local install_dir="${%%tools.dist.name%%_HOME}/lib"
  if [[ -n "$1" && "$1" != "--no-prompt" ]]; then
    install_dir="$1"
    shift
  fi

  local excludes
  excludes="$(exclude_dependencies)"
  if [[ -n "$excludes" ]]; then
    local jars=()
    for jar in $excludes; do
      if [[ -f "$install_dir/$jar" ]]; then
        jars+=("$jar")
      fi
    done
    if [[ ${#jars[@]} -gt 0 ]]; then
      echo >&2 "Found conflicting JAR(s):"
      echo >&2 "  ${jars[*]// /\n  /}"
      echo >&2 ""
      confirm="yes"
      if [[ "$1" != "--no-prompt" ]]; then
        read -r -p "Remove them? (y/n) " confirm
        confirm=${confirm,,} # lower-casing
      fi
      if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
        for jar in "${jars[@]}"; do
          rm -f "$install_dir/$jar"
        done
      fi
    fi
  fi

  local includes
  includes="$(dependencies)"
  local classpath=""
  if [[ -d "$install_dir" ]]; then
    classpath="$(ls "$install_dir" | tr '\n' ':')"
  fi
  local gavs=()
  for gav in $includes; do
    group="${gav%%:*}"
    artifact="${gav#$group:}"
    artifact="${artifact%%:*}"
    if [[ ! $classpath =~ (^|:|/)${artifact}(-[^:]*)*\.jar ]]; then
      gavs+=("$gav")
    fi
  done

  if [[ ${#gavs[@]} -gt 0 ]]; then
    download_maven "$install_dir" gavs[@] "Preparing to install the following artifacts into $install_dir:"$'\n' "$1"
    exit $?
  else
    echo >&2 "All required dependencies already exist in $install_dir"
  fi
}

download_dependencies "$@"
