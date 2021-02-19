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

# lib directory, by default $%%gmtools.dist.name%%_HOME/lib
export GEOMESA_LIB="${GEOMESA_LIB:-$%%gmtools.dist.name%%_HOME/lib}"

# logs directory, by default $%%gmtools.dist.name%%_HOME/logs
# this directory needs to be writable - in multi-tenant environments, each user should specify this differently
export GEOMESA_LOG_DIR="${GEOMESA_LOG_DIR:-$%%gmtools.dist.name%%_HOME/logs}"

# debug options passed to the java process when invoked with 'debug'
export GEOMESA_DEBUG_OPTS="-Xmx8192m -XX:-UseGCOverheadLimit -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=9898"

# ==================================================================
# Java Environment Variables
# ==================================================================

# add additional resources to the classpath, including the distributed classpath for m/r jobs
# export GEOMESA_EXTRA_CLASSPATHS="${GEOMESA_EXTRA_CLASSPATHS:-/some/dir/:/another/dir/}"

# add additional native libraries to the java classpath (LD_LIBRARY_PATH)
# export JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH:-/path/to/java/native/library}"

# additional java process options, includes JAVA_OPTS by default
export CUSTOM_JAVA_OPTS="${CUSTOM_JAVA_OPTS} $JAVA_OPTS"

newline=$'\n'

# setup opts for invoking the geomesa java process
function get_options() {
    # create log dir if needed
  if [[ ! -d "${GEOMESA_LOG_DIR}" ]]; then
    mkdir -p "${GEOMESA_LOG_DIR}"
  fi

  # set java opts
  local GEOMESA_OPTS="-Duser.timezone=UTC -DEPSG-HSQL.directory=/tmp/$(whoami)"
  GEOMESA_OPTS="${GEOMESA_OPTS} -Djava.awt.headless=true"
  GEOMESA_OPTS="${GEOMESA_OPTS} -Dlog4j.configuration=file://${GEOMESA_CONF_DIR}/log4j.properties"
  GEOMESA_OPTS="${GEOMESA_OPTS} -Dgeomesa.home=${%%gmtools.dist.name%%_HOME}"
  GEOMESA_OPTS="${GEOMESA_OPTS} -Dgeomesa.log.dir=${GEOMESA_LOG_DIR}"

  # configure java library path
  if [[ -n "$JAVA_LIBRARY_PATH" ]]; then
    GEOMESA_OPTS="${GEOMESA_OPTS} -Djava.library.path=${JAVA_LIBRARY_PATH}"
    export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$JAVA_LIBRARY_PATH"
  fi

  if [[ $1 = debug ]]; then
    GEOMESA_OPTS="${GEOMESA_OPTS} ${GEOMESA_DEBUG_OPTS}"
  fi

  echo "$CUSTOM_JAVA_OPTS $GEOMESA_OPTS"
}

# get base classpath (geomesa lib and conf dirs)
function get_base_classpath() {
  # start constructing GEOMESA_CP (classpath)
  # include geomesa first so that the correct log4j.properties is picked up
  local GEOMESA_CP="${GEOMESA_CONF_DIR}:$(find_jars $GEOMESA_LIB)"
  # prepend user defined directories to the classpath using java classpath syntax
  # we prepend so that they take precedence when explicitly defined by the user
  if [[ -n "${GEOMESA_EXTRA_CLASSPATHS}" ]]; then
    GEOMESA_CP="${GEOMESA_EXTRA_CLASSPATHS}:${GEOMESA_CP}"
  fi
  echo "$GEOMESA_CP"
}

# extract the version of a jar from a classpath string
# args: jar base name, classpath, default version if not found
function get_classpath_version() {
  local version="$(expr match "$2" ".*$1-\([^:/][^:/]*\)\.jar.*")"
  if [[ -z "$version" ]]; then
    version="$3"
  fi
  echo "$version"
}

# find_jars [path] [bool: do not descend into sub directories]
function find_jars() {
  local home="$1"
  local jars=()
  if [[ -d "${home}" ]]; then
    local depth=""
    if [[ "$2" == "true" ]]; then
      depth="-maxdepth 1"
    fi
    for jar in $(find -L ${home} ${depth} -iname "*.jar" -type f); do
      if [[ "$jar" != *-sources.jar && "$jar" != *-tests.jar ]]; then
        jars+=(${jar})
      fi
    done
    if [[ -d "${home}/native" ]]; then
      if [[ -z "${JAVA_LIBRARY_PATH}" ]]; then
        JAVA_LIBRARY_PATH="${home}/native"
      else
        JAVA_LIBRARY_PATH="${home}/native:${JAVA_LIBRARY_PATH}"
      fi
    fi
  fi
  ret=$(IFS=: ; echo "${jars[*]}")
  echo "$ret"
}

# args: destination for missing jars, current classpath, gavs, '--no-prompt'
# returns: 0 classpath is ok, 1 user cancelled, 2 classpath was updated, 3+ error code
function check_classpath() {
  echo -n >&2 "Initial run - checking classpath... "

  local dest="$1"
  local classpath="$2"
  local includes="$3"
  local noprompt="$4"

  local missing=()

  for gav in ${includes}; do
    group="${gav%%:*}"
    artifact="${gav#$group:}"
    artifact="${artifact%%:*}"
    if [[ ! $classpath =~ (^|:|/)${artifact}(-[^:]*)*\.jar ]]; then
      missing+=("$gav")
    fi
  done

  if [[ ${#missing[@]} -eq 0 ]]; then
    echo >&2 "classpath OK"
    # suppress future checks
    disable_classpath_checks
    return 0
  else
    echo >&2 "" # get the newline
    if [[ "${noprompt}" = "--no-prompt" ]]; then
      echo >&2 "Detected missing classpath entries:"
      download_maven $dest missing[@] "--no-prompt"
    else
      download_maven $dest missing[@] "Detected missing classpath entries:"
    fi
    error=$?
    if [[ $error -eq 0 ]]; then
      # successfully downloaded the jars, disable checks going forward
      disable_classpath_checks
      return 1
    elif [[ $error -eq 1 ]]; then
      # user cancelled download
      read -r -p "Would you like to suppress future classpath checks (y/n)? " confirm
      confirm=${confirm,,} # lower-casing
      if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
        disable_classpath_checks
      fi
      return 2
    else
      return 3
    fi
  fi
}

function disable_classpath_checks() {
  if [[ -n "$(grep '^export GEOMESA_CHECK_DEPENDENCIES' "${GEOMESA_CONF_DIR}/geomesa-env.sh")" ]]; then
    sed 's/^export GEOMESA_CHECK_DEPENDENCIES.*/export GEOMESA_CHECK_DEPENDENCIES="false"/' \
      "${GEOMESA_CONF_DIR}/geomesa-env.sh" > tmp && mv tmp "${GEOMESA_CONF_DIR}/geomesa-env.sh" \
      && echo >&2 "You may re-enable classpath checks by setting GEOMESA_CHECK_DEPENDENCIES=true in ${GEOMESA_CONF_DIR}/geomesa-env.sh$newline"
  else
    echo >&2 "You may disable classpath checks by setting GEOMESA_CHECK_DEPENDENCIES=false in ${GEOMESA_CONF_DIR}/geomesa-env.sh$newline"
  fi
}

# args: destination, gavs array, prompt (or --no-prompt)
# return: 0 ok, 1 if user cancelled, 2+ if error
function download_maven() {
  local dest=$1
  # requires that the urls be passed in with the syntax urls[@]
  # old fashioned bash before local -n (namerefs) which came in bash 4.3
  local gavs=("${!2}")
  local prompt=$3

  local urls=()

  for gav in "${gavs[@]}"; do
    group="${gav%%:*}"
    artifact="${gav#$group:}"
    artifact="${artifact%%:*}"
    version="${gav#$group:$artifact:}"
    version="${version%%:*}"
    type="${gav#$group:$artifact:$version:}"
    urls+=("${GEOMESA_MAVEN_URL}${group//.//}/${artifact}/${version}/${artifact}-${version}.${type}")
    names+="$newline  $gav"
  done

  local repo="$(expr match "$GEOMESA_MAVEN_URL" 'https*://\([^/]*\)/.*')"
  if [[ -z "$repo" || "$repo" = "0" ]]; then
    repo = "$GEOMESA_MAVEN_URL"
  fi

  local confirm="yes"
  if [[ "${prompt}" = "--no-prompt" ]]; then
    echo >&2 "Downloading the following artifacts from $repo:$names"
  else
    echo >&2 "$prompt$names$newline"
    echo >&2 "WARNING: ENSURE THAT JAR VERSIONS MATCH YOUR ENVIRONMENT"
    read -r -p "Would you like to download them from '$repo' (y/n)? " confirm
    confirm=${confirm,,} # lower-casing
  fi

  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    download_urls "$dest" urls[@]
    error=$?
    if [[ $error -eq 0 ]]; then
      echo >&2 "download complete$newline"
      return 0
    else
      return 2
    fi
  else
    return 1
  fi
}

# download a list of urls to a directory
# args: destination directory, list of urls passed as array reference (e.g. urls[@])
function download_urls() {
  local dest=$1
  # requires that the urls be passed in with the syntax urls[@]
  # old fashioned bash before local -n (namerefs) which came in bash 4.3
  local urls=("${!2}")

  mkdir -p "$dest"
  local downloads=()
  for url in "${urls[@]}"; do
    fname="$(basename "$url")" # filename we'll save to
    tmpfile=$(mktemp)
    # -sS disables progress meter but keeps error messages, -f don't save failed files, -o write to destination file, -L follow redirects
    downloads+=("(echo >&2 fetching $fname && curl -LsSfo '$tmpfile' '$url' && mv '$tmpfile' '${dest}/${fname}' && chmod 644 '${dest}/${fname}') || echo [ERROR] Failed to fetch $fname")
  done
  # pass to xargs to run with 4 threads
  # delimit with null char to avoid issues with spaces
  # execute in a new shell to allow for multiple commands per file
  printf "%s\0" "${downloads[@]}" | xargs -P 4 -n 1 -0 sh -c
  return $?
}

# expand wildcards in a classpath to resolve the actual jar names
# args: classpath
function expand_classpath() {
  local classpath="$1"
  local expanded=""

  for e in ${classpath//:/ }; do
    if [[ $e = *\* && -d "${e%\*}" ]]; then
      for f in ls $e; do
        expanded+=":${e%\*}$f"
      done
    else
     expanded+=":$e"
    fi
  done

  echo "${expanded:1}"
}

# remove leading/trailing/double colons
# args: classpath
function fix_classpath_format() {
  echo "$1" | sed -e 's/:\{1,\}/:/g' -e 's/^://g' -e 's/:$//g'
}

# remove slf4j jars from a classpath string
function remove_slf4j_from_classpath() {
  echo "$1" | sed 's/[^:]*slf4j[^:]*jar//g'
}

function geomesa_scala_console() {
  classpath=${1}
  shift 1
  OPTS=${@}

  # Check if we already downloaded scala
  if [[ -d "${%%gmtools.dist.name%%_HOME}/dist/scala-%%scala.version%%/" ]]; then
    scalaCMD="${%%gmtools.dist.name%%_HOME}/dist/scala-%%scala.version%%/bin/scala"
  elif [[ $(which scala > /dev/null) -eq 0 && -n "$(scala -version 2>&1 | grep %%scala.binary.version%%)" ]]; then
    scalaCMD="scala"
  else
    read -r -p "Download scala %%scala.binary.version%% (y/n)? " confirm
    confirm=${confirm,,} # lower-casing
    if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
      sourceURL=("https://downloads.lightbend.com/scala/%%scala.version%%/scala-%%scala.version%%.tgz")
      outputDir="${%%gmtools.dist.name%%_HOME}/dist/"
      outputFile="${outputDir}/scala-%%scala.version%%.tgz"
      download_urls ${outputDir} sourceURL[@]
      if [[ $? -ne 0 ]]; then
        exit 1
      fi
      tar xf $outputFile -C "${%%gmtools.dist.name%%_HOME}/dist/"
      scalaCMD="${%%gmtools.dist.name%%_HOME}/dist/scala-%%scala.version%%/bin/scala"
    else
      echo >&2 "Please install Scala version %%scala.binary.version%% and re-run this script"
      exit 1
    fi
  fi

  exec $scalaCMD ${OPTS} -classpath ${classpath} -i "${GEOMESA_CONF_DIR}/.scala_repl_init"
}

function geomesa_configure() {
  echo >&2 "Current configuration:"
  echo >&2 "  %%gmtools.dist.name%%_HOME=${%%gmtools.dist.name%%_HOME}"
  echo >&2 "  GEOMESA_LIB=${GEOMESA_LIB}"
  echo >&2 "  GEOMESA_LOG_DIR=${GEOMESA_LOG_DIR}"
  echo >&2 " "
  read -r -p "Is this correct (y/n)? " confirm
  local confirm=${confirm,,} # lower-casing
  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    :
  else
    read -r -p "Enter new value for %%gmtools.dist.name%%_HOME: " newHome
    read -r -p "Enter new value for GEOMESA_LIB (default $newHome/lib): " newLib
    if [[ $newLib = "" ]]; then
      newLib="$newHome/lib"
    fi
    read -r -p "Enter new value for GEOMESA_LOG_DIR (default $newHome/logs): " $newLog
    if [[ $newLog = "" ]]; then
      $newLog="$newHome/logs"
    fi
    %%gmtools.dist.name%%_HOME="${newHome}"
    GEOMESA_LIB="${newLib}"
    GEOMESA_LOG_DIR="${newLog}"
  fi

  confirm="no"
  if [[ -f "~/.bashrc" ]]; then
    read -r -p "Persist environment to ~/.bashrc (y/n)? " confirm
    confirm=${confirm,,} # lower-casing
  fi
  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    echo "export %%gmtools.dist.name%%_HOME=\"$%%gmtools.dist.name%%_HOME\"" >> ~/.bashrc
    echo "export GEOMESA_LIB=\"${GEOMESA_LIB}\"" >> ~/.bashrc
    echo "export GEOMESA_LOG_DIR=\"${GEOMESA_LOG_DIR}\"" >> ~/.bashrc
    echo "export PATH=\${%%gmtools.dist.name%%_HOME}/bin:\$PATH" >> ~/.bashrc
  else
    echo >&2 "To put $(basename $0) on the executable path, add the following line to your environment:"
    echo >&2 "export %%gmtools.dist.name%%_HOME=\"$%%gmtools.dist.name%%_HOME\""
    echo >&2 "export GEOMESA_LIB=\"${GEOMESA_LIB}\""
    echo >&2 "export GEOMESA_LOG_DIR=\"${GEOMESA_LOG_DIR}\""
    echo >&2 "export PATH=\${%%gmtools.dist.name%%_HOME}/bin:\$PATH"
  fi

  read -r -p "Register auto-complete for GeoMesa CLI commands (y/n)? " confirm
  confirm=${confirm,,} # lower-casing
  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    confirm="yes"
    read -r -p "Enter path to .bash_completion (deault ~/.bash_completion): " comp
    if [[ -z "$comp" ]]; then
      comp="~/.bash_completion"
    fi
    if [[ -f "$comp" ]]; then
      # search .bash_completion for this entry so we don't add it twice
      head="$(head -n 1 ${GEOMESA_CONF_DIR}/autocomplete.sh)"
      res="$(grep -F $head ${comp})"
      if [[ -n "${res}" ]]; then
        echo >&2 "Auto-complete function already installed"
        confirm="no"
      fi
    fi
    if [[ "$confirm" = "yes" ]]; then
      [[ -f ${comp} ]] || touch ${comp}
      cat ${GEOMESA_CONF_DIR}/autocomplete.sh >> ${comp}
      echo >&2 "Auto-complete installed, to use now run:"
      echo >&2 ". ${comp}"
    fi
  fi
}
