#!/usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# https://www.apache.org/licenses/LICENSE-2.0
#

# This script provides setup functions

# checks a version string is >= a test
function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

# extract the version of a jar from a classpath string
# args: jar base name, classpath, default version if not found
function get_classpath_version() {
  local version
  version="$([[ "$2" =~ .*$1-([^:/][^:/]*)\.jar.* ]] && echo "${BASH_REMATCH[1]}")"
  if [[ -z "$version" ]]; then
    version="$3"
  fi
  echo "$version"
}

# find_jars [path] [bool: do not descend into sub directories]
function find_jars() {
  local home="$1"
  local jars=""
  if [[ -d "${home}" ]]; then
    local find_args
    find_args=("-type" "f" "-iname" "*.jar" "-not" "-iname" "*-sources.jar" "-not" "-iname" "*-tests.jar")
    if [[ "$2" == "true" ]]; then
      find_args=("-maxdepth" "1" "${find_args[@]}")
    fi
    jars="$(find "-L" "$home" "${find_args[@]}" | paste -sd: -)"
    if [[ -d "${home}/native" ]]; then
      # TODO this doesn't export back to the parent shell... fix it
      if [[ -z "${JAVA_LIBRARY_PATH}" ]]; then
        JAVA_LIBRARY_PATH="${home}/native"
      else
        JAVA_LIBRARY_PATH="${home}/native:${JAVA_LIBRARY_PATH}"
      fi
    fi
  fi
  echo "$jars"
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
      download_maven "$dest" missing[@] "--no-prompt"
    else
      download_maven "$dest" missing[@] "Detected missing classpath entries:"
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
      return 0
    else
      return 3
    fi
  fi
}

function disable_classpath_checks() {
  if grep -q '^export GEOMESA_CHECK_DEPENDENCIES' "${GEOMESA_CONF_DIR}/geomesa-env.sh"; then
    sed -i 's/^export GEOMESA_CHECK_DEPENDENCIES.*/export GEOMESA_CHECK_DEPENDENCIES="false"/' "${GEOMESA_CONF_DIR}/geomesa-env.sh"
    echo -e >&2 "You may re-enable classpath checks by setting GEOMESA_CHECK_DEPENDENCIES=true in ${GEOMESA_CONF_DIR}/geomesa-env.sh\n"
  else
    echo -e >&2 "You may disable classpath checks by setting GEOMESA_CHECK_DEPENDENCIES=false in ${GEOMESA_CONF_DIR}/geomesa-env.sh\n"
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
  local names=()

  for gav in "${gavs[@]}"; do
    group="${gav%%:*}"
    artifact="${gav#$group:}"
    artifact="${artifact%%:*}"
    version="${gav#$group:$artifact:}"
    version="${version%%:*}"
    type="${gav#$group:$artifact:$version:}"
    type="${type%%:*}"
    qualifier="${gav#$group:$artifact:$version:$type}"
    if [[ -n "$qualifier" ]]; then
      qualifier="${qualifier/:/-}"
    fi
    urls+=("${GEOMESA_MAVEN_URL}${group//.//}/${artifact}/${version}/${artifact}-${version}${qualifier}.${type}")
    names+=("$gav")
  done

  local repo
  repo="$([[ "$GEOMESA_MAVEN_URL" =~ https*://([^/]*)/.* ]] && echo "${BASH_REMATCH[1]}")"
  if [[ -z "$repo" || "$repo" = "0" ]]; then
    repo="$GEOMESA_MAVEN_URL"
  fi

  local confirm="yes"
  if [[ "${prompt}" = "--no-prompt" ]]; then
    echo >&2 "Downloading the following artifacts from $repo:"
    printf >&2 '%s\n' "${names[@]}"
  else
    echo >&2 "$prompt"
    printf >&2 '%s\n' "${names[@]}"
    echo -e >&2 "\nWARNING: ENSURE THAT JAR VERSIONS MATCH YOUR ENVIRONMENT"
    read -r -p "Would you like to download them from '$repo' (y/n)? " confirm
    confirm=${confirm,,} # lower-casing
  fi

  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    download_urls "$dest" urls[@]
    error=$?
    if [[ $error -eq 0 ]]; then
      echo -e >&2 "download complete\n"
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

  for entry in ${classpath//:/ }; do
    if [[ $entry = *\* && -d "${entry%\*}" ]]; then
      expanded+=":$(find "-L" "${entry%\*}" "-maxdepth" "1" "-type" "f" "-iname" "*.jar" "-not" "-iname" "*-sources.jar" "-not" "-iname" "*-tests.jar" | paste -sd: -)"
    else
     expanded+=":$entry"
    fi
  done

  fix_classpath_format "${expanded:1}"
}

# remove leading/trailing/double colons
# args: classpath
function fix_classpath_format() {
  echo "$1" | sed -e 's/:\{1,\}/:/g' -e 's/^://g' -e 's/:$//g'
}

# remove slf4j jars from a classpath string
function remove_slf4j_from_classpath() {
  echo "$1" | sed -E 's/[^:]*slf4j[^:]*jar//g'
}

# remove log4j1 jars from a classpath string
function remove_log4j1_from_classpath() {
  echo "$1" | sed -E 's/[^:]*log4j-1[^:]*jar//g'
}

function geomesa_scala_console() {
  classpath=${1}
  shift 1
  OPTS=("$@")

  # Check if we already downloaded scala
  if [[ -d "${%%tools.dist.name%%_HOME}/dist/scala-%%scala.version%%/" ]]; then
    scalaCMD="${%%tools.dist.name%%_HOME}/dist/scala-%%scala.version%%/bin/scala"
  elif which scala >/dev/null 2>&1 && scala -version 2>&1 | grep -q %%scala.binary.version%%; then
    scalaCMD="scala"
  else
    read -r -p "Download scala %%scala.binary.version%% (y/n)? " confirm
    confirm=${confirm,,} # lower-casing
    if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
      local sourceURL=("https://downloads.lightbend.com/scala/%%scala.version%%/scala-%%scala.version%%.tgz")
      local outputDir="${%%tools.dist.name%%_HOME}/dist/"
      local outputFile="${outputDir}/scala-%%scala.version%%.tgz"
      if ! download_urls "$outputDir" sourceURL[@]; then
        exit 1
      fi
      tar xf "$outputFile" -C "${%%tools.dist.name%%_HOME}/dist/"
      scalaCMD="${%%tools.dist.name%%_HOME}/dist/scala-%%scala.version%%/bin/scala"
    else
      echo >&2 "Please install Scala version %%scala.binary.version%% and re-run this script"
      exit 1
    fi
  fi

  exec "$scalaCMD" "${OPTS[@]}" -classpath "$classpath" -i "${GEOMESA_CONF_DIR}/.scala_repl_init"
}

function geomesa_configure() {
  read -r -p "Using %%tools.dist.name%%_HOME=${%%tools.dist.name%%_HOME} - continue (y/n)? " confirm
  if [[ ${confirm,,} =~ ^(yes|y) ]]; then # note: ${foo,,} is to lower-case
    :
  else
    echo "Please re-run this script from the appropriate %%tools.dist.name%%_HOME directory"
    exit 1
  fi
  read -r -p "Enter path for GEOMESA_LOG_DIR (default ${GEOMESA_LOG_DIR}): " newLog
  if [[ -n "$newLog" ]]; then
    export GEOMESA_LOG_DIR="${newLog}"
    # re-load env to get the updates
    source "${GEOMESA_CONF_DIR}/geomesa-env.sh"
  fi

  read -r -p "Persist environment to ~/.bashrc (y/n)? " confirm
  if [[ ${confirm,,} =~ ^(yes|y) ]]; then # note: ${foo,,} is to lower-case
    {
    echo "export %%tools.dist.name%%_HOME=\"${%%tools.dist.name%%_HOME}\""
    echo "export GEOMESA_LOG_DIR=\"${GEOMESA_LOG_DIR}\""
    echo "export PATH=\"\${%%tools.dist.name%%_HOME}/bin:\$PATH\""
    } >> ~/.bashrc
  else
    echo >&2 "To put $(basename "$0") on the executable path, add the following line to your environment:"
    echo >&2 "export %%tools.dist.name%%_HOME=\"${%%tools.dist.name%%_HOME}\""
    echo >&2 "export GEOMESA_LOG_DIR=\"${GEOMESA_LOG_DIR}\""
    echo >&2 "export PATH=\"\${%%tools.dist.name%%_HOME}/bin:\$PATH\""
  fi
  "${%%tools.dist.name%%_HOME}/bin/geomesa-%%tools.module%%" autocomplete
}
