#!/usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# Common functions for GeoMesa command line tools
# Set environment variables in bin/geomesa-env.sh
# Set $GEOMESA_CONF_DIR before running to use alternate configurations

function setGeoLog() {
  if [[ -z "${GEOMESA_LOG_DIR}" ]]; then
    export GEOMESA_LOG_DIR="${%%gmtools.dist.name%%_HOME}/logs"
  fi
  if [[ ! -d "${GEOMESA_LOG_DIR}" ]]; then
    mkdir -p "${GEOMESA_LOG_DIR}"
  fi
  export GEOMESA_OPTS="-Dgeomesa.log.dir=${GEOMESA_LOG_DIR} ${GEOMESA_OPTS}"
}

# findJars [path] [bool: remove slf4j jars] [bool: do not descend into sub directories]
function findJars() {
  home="$1"
  CP=()
  if [[ -d "${home}" ]]; then
    if [[ "$3" == "true" ]]; then
      for jar in $(find -L ${home} -maxdepth 1 -iname "*.jar" -type f); do
        if [[ "$jar" != *-sources.jar && ("$2" != "true" || "$jar" != *slf4j*) ]]; then
          CP+=(${jar})
        fi
      done
    else
      for jar in $(find -L ${home} -type f -iname "*.jar"); do
        if [[ "$jar" != *-sources.jar && ("$2" != "true" || "$jar" != *slf4j*) ]]; then
          CP+=(${jar})
        fi
      done
    fi
    if [[ -d "${home}/native" ]]; then
      if [[ -z "${JAVA_LIBRARY_PATH}" ]]; then
        JAVA_LIBRARY_PATH="${home}/native"
      else
        JAVA_LIBRARY_PATH="${home}/native:${JAVA_LIBRARY_PATH}"
      fi
    fi
  fi
  ret=$(IFS=: ; echo "${CP[*]}")
  echo "$ret"
}

#############################################################
# Download a list of urls to a destination - takes three args:
#  - destination directory
#  - list of urls passed as an array reference
#  - --no-prompt [optional] bypass confirmation. Useful when
#    you'd like to provide your own confirmation
#    messages/handling
#
# for example:
#   urls=("1" "2" "3")
#   downloadURLS /tmp/foobar urls[@]
# or
#   downloadURLS /tmp/foobar urls[@] --no-prompt
#############################################################
function downloadUrls() {
  local dest=$1
  # requires that the urls be passed in with the syntax urls[@]
  # old fashioned bash before local -n (namerefs) which came in bash 4.3
  local urls=("${!2}")
  local noPrompt=$3

  echo "Downloading the following ${#urls[@]} files to '${dest}':"
  for url in "${urls[@]}"; do
    echo "  $url"
  done
  if [[ "${noPrompt}" != "--no-prompt" ]]; then
    read -r -p "Continue? (y/n) " confirm
    confirm=${confirm,,} # lower-casing
  else
    confirm="yes"
  fi

  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    mkdir -p "$dest"
    declare -a downloads=()
    for url in "${urls[@]}"; do
      fname="$(basename "$url")" # filename we'll save to
      tmpfile=$(mktemp)
      # -sS disables progress meter but keeps error messages, -f don't save failed files, -o write to destination file
      downloads+=("(echo fetching $fname && curl -sSfo '$tmpfile' '$url' && mv '$tmpfile' '${dest}/${fname}' && chmod 644 '${dest}/${fname}') || echo [ERROR] Failed to fetch $fname")
    done
    # pass to xargs to run with 4 threads
    # delimit with null char to avoid issues with spaces
    # execute in a new shell to allow for multiple commands per file
    printf "%s\0" "${downloads[@]}" | xargs -P 4 -n 1 -0 sh -c
    error=$?
    if [[ "$error" != "0" ]]; then
      echo "Error: failed to download some dependencies"
      return $error
    fi
  else
    echo "Download cancelled"
  fi
}

# Combine two arguments into a classpath (aka add a : between them)
# Handles if either argument is empty
# TODO in the future take a variable number of arguments and combine them all
function combineClasspaths() {
  local first=$1
  local second=$2
  if [[ -n "${first}" && -n "${second}" ]]; then
    echo "${first}:${second}"
  elif [[ -n "${first}" && ! -n "${second}" ]]; then
    echo "${first}"
  elif [[ ! -n "${first}" && -n "${second}" ]]; then
    echo "${second}"
  else
    echo ""
  fi
}

function filterSLF4J() {
  base="$1"
  CP=()
  for jar in $(find -L ${base} -maxdepth 1 -type f -name "*.jar"); do
    if [[ "$jar" != *"slf4j"* ]]; then
      CP+=(${jar})
    fi
  done
  ret=$(IFS=: ; echo "${CP[*]}")
  echo "$ret"
}

# Expand a classpath out by running findjars on directories
# TODO check the dirs for log files and retain * dirs if they have no logging jars
# so that we don't explode things out as much
function excludeLogJarsFromClasspath() {
 local classpath="$1"
 local filtered=()
 for e in ${classpath//:/ }; do
   if [[ $e = *\* ]]; then
     if [[ -d "$e" && -n "$(ls $e | grep slf4j)" ]]; then
       filtered+=($(filterSLF4J "$e"))
     else
       filtered+=("$e")
     fi
   elif [[ $e != *"slf4j"*jar ]]; then
     filtered+=("$e")
   fi
 done
 ret=$(IFS=: ; echo "${filtered[*]}")
 echo "$ret"
}


function setHadoopClasspath() {

  # First check to see if the GEOMESA_HADOOP_CLASSPATH is already set
  # and if so ignore it
  if [[ -n "${GEOMESA_HADOOP_CLASSPATH}" ]]; then
    return
  fi

  if [[ -n "$HADOOP_HOME" ]]; then
    if [[ -z "$HADOOP_CONF_DIR" && -d "${HADOOP_HOME}/etc/hadoop" ]]; then
      HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
    fi
  fi

  # Lastly, do a bunch of complicated guessing
  # Get the hadoop jars, ignoring jars with names containing slf4j and test
  # Copied from accumulo classpath
  if [[ "$hadoopCDH" == "1" ]]; then
    # Hadoop CDH configuration
    hadoopDirs=(
      $HADOOP_HOME
      $HADOOP_CONF_DIR
      $HADOOP_COMMON_HOME
      $HADOOP_HDFS_HOME
      $YARN_HOME
      $HADOOP_MAPRED_HOME
      $HADOOP_CUSTOM_CP
    )
  else
    hadoopDirs=(
      # Hadoop 2 requirements
      $HADOOP_HOME/share/hadoop/common
      $HADOOP_HOME/share/hadoop/hdfs/
      $HADOOP_HOME/share/hadoop/mapreduce/
      $HADOOP_HOME/share/hadoop/tools/lib
      $HADOOP_HOME/share/hadoop/yarn/
      # HDP 2.0 requirements
      /usr/lib/hadoop/
      /usr/lib/hadoop-hdfs/
      /usr/lib/hadoop-mapreduce/
      /usr/lib/hadoop-yarn/
      # HDP 2.2 requirements
      /usr/hdp/current/hadoop-client/
      /usr/hdp/current/hadoop-hdfs-client/
      /usr/hdp/current/hadoop-mapreduce-client/
      /usr/hdp/current/hadoop-yarn-client/
      # IOP 4.1 requirements
      /usr/iop/current/hadoop-client/
      /usr/iop/current/hadoop-hdfs-client/
      /usr/iop/current/hadoop-mapreduce-client/
      /usr/iop/current/hadoop-yarn-client/
    )
  fi

  for home in ${hadoopDirs[*]}; do
    tmp="$(findJars $home true)"
    if [[ -n "$tmp" ]]; then
      HADOOP_CP="$HADOOP_CP:$tmp"
    fi
  done

  if [[ -n "${HADOOP_CONF_DIR}" && -n "${HADOOP_CP}" ]]; then
    HADOOP_CP="${HADOOP_CP}:${HADOOP_CONF_DIR}"
  fi

  if [[ "${HADOOP_CP:0:1}" = ":" ]]; then
    HADOOP_CP="${HADOOP_CP:1}"
  fi

  # Next attempt to cheat by stealing the classpath from the hadoop command
  if [[ -z "${HADOOP_CP}" && -n "$(command -v hadoop)" ]]; then
    HADOOP_CP=$(hadoop classpath)
  fi

  export GEOMESA_HADOOP_CLASSPATH="${HADOOP_CP}"
}

function geomesaConfigure() {
  echo >&2 "Using %%gmtools.dist.name%%_HOME as set: $%%gmtools.dist.name%%_HOME"
  read -p "Is this intentional? Y\n " -n 1 -r
  if [[  $REPLY =~ ^[Nn]$ ]]; then
    bin="$( cd -P "$( dirname "${SOURCE}" )" && cd ../ && pwd )"
    export %%gmtools.dist.name%%_HOME="$bin"
    echo >&2 ""
    echo "Now set to ${%%gmtools.dist.name%%_HOME}"
  fi

  if [[ -z "$GEOMESA_LIB" ]]; then
    GEOMESA_LIB=${%%gmtools.dist.name%%_HOME}/lib
  elif containsElement "GEOMESA_LIB" "${existingEnvVars[@]}"; then
    message="Warning: GEOMESA_LIB already set, probably by a prior configuration."
    message="${message}\n Current value is ${GEOMESA_LIB}."
    echo >&2 ""
    echo -e >&2 "$message"
    echo >&2 ""
    read -p "Is this intentional? Y\n " -n 1 -r
    if [[  $REPLY =~ ^[Nn]$ ]]; then
      GEOMESA_LIB=${%%gmtools.dist.name%%_HOME}/lib
      echo >&2 ""
      echo "Now set to ${GEOMESA_LIB}"
    fi
    echo >&2 ""
  fi

  echo >&2 ""
  echo "To persist the configuration please edit conf/geomesa-env.sh or update your bashrc file to include: "
  echo "export %%gmtools.dist.name%%_HOME="$%%gmtools.dist.name%%_HOME""
  echo "export PATH=\${%%gmtools.dist.name%%_HOME}/bin:\$PATH"

  registerAutocomplete
}

function containsElement() {
  local element
  for element in "${@:2}"; do [[ "$element" == "$1" ]] && return 0; done
  return 1
}

function registerAutocomplete() {
  echo "Do you want to register Autocomplete?"
  echo "(This requires ~/.bash_completion exists or is creatable.)"
  read -p "Use default [Y/n] or enter path: " -r
  if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    if [[ $REPLY =~ ^[Yy]$ || $REPLY == "" ]]; then
      eval compFile="${HOME}/.bash_completion"
    else
      compFile=$REPLY
    fi
    [[ -f ${compFile} ]] || touch ${compFile}
    # Search .bash_completion for this entry so we don't add it twice
    head=$(head -n 1 ${GEOMESA_CONF_DIR}/autocomplete.sh)
    res=$(grep -F $head ${compFile})
    if [[ -z "${res}" ]]; then
      echo "Installing Autocomplete Function"
      cat ${GEOMESA_CONF_DIR}/autocomplete.sh >> ${compFile} 2> /dev/null
      echo "Autocomplete function available, to use now run:"
      echo ". ${compFile}"
    else
      echo "Autocomplete Function appears to already be installed."
    fi
  fi
}

function geomesaScalaConsole() {
  classpath=${1}
  shift 1
  OPTS=${@}
  # Check if we already downloaded scala
  if [[ -d "${%%gmtools.dist.name%%_HOME}/dist/scala-%%scala.version%%/" ]]; then
    scalaCMD="${%%gmtools.dist.name%%_HOME}/dist/scala-%%scala.version%%/bin/scala"
  else
    sourceURL=("https://downloads.lightbend.com/scala/%%scala.version%%/scala-%%scala.version%%.tgz")
    outputDir="${%%gmtools.dist.name%%_HOME}/dist/"
    outputFile="${outputDir}/scala-%%scala.version%%.tgz"

    if which scala > /dev/null; then
      scalaCMD=$(which scala)
      version=$(scala -version 2>&1 | grep %%scala.binary.version%%)
      if [[ -z "${version}" ]]; then
        read -p "The wrong Scala version is installed, do you want to download the correct one? (This will not affect your current install) Y\n" -n 1 -r
        if [[  $REPLY =~ ^[Yy]$ || $REPLY == "" ]]; then
          echo >&2 ""
          downloadUrls ${outputDir} sourceURL[@] --no-prompt
          tar xf $outputFile -C "${%%gmtools.dist.name%%_HOME}/dist/"
          scalaCMD="${%%gmtools.dist.name%%_HOME}/dist/scala-%%scala.version%%/bin/scala"
        else
          echo "\nThe correct Scala version, Scala %%scala.binary.version%%, is required to continue."
          exit 1
        fi
      fi
    else
      read -p "Scala not installed, do you want to download it? (This will not install it) Y\n" -n 1 -r
      if [[  $REPLY =~ ^[Yy]$ || $REPLY == "" ]]; then
        echo >&2 ""
        downloadUrls ${outputDir} sourceURL[@] --no-prompt
        tar xf $outputFile -C "${%%gmtools.dist.name%%_HOME}/dist/"
        scalaCMD="${%%gmtools.dist.name%%_HOME}/dist/scala-%%scala.version%%/bin/scala"
      else
        echo "\nScala is required to run the console."
        exit 1
      fi
    fi
  fi

  exec $scalaCMD ${OPTS} -classpath ${classpath} -i "${GEOMESA_CONF_DIR}/.scala_repl_init"
}

# Reconfigure %%gmtools.dist.name%%_HOME
if [[ $1 = configure ]]; then
  echo >&2 "Using %%gmtools.dist.name%%_HOME = $%%gmtools.dist.name%%_HOME"
  read -p "Do you want to reset this? Y\n " -n 1 -r
  if [[  $REPLY =~ ^[Yy]$ || $REPLY == "" ]]; then
    echo >&2 ""
    geomesaConfigure
  fi
  echo >&2 ""
fi

# Define GEOMESA_CONF_DIR so we can find geomesa-env.sh
if [[ -z "$GEOMESA_CONF_DIR" ]]; then
  GEOMESA_CONF_DIR=${%%gmtools.dist.name%%_HOME}/conf
  if [[ ! -d "$GEOMESA_CONF_DIR" ]]; then
    message="Warning: Unable to locate GeoMesa config directory"
    message="${message}\n The current value is ${GEOMESA_CONF_DIR}."
    echo >&2 ""
    echo -e >&2 "$message"
    echo >&2 ""
    read -p "Do you want to continue? Y\n " -n 1 -r
    if [[  $REPLY =~ ^[Yy]$ || $REPLY == "" ]]; then
      echo "Continuing without configuration, functionality will be limited"
    else
      message="You may set this value manually using 'export GEOMESA_CONF_DIR=/path/to/dir'"
      message="${message} and running this script again."
      echo >&2 ""
      echo -e >&2 "$message"
      echo >&2 ""
      exit -1
    fi
    echo >&2 ""
  fi
elif [[ $1 = configure ]]; then
  message="Warning: GEOMESA_CONF_DIR was already set, probably by a prior configuration."
  message="${message}\n The current value is ${GEOMESA_CONF_DIR}."
  echo >&2 ""
  echo -e >&2 "$message"
  echo >&2 ""
  read -p "Do you want to reset this to ${%%gmtools.dist.name%%_HOME}/conf? Y\n " -n 1 -r
  if [[  $REPLY =~ ^[Yy]$ || $REPLY == "" ]]; then
    GEOMESA_CONF_DIR=${%%gmtools.dist.name%%_HOME}/conf
    echo >&2 ""
    echo "Now set to ${GEOMESA_CONF_DIR}"
  fi
  echo >&2 ""
fi

# Find geomesa-env and load config
GEOMESA_ENV=${GEOMESA_CONF_DIR}/geomesa-env.sh
if [[ -f "$GEOMESA_ENV" ]]; then
  . ${GEOMESA_ENV}
  if [[ "${#existingEnvVars[@]}" -ge "1" ]]; then
    echo "The following variables were not loaded from ${GEOMESA_ENV} due to an existing configuration."
    for i in "${existingEnvVars[@]}"; do echo "$i"; done
  fi
elif [[ -d "$GEOMESA_CONF_DIR" ]]; then
  # If the directory doesn't exist then we already warned about this.
  message="Warning: geomesa-env configuration file not found in ${GEOMESA_CONF_DIR}."
fi

# GEOMESA paths, GEOMESA_LIB should live inside %%gmtools.dist.name%%_HOME, but can be pointed elsewhere in geomesa-env
if [[ -z "$GEOMESA_LIB" ]]; then
  GEOMESA_LIB=${%%gmtools.dist.name%%_HOME}/lib
elif [[ $1 = configure ]] && containsElement "GEOMESA_LIB" "${existingEnvVars[@]}"; then
  message="Warning: GEOMESA_LIB was already set, probably by a prior configuration or the geomesa-env config."
  message="${message}\n The current value is ${GEOMESA_LIB}."
  echo >&2 ""
  echo -e >&2 "$message"
  echo >&2 ""
  read -p "Do you want to reset this to ${%%gmtools.dist.name%%_HOME}/lib? Y\n " -n 1 -r
  if [[  $REPLY =~ ^[Yy]$ || $REPLY == "" ]]; then
    GEOMESA_LIB=${%%gmtools.dist.name%%_HOME}/lib
    echo >&2 ""
    echo "Now set to ${GEOMESA_LIB}"
  fi
  echo >&2 ""
fi

NL=$'\n'

# Set GeoMesa parameters
GEOMESA_OPTS="-Duser.timezone=UTC -DEPSG-HSQL.directory=/tmp/$(whoami)"
GEOMESA_OPTS="${GEOMESA_OPTS} -Djava.awt.headless=true"
GEOMESA_OPTS="${GEOMESA_OPTS} -Dlog4j.configuration=file://${GEOMESA_CONF_DIR}/log4j.properties"
GEOMESA_OPTS="${GEOMESA_OPTS} -Dgeomesa.home=${%%gmtools.dist.name%%_HOME}"
GEOMESA_DEBUG_OPTS="-Xmx8192m -XX:-UseGCOverheadLimit -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=9898"
GEOMESA_CP=""

# Configure geomesa logging directory this can be set in geomesa-env
setGeoLog

# Configure Java Library Path this can be set in geomesa-env.
if [[ -n "$JAVA_LIBRARY_PATH" ]]; then
  GEOMESA_OPTS="${GEOMESA_OPTS} -Djava.library.path=${JAVA_LIBRARY_PATH}"
  export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$JAVA_LIBRARY_PATH"
fi

# Configure Java Options this can be set in geomesa-env.
if [[ -z "$CUSTOM_JAVA_OPTS" ]]; then
  export CUSTOM_JAVA_OPTS="${JAVA_OPTS}"
fi
