#!/usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
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
    export GEOMESA_OPTS="-Dgeomesa.log.dir=${GEOMESA_LOG_DIR} $GEOMESA_OPTS"
  fi
  if [[ ! -d "${GEOMESA_LOG_DIR}" ]]; then
    mkdir "${GEOMESA_LOG_DIR}"
  fi
}

function findJars() {
  # findJars [path] [bool: exclude test and slf2j jars] [bool: do not descend into sub directories]
  home="$1"
  CP=""
  if [[ -n "$home" && -d "$home" ]]; then
    if [[ ("$3" == "true") ]]; then
      for jar in $(find ${home} -maxdepth 1 -name "*.jar"); do
        if [[ ("$2" != "true") || (("$jar" != *"test"*) && ("$jar" != *"slf4j"*)) ]]; then
          if [[ "$jar" = "${jar%-sources.jar}" && "$jar" = "${jar%-test.jar}" ]]; then
            CP="$CP:$jar"
          fi
        fi
      done
    else
      for jar in $(find ${home} -name "*.jar"); do
        if [[ ("$2" != "true") || (("$jar" != *"test"*) && ("$jar" != *"slf4j"*)) ]]; then
          if [[ "$jar" = "${jar%-sources.jar}" && "$jar" = "${jar%-test.jar}" ]]; then
            CP="$CP:$jar"
          fi
        fi
      done
    fi
    if [[ -d "$home/native" ]]; then
      if [[ -z "$JAVA_LIBRARY_PATH" ]]; then
        JAVA_LIBRARY_PATH="$home/native"
      else
        JAVA_LIBRARY_PATH="$home/native:$JAVA_LIBRARY_PATH"
      fi
    fi
  fi
  if [[ "${CP:0:1}" = ":" ]]; then
    CP="${CP:1}"
  fi
  echo $CP
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
  eval compFile="~/.bash_completion" # resolve tilde
  [[ -f ${compFile} ]] || touch ${compFile}
  if [[ -f "${GEOMESA_HOME}/conf/autocomplete.sh" ]]; then
    head=$(head -n 1 ${GEOMESA_HOME}/conf/autocomplete.sh) # Don't use GEOMESA_CONF_DIR as it may not be properly set at this point
    # Search .bash_completion for this entry so we don't add it twice
    res=$(grep -F $head ${compFile})
    if [[ -z "${res}" ]]; then
      echo "Installing Autocomplete Function"
      cat ${GEOMESA_HOME}/conf/autocomplete.sh >> ${compFile}
      echo "Autocomplete function available, to use now run:"
      echo ". ${compFile}"
    fi
  fi
}

# Version check
GEOMESA_TOOLS_VERSION=%%gmtools.version%%
if [[ -d $GEOMESA_CONF_DIR ]]; then
  if [[ -f "${GEOMESA_CONF_DIR}/geomesa-env.sh" ]]; then
    GEOMESA_CONF_DIR_TOOLS_VERSION="`cat ${GEOMESA_CONF_DIR}/geomesa-env.sh | grep GEOMESA_TOOLS_VERSION | sed 's/GEOMESA_TOOLS_VERSION//'`"
  else GEOMESA_CONF_DIR_TOOLS_VERSION=""; fi # legacy support
  if [[ $GEOMESA_TOOLS_VERSION != $GEOMESA_CONF_DIR_TOOLS_VERSION ]]; then
    echo "Warning: Using GEOMESA_CONF_DIR = ${GEOMESA_CONF_DIR} does not match Geomesa tools version."
  fi
fi
if [[ -d ${%%gmtools.dist.name%%_HOME} ]]; then
  if [[ -f "${%%gmtools.dist.name%%_HOME}/conf/geomesa-env.sh" ]]; then
    GEOMESA_HOME_TOOLS_VERSION="`cat ${%%gmtools.dist.name%%_HOME}/conf/geomesa-env.sh | grep GEOMESA_TOOLS_VERSION | sed 's/GEOMESA_TOOLS_VERSION=//'`"
  else GEOMESA_HOME_TOOLS_VERSION=""; fi # legacy support
  if [[ $GEOMESA_TOOLS_VERSION != $GEOMESA_HOME_TOOLS_VERSION ]]; then
    echo "Warning: Using %%gmtools.dist.name%%_HOME = ${%%gmtools.dist.name%%_HOME} does not match Geomesa tools version"
  fi
fi
if [[ -n "$GEOMESA_CLASSPATH" ]]; then
  # Search the classpath for conflicting versions
  cpArr=(`echo $GEOMESA_CLASSPATH | sed 's/:/ /g'`)
  for i in "${cpArr[@]}"; do
    if [[ -d $i && -f "${i}/geomesa-env.sh" ]]; then # conf dir on cp
      GEOMESA_CLASSPATH_TOOLS_VERSION="`cat ${i}/geomesa-env.sh | grep GEOMESA_TOOLS_VERSION | sed 's/GEOMESA_TOOLS_VERSION=//'`"
    elif [[ -d $i && -d "${i}/conf/" ]]; then # home dir on cp
      if [[ -f "${i}/conf/geomesa-env.sh" ]]; then
        GEOMESA_CLASSPATH_TOOLS_VERSION="`cat ${i}/conf/geomesa-env.sh | grep GEOMESA_TOOLS_VERSION | sed 's/GEOMESA_TOOLS_VERSION=//'`"
      else GEOMESA_CLASSPATH_TOOLS_VERSION=""; fi # legacy support
    elif [[ "`basename $i`" == "geomesa-env.sh" && -f $i ]]; then # geomesa-env.sh on cp
      GEOMESA_CLASSPATH_TOOLS_VERSION="`cat ${i} | grep GEOMESA_TOOLS_VERSION | sed 's/GEOMESA_TOOLS_VERSION=//'`"
    fi
    if [[ $GEOMESA_TOOLS_VERSION != $GEOMESA_CLASSPATH_TOOLS_VERSION ]]; then
      echo "Warning: Resource ${i} found on GEOMESA_CLASSPATH does not match Geomesa tools version."
    fi
  done
fi

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
