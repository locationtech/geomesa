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
# Load config Env Variables
. "${0%/*}"/geomesa-env.sh

function setGeoHome() {
    SOURCE="${BASH_SOURCE[0]}"
    # resolve $SOURCE until the file is no longer a symlink
    while [[ -h "${SOURCE}" ]]; do
        bin="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"
        SOURCE="$(readlink "${SOURCE}")"
        # if $SOURCE was a relative symlink, we need to resolve it relative to the path where
        # the symlink file was located
        [[ "${SOURCE}" != /* ]] && SOURCE="${bin}/${SOURCE}"
    done
    bin="$( cd -P "$( dirname "${SOURCE}" )" && cd ../ && pwd )"
    export GEOMESA_HOME="$bin"
    export PATH=${GEOMESA_HOME}/bin:$PATH
    setGeoLog
    echo "Warning: GEOMESA_HOME is not set, using $GEOMESA_HOME" >> ${GEOMESA_LOG_DIR}/geomesa.err
}

function setGeoLog() {
    if [[ -z "${GEOMESA_LOG_DIR}" ]]; then
      export GEOMESA_LOG_DIR="${GEOMESA_HOME}/logs"
      export GEOMESA_OPTS="-Dgeomesa.log.dir=${GEOMESA_LOG_DIR} $GEOMESA_OPTS"
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
    echo >&2 "Using GEOMESA_HOME as set: $GEOMESA_HOME"
    read -p "Is this intentional? Y\n " -n 1 -r
    if [[  $REPLY =~ ^[Nn]$ ]]; then
        bin="$( cd -P "$( dirname "${SOURCE}" )" && cd ../ && pwd )"
        export GEOMESA_HOME="$bin"
        echo >&2 ""
        echo "Now set to ${GEOMESA_HOME}"
    fi

    if [[ -z "$GEOMESA_LIB" ]]; then
        GEOMESA_LIB=${GEOMESA_HOME}/lib
    else
        message="Warning: GEOMESA_LIB already set, probably by a prior configuration or the geomesa-env config."
        message="${message}\n Current value is ${GEOMESA_LIB}."
        echo >&2 ""
        echo -e >&2 "$message"
        echo >&2 ""
        read -p "Is this intentional? Y\n " -n 1 -r
        if [[  $REPLY =~ ^[Nn]$ ]]; then
            GEOMESA_LIB=${GEOMESA_HOME}/lib
            echo >&2 ""
            echo "Now set to ${GEOMESA_LIB}"
        fi
        echo >&2 ""
    fi

    echo >&2 ""
    echo "To persist the configuration please update your bashrc file to include: "
    echo "export GEOMESA_HOME="$GEOMESA_HOME""
    echo "export PATH=\${GEOMESA_HOME}/bin:\$PATH"
}

GEOMESA_OPTS="-Duser.timezone=UTC -DEPSG-HSQL.directory=/tmp/$(whoami)"
GEOMESA_OPTS="${GEOMESA_OPTS} -Djava.awt.headless=true"
GEOMESA_DEBUG_OPTS="-Xmx8192m -XX:MaxPermSize=512m -XX:-UseGCOverheadLimit -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=9898"
GEOMESA_CP=""

# Define GEOMESA_HOME and update the PATH if necessary.
if [[ -z "$GEOMESA_HOME" ]]; then
setGeoHome
else
    echo >&2 "Using GEOMESA_HOME = $GEOMESA_HOME"
    if [[ $1 = configure ]]; then
        read -p "Do you want to reset this? Y\n " -n 1 -r
        if [[  $REPLY =~ ^[Yy]$ ]]; then
            echo >&2 ""
            setGeoHome
        fi
        echo >&2 ""
    fi
fi

# GEOMESA paths, GEOMESA_LIB should live inside GEOMESA_HOME, but can be pointed elsewhere in geomesa-env
if [[ -z "$GEOMESA_LIB" ]]; then
    GEOMESA_LIB=${GEOMESA_HOME}/lib
elif [[ $1 = configure ]]; then
    message="Warning: GEOMESA_LIB was already set, probably by a prior configuration or the geomesa-env config."
    message="${message}\n The current value is ${GEOMESA_LIB}."
    echo >&2 ""
    echo -e >&2 "$message"
    echo >&2 ""
    read -p "Do you want to reset this to ${GEOMESA_HOME}/lib? Y\n " -n 1 -r
    if [[  $REPLY =~ ^[Yy]$ ]]; then
        GEOMESA_LIB=${GEOMESA_HOME}/lib
        echo >&2 ""
        echo "Now set to ${GEOMESA_LIB}"
    fi
    echo >&2 ""
fi

# Configure geomesa conf directory this can be set in geomesa-env
if [[ -z "$GEOMESA_CONF_DIR" ]]; then
    GEOMESA_CONF_DIR=${GEOMESA_HOME}/conf
elif [[ $1 = configure ]]; then
    message="Warning: GEOMESA_CONF_DIR was already set, probably by a prior configuration or the geomesa-env config."
    message="${message}\n The current value is ${GEOMESA_CONF_DIR}."
    echo >&2 ""
    echo -e >&2 "$message"
    echo >&2 ""
    read -p "Do you want to reset this to ${GEOMESA_HOME}/conf? Y\n " -n 1 -r
    if [[  $REPLY =~ ^[Yy]$ ]]; then
        GEOMESA_CONF_DIR=${GEOMESA_HOME}/conf
        echo >&2 ""
        echo "Now set to ${GEOMESA_CONF_DIR}"
    fi
    echo >&2 ""
fi

GEOMESA_OPTS="${GEOMESA_OPTS} -Dlog4j.configuration=file://${GEOMESA_CONF_DIR}/log4j.properties"

# Configure geomesa logging directory this can be set in geomesa-env
setGeoLog

# Configure Java Library Path this can be set in geomesa-env.
if [[ -n "$JAVA_LIBRARY_PATH" ]]; then
  GEOMESA_OPTS="${GEOMESA_OPTS} -Djava.library.path=${JAVA_LIBRARY_PATH}"
  export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$JAVA_LIBRARY_PATH"
fi

# Configure Java Options this can be set in geomesa-env.
if [[ -z "$JAVA_CUSTOM_OPTS" ]]; then
  export JAVA_CUSTOM_OPTS="${JAVA_OPTS}"
fi
