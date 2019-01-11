#!/usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# Allows specification of non-default resource locations so they don't need to be
# provided as parameters when using the GeoMesa tools. Uncomment the desired lines
# and replace the path with an appropriate location.
#
# You can use alternate configuration files by setting $GEOMESA_CONF_DIR to another
# location before running commands.
#
# By default existing environment variables take precedent. If you would prefer the variables
# set in this config to take precedent set the following variable to "1". This can be helpful if you
# frequently use multiple configuration files/folders.
configPriority="0"

# ------------------- Do not alter this section --------------------
existingEnvVars=()
function setvar() {
  if [[ "$configPriority" == "0" ]]; then
    test -z "$(eval "echo \$$1")" && export $1=$2 || existingEnvVars=("${existingEnvVars[@]}" $1)
  else
    export $1=$2
  fi
}
# ------------------------------------------------------------------

# ==================================================================
# General Environment Variables
# ==================================================================

# Maven repository
# Used for downloading dependencies in the various 'install-xxx.sh' scripts
#
# setvar GEOMESA_MAVEN_URL "https://search.maven.org/remotecontent?filepath="

# ==================================================================
# GeoMesa Environment Variables
# ==================================================================

# To change %%gmtools.dist.name%%_HOME and/or GEOMESA_CONF_DIR, export the desired path to the respective variable
# before running any scripts. These existing environmental variables will not be overwritten.

# GeoMesa lib directory.
# This resides inside GeoMesa tools by default but can be moved elsewhere. If this is
# not set but %%gmtools.dist.name%%_HOME is, this will default to %%gmtools.dist.name%%_HOME/lib.
#
# setvar GEOMESA_LIB /path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION/lib

# GeoMesa logs directory.
# This resides inside GeoMesa tools by default but can be moved elsewhere. If this is
# not set but %%gmtools.dist.name%%_HOME is, this will default to %%gmtools.dist.name%%_HOME/logs.
#
# setvar GEOMESA_LOG_DIR /path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION/logs

# ==================================================================
# Accumulo Environment Variables
# ==================================================================

# Accumulo directory.
#
# setvar ACCUMULO_HOME /path/to/accumulo

# Accumulo lib directory.
# This resides inside Accumulo home by default. If this is not set but ACCUMULO_HOME is,
# this will default to ACCUMULO_HOME/lib.
#
# setvar ACCUMULO_LIB /path/to/accumulo/lib

# Accumulo conf directory.
# This resides inside Accumulo home by default. If this is not set but ACCUMULO_HOME is,
# this will default to ACCUMULO_HOME/conf.
#
# setvar ACCUMULO_CONF_DIR /path/to/accumulo/conf

# ==================================================================
# Hadoop Environment Variables
# ==================================================================

# Set this variable to provide GeoMesa with Hadoop jars. A good starting point is to run
# the "hadoop classpath" command and set the value to its output
# export GEOMESA_HADOOP_CLASSPATH=

# Hadoop directory.
#
# setvar HADOOP_HOME /path/to/hadoop

# Hadoop conf directory.
# This resides inside ${HADOOP_HOME}/etc/hadoop by default. If this is
# not set but HADOOP_HOME is, this will default to HADOOP_HOME/etc/hadoop.
#
# setvar HADOOP_CONF_DIR /path/to/hadoop/etc/hadoop

# Hadoop CDH configuration
# Setting this variable to "1" will configure classpath settings for Hadoop
# CDH. HADOOP_HOME and HADOOP_CONF_DIR are still used.
#
hadoopCDH="0"
#
# Hadoop CDH classpath variables
# Depending on your installation configuration these may not be needed.
# These are all loaded into the classpath. slf4j jars will be excluded.
#
# setvar HADOOP_COMMON_HOME /path/to/hadoop/common/home
# setvar HADOOP_HDFS_HOME /path/to/hadoop/hdfs/home
# setvar YARN_HOME /path/to/yarn/home
# setvar HADOOP_MAPRED_HOME /path/to/map/reduce/home
# setvar HADOOP_CUSTOM_CP /path/to/jars:/path/to/jars

# ==================================================================
# Zookeeper Environment Variables
# ==================================================================

# Zookeeper directory.
#
# setvar ZOOKEEPER_HOME /path/to/zookeeper

# ==================================================================
# Java Environment Variables
# ==================================================================

# Prepend user defined classpaths to the GEOMESA_CP (class path variable)
# Follows the standard Java classpaths syntax
#
# setvar GEOMESA_EXTRA_CLASSPATHS /some/dir/:/another/dir/

# Java library path. Used to set GeoMesa Options.
#
# setvar JAVA_LIBRARY_PATH /path/to/java/library

# Java command parameters.
# $JAVA_OPTS is included by default, editing this option will overwrite this default.
# Add additional options after {$JAVA_OPTS} to preserve current JAVA_OPTS.
# Replace {$JAVA_OPTS} to use different java options.
#
# setvar CUSTOM_JAVA_OPTS "${JAVA_OPTS}"
