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

# ==================================================================
# General Environment Variables
# ==================================================================

# Maven repository
# Used for downloading dependencies in the various 'install-xxx.sh' scripts
#
# export GEOMESA_MAVEN_URL="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

# ==================================================================
# GeoMesa Environment Variables
# ==================================================================

# To change %%gmtools.dist.name%%_HOME and/or GEOMESA_CONF_DIR, export the desired path to the respective variable
# before running any scripts. These existing environmental variables will not be overwritten.

# GeoMesa lib directory.
# This resides inside GeoMesa tools by default but can be moved elsewhere. If this is
# not set but %%gmtools.dist.name%%_HOME is, this will default to %%gmtools.dist.name%%_HOME/lib.
#
# export GEOMESA_LIB="{GEOMESA_LIB:-$%%gmtools.dist.name%%_HOME/lib}"

# GeoMesa logs directory.
# This resides inside GeoMesa tools by default but can be moved elsewhere. If this is
# not set but %%gmtools.dist.name%%_HOME is, this will default to %%gmtools.dist.name%%_HOME/logs.
#
# export GEOMESA_LOG_DIR="{GEOMESA_LOG_DIR:-$%%gmtools.dist.name%%_HOME/logs}"

# ==================================================================
# Accumulo Environment Variables
# ==================================================================

# Accumulo directory.
#
# export ACCUMULO_HOME="{ACCUMULO_HOME:-/path/to/accumulo}"

# Accumulo lib directory.
# This resides inside Accumulo home by default. If this is not set but ACCUMULO_HOME is,
# this will default to ACCUMULO_HOME/lib.
#
# export ACCUMULO_LIB="{ACCUMULO_LIB:-$ACCUMULO_HOME/lib}"

# Accumulo conf directory.
# This resides inside Accumulo home by default. If this is not set but ACCUMULO_HOME is,
# this will default to ACCUMULO_HOME/conf.
#
# export ACCUMULO_CONF_DIR="${ACCUMULO_CONF_DIR:-$ACCUMULO_HOME/conf}"

# ==================================================================
# Hadoop Environment Variables
# ==================================================================

# Set this variable to provide GeoMesa with Hadoop jars. A good starting point is to run
# the "hadoop classpath" command and set the value to its output
# export GEOMESA_HADOOP_CLASSPATH=${GEOMESA_HADOOP_CLASSPATH:-$(hadoop classpath)}"

# Hadoop directory.
#
# export HADOOP_HOME="${HADOOP_HOME:-/path/to/hadoop}"

# Hadoop conf directory.
# This resides inside ${HADOOP_HOME}/etc/hadoop by default. If this is
# not set but HADOOP_HOME is, this will default to HADOOP_HOME/etc/hadoop.
#
# export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/path/to/hadoop/etc/hadoop}"

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
# export HADOOP_COMMON_HOME="${HADOOP_COMMON_HOME:-/path/to/hadoop/common/home}"
# export HADOOP_HDFS_HOME="${HADOOP_HDFS_HOME:-/path/to/hadoop/hdfs/home}"
# export YARN_HOME="${YARN_HOME:-/path/to/yarn/home}"
# export HADOOP_MAPRED_HOME="${HADOOP_MAPRED_HOME:-/path/to/map/reduce/home}"
# export HADOOP_CUSTOM_CP="${HADOOP_CUSTOM_CP:-/path/to/jars:/path/to/jars}"

# ==================================================================
# Zookeeper Environment Variables
# ==================================================================

# Zookeeper directory.
#
# export ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-/path/to/zookeeper}"

# ==================================================================
# Java Environment Variables
# ==================================================================

# Prepend user defined classpaths to the GEOMESA_CP (class path variable)
# Follows the standard Java classpaths syntax
#
# export GEOMESA_EXTRA_CLASSPATHS="${GEOMESA_EXTRA_CLASSPATHS:-/some/dir/:/another/dir/}"

# Java library path. Used to set GeoMesa Options.
#
# export JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH:-/path/to/java/library}"

# Java command parameters.
# $JAVA_OPTS is included by default, editing this option will overwrite this default.
# Add additional options after {$JAVA_OPTS} to preserve current JAVA_OPTS.
# Replace {$JAVA_OPTS} to use different java options.
#
# export CUSTOM_JAVA_OPTS="${CUSTOM_JAVA_OPTS:-$JAVA_OPTS}"
