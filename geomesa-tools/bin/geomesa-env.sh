#!/usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# Allows specification of non-default resource locations so they don't need to be
# provided as parameters when using the GeoMesa tools. Uncomment the desired lines
# and replace the path with an appropriate loation.

# ==================================================================
# GeoMesa Environment Variables
# ==================================================================

# GeoMesa tools directory.
#
# export GEOMESA_HOME=/path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION

# GeoMesa lib directory.
# This resides inside GeoMesa tools by default but can be moved elsewhere. If this is
# not set but GEOMESA_HOME is, this will default to GEOMESA_HOME/lib.
#
# export GEOMESA_LIB=/path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION/lib

# GeoMesa conf directory.
# This resides inside GeoMesa tools by default but can be moved elsewhere. If this is
# not set but GEOMESA_HOME is, this will default to GEOMESA_HOME/conf.
#
# export GEOMESA_CONF_DIR=/path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION/conf

# GeoMesa logs directory.
# This resides inside GeoMesa tools by default but can be moved elsewhere. If this is
# not set but GEOMESA_HOME is, this will default to GEOMESA_HOME/logs.
#
# export GEOMESA_LOG_DIR=/path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION/logs

# ==================================================================
# Accumulo Environment Variables
# ==================================================================

# Accumulo directory.
#
# export ACCUMULO_HOME=/path/to/accumulo

# Accumulo lib directory.
# This resides inside Accumulo home by default. If this is not set but ACCUMULO_HOME is,
# this will default to ACCUMULO_HOME/lib.
#
# export ACCUMULO_LIB=/path/to/accumulo/lib

# Accumulo conf directory.
# This resides inside Accumulo home by default. If this is not set but ACCUMULO_HOME is,
# this will default to ACCUMULO_HOME/conf.
#
# export ACCUMULO_CONF_DIR=/path/to/accumulo/conf

# ==================================================================
# Hadoop Environment Variables
# ==================================================================

# Hadoop directory.
#
# export HADOOP_HOME=/path/to/hadoop

# Hadoop conf directory.
# This resides inside ${HADOOP_HOME}/etc/hadoop by default. If this is
# not set but HADOOP_HOME is, this will default to HADOOP_HOME/etc/hadoop.
#
# export HADOOP_CONF_DIR=/path/to/hadoop/etc/hadoop

# ==================================================================
# Zookeeper Environment Variables
# ==================================================================

# Zookeeper directory.
#
# export ZOOKEEPER_HOME=/path/to/zookeeper

# ==================================================================
# Java Environment Variables
# ==================================================================

# Add directory to classpath
# Add a colon separated list of directories to the GEOMESA_CP (class path variable).
# Note this will exclude any slf4j files.
#
# export GEOMESA_EXTRA_CLASSPATHS=/some/dir/:/another/dir/

# Java library path. Used to set GeoMesa Options.
#
# export JAVA_LIBRARY_PATH=/path/to/java/library

# Java command parameters.
# $JAVA_OPTS is included by default, editing this option will overwrite this default.
# Add additional options after {$JAVA_OPTS} to preserve current JAVA_OPTS.
# Replace {$JAVA_OPTS} to use different java options.
#
# export CUSTOM_JAVA_OPTS="{$JAVA_OPTS}"
