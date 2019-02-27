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
#configPriority="0"

# ------------------- Do not alter this section --------------------
#existingEnvVars=()
#function setvar() {
#  if [[ "$configPriority" == "0" ]]; then
#    test -z "$(eval "echo \$$1")"  && export $1=$2 || existingEnvVars=("${existingEnvVars[@]}" $1)
#  else
#    export $1=$2
#  fi
#}
# ------------------------------------------------------------------

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
# setvar CUSTOM_JAVA_OPTS "{$JAVA_OPTS}"
