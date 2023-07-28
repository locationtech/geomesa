#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for running the geomesa command-line tools.
# Usually these dependencies will be provided by the environment (e.g. HADOOP_HOME).
# Update the versions as required to match the target environment.

# gets the dependencies for this module
# args:
#   $1 - current classpath
function dependencies() {
  echo ""
}

# gets any dependencies that should be removed from the classpath for this module
# args:
#   $1 - current classpath
function exclude_dependencies() {
  echo ""
}
