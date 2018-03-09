#!/usr/bin/env bash
#
# Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#
# Bootstrap Script to install a GeoMesa Jupyter notebook
#

# Load common functions and setup
if [[ -z "${%%gmtools.dist.name%%_HOME}" ]]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
fi

function log() {
  timeStamp=$(date +"%T")
  echo "${timeStamp}| ${@}" | tee -a /tmp/bootstrap.log
}

log "Zeppelin not impl"
