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
  %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
fi
GM_TOOLS_HOME="${%%gmtools.dist.name%%_HOME}"

function log() {
  timeStamp=$(date +"%T")
  echo "${timeStamp}| ${@}" | tee -a /tmp/bootstrap.log
}

# Verify that we are running in sudo mode
if [[ "$EUID" -ne 0 ]]; then
  log "ERROR: Please run in sudo mode"
  exit
fi

log "Installing Python 3.6"
sudo yum install -q -y python36 python36-devel
sudo python36 -m pip install --upgrade pip

# Check if geomesa_pyspark is available and should be installed
gm_pyspark=${GM_TOOLS_HOME}/dist/spark/geomesa_pyspark-*
if ls $gm_pyspark 1> /dev/null 2>&1; then
  log "Installing Geomesa Pyspark"
  sudo python36 -m pip install $gm_pyspark
else
  log "[Warning] geomesa_pyspark is not available for install. Geomesa python interop will not be available. Rebuild the tools distribution with the 'python' profile to enable this functionality."
fi

log "Python ready"
