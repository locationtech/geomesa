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

if [[ "${1}" == "-h" || "${2}" == "--help" ]]; then
  echo "Usage: ./aws-bootstrap-geomesa-jupyter.sh <jupyter_password>"
fi

# Verify that we are running in sudo mode
if [[ "$EUID" -ne 0 ]]; then
  log "ERROR: Please run in sudo mode"
  exit
fi

user="hadoop"

JUPYTER_PASSWORD=$1
if [[ -z "${JUPYTER_PASSWORD}" ]]; then
  JUPYTER_PASSWORD="geomesa"
  log "Using default password: geomesa"
fi

log "Installing Python 3.6"
sudo yum install -q -y python36 gcc python-devel

log "Installing Jupyter"
sudo python36 -m pip install --upgrade pip
sudo python36 -m pip install jupyter pandas

#log "Installing s3fs-fuse"
#export PKG_CONFIG_PATH=/usr/lib/pkgconfig
#sudo yum install -y automake fuse fuse-devel gcc-c++ git libcurl-devel libxml2-devel make openssl-devel
#
#pushd /mnt
#s3fsVersion=1.83
#wget https://github.com/s3fs-fuse/s3fs-fuse/archive/v${s3fsVersion}.zip
#unzip v${s3fsVersion}.zip
#cd s3fs-fuse-${s3fsVersion}
#./autogen.sh
#./configure
#make
#sudo make install
#mkdir -p /mnt/s3fs-cache
#mkdir -p /mnt/$BUCKET
#popd

log "Generating Jupyter Notebook Config"
# This IP is the EC2 instance metadata service and is the recommended way to retrieve this information
publicDNS=$(curl http://169.254.169.254/latest/meta-data/public-hostname)
password=$((python36 -c "from notebook.auth import passwd; exit(passwd(\"${JUPYTER_PASSWORD}\"))") 2>&1)
notebookRes=($(sudo -H -u ${user} /usr/bin/python36 /usr/local/bin/jupyter-notebook --generate-config -y))
notebookConf="${notebookRes[-1]}"
rm -f ${notebookConf}
cat > ${notebookConf} <<EOF
c.NotebookApp.ip = '${publicDNS}'
c.NotebookApp.notebook_dir = u'${%%gmtools.dist.name%%_HOME}/examples/'
c.NotebookApp.open_browser = False
c.NotebookApp.password = u'${password}'
c.NotebookApp.port = 8888
EOF

sudo -H -u ${user} nohup /usr/bin/python36 /usr/local/bin/jupyter-notebook &>/tmp/jupyter.log &
log "Jupyter ready"
