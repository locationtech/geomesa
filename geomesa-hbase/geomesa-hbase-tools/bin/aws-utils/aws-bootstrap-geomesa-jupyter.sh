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

projectVersion="%%project.version%%"
scalaBinVersion="%%scala.binary.version%%"

if [[ -z "${%%gmtools.dist.name%%_HOME}" ]]; then
  %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
fi
GM_TOOLS_HOME="${%%gmtools.dist.name%%_HOME}"

function log() {
  timeStamp=$(date +"%T")
  echo "${timeStamp}| ${@}" | tee -a /tmp/bootstrap.log
}

if [[ "${1}" == "-h" || "${2}" == "--help" ]]; then
  echo "Usage: ./aws-bootstrap-geomesa-jupyter.sh <jupyter_password>"
  exit 0
fi

# Verify that we are running in sudo mode
if [[ "$EUID" -ne 0 ]]; then
  log "ERROR: Please run in sudo mode"
  exit 1
fi

user="jupyter"
sudo useradd $user
sudo -u hadoop hdfs dfs -mkdir /user/$user
sudo -u hadoop hdfs dfs -chown $user /user/$user

JUPYTER_PASSWORD=$1
if [[ -z "${JUPYTER_PASSWORD}" ]]; then
  JUPYTER_PASSWORD="geomesa"
  log "Using default password: geomesa"
fi

sudo ${GM_TOOLS_HOME}/bin/aws-utils/aws-bootstrap-geomesa-python.sh

log "Installing Jupyter"
sudo python36 -m pip install jupyter pandas folium matplotlib geopandas

# Prepare runtime (gm-hbase bootstrap may not have run)
runtimeJar="geomesa-hbase-spark-runtime_${scalaBinVersion}-${projectVersion}.jar"
linkFile="/opt/geomesa/dist/spark/geomesa-hbase-spark-runtime.jar"
[[ ! -h $linkFile ]] && sudo ln -s $runtimeJar $linkFile

log "Generating Jupyter Notebook Config"
notebookDir="${GM_TOOLS_HOME}/examples/jupyter"
sudo chown -R $user $notebookDir
# This IP is the EC2 instance metadata service and is the recommended way to retrieve this information
publicDNS=$(curl http://169.254.169.254/latest/meta-data/public-hostname)
password=$((python36 -c "from notebook.auth import passwd; exit(passwd(\"${JUPYTER_PASSWORD}\"))") 2>&1)
notebookRes=($(sudo -H -u ${user} /usr/bin/python36 /usr/local/bin/jupyter-notebook --generate-config -y))
notebookConf="${notebookRes[-1]}"
rm -f ${notebookConf}
cat > ${notebookConf} <<EOF
c.NotebookApp.ip = '${publicDNS}'
c.NotebookApp.notebook_dir = u'$notebookDir'
c.NotebookApp.open_browser = False
c.NotebookApp.password = u'${password}'
c.NotebookApp.port = 8888
EOF

sudo -H -u ${user} nohup /usr/bin/python36 /usr/local/bin/jupyter-notebook &>/tmp/jupyter.log &
log "Jupyter ready"
