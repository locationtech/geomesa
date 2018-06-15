#!/bin/bash
#
# Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

projectVersion="%%project.version%%"
scalaBinVersion="%%scala.binary.version%%"

logFile=/tmp/bootstrap.log
function log() {
  timeStamp=$(date +"%T")
  echo "${timeStamp}| ${@}" | tee -a $logFile
}

# Verify that we are running in sudo mode
if [[ "$EUID" -ne 0 ]]; then
  log "ERROR: Please run in sudo mode"
  exit
fi

GMUSER=hadoop

# Get tools home location and bootstrap from there installing to /opt/geomesa
if [[ -z "${%%gmtools.dist.name%%_HOME}" ]]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
fi

if [[ ! -d "${%%gmtools.dist.name%%_HOME}" ]]; then
  log "Unable to find geomesa directory at ${%%gmtools.dist.name%%_HOME}"
  log "Set environment variable 'GEOMESA_HBASE_HOME' to fix this."
  exit 1
fi

log "Bootstrapping GeoMesa-FSDS with version ${projectVersion} installed at ${%%gmtools.dist.name%%_HOME}"

/usr/local/bin/pip install --upgrade awscli

if [[ ! -d "/opt" ]]; then
  log "Unable to find /opt"
  exit 1
fi
chmod a+rwx /opt

ln -s ${%%gmtools.dist.name%%_HOME} /opt/geomesa

cat <<EOF > /etc/profile.d/geomesa.sh
export %%gmtools.dist.name%%_HOME=/opt/geomesa
export PATH=\$PATH:\$%%gmtools.dist.name%%_HOME/bin

EOF

chown -R $GMUSER:$GMUSER ${%%gmtools.dist.name%%_HOME}

# Prepare runtime
runtimeJar="geomesa-fs-spark-runtime_${scalaBinVersion}-${projectVersion}.jar"
linkFile="/opt/geomesa/dist/spark/geomesa-fs-spark-runtime.jar"
[[ ! -h $linkFile ]] && sudo ln -s $runtimeJar $linkFile

# Create an HDFS directory for Spark jobs
sudo -u $GMUSER hadoop fs -test -d /user/$GMUSER || sudo -u $GMUSER hadoop fs -mkdir /user/$GMUSER
sudo -u $GMUSER hadoop fs -chown $GMUSER:$GMUSER /user/$GMUSER

# Set up the classpath for Hadoop and HBase
cat <<EOF >> ${%%gmtools.dist.name%%_HOME}/conf/geomesa-env.sh

# Set the Hadoop Classpath
export GEOMESA_HADOOP_CLASSPATH=$(hadoop classpath)

EOF

# Make sure everyone can write to the log
sudo chmod 777 ${%%gmtools.dist.name%%_HOME}/logs/geomesa.log

log "GeoMesa-HBase Bootstrap complete...log out and re-login to complete process"
