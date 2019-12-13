#!/bin/bash

#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# Verify that we are running in sudo mode
if [[ "$EUID" -ne 0 ]]; then
  echo "ERROR: Please run in sudo mode"
  exit
fi

GMUSER=hadoop

# todo bootstrap from the current running location and ask the user if they want to 
# install to /opt/geomesa ? Maybe propmpt with a default of /opt/geomesa similar to
# how the maven release plugin works?
GMDIR="/opt/geomesa-hbase_2.11-%%project.version%%"

if [[ ! -d "${GMDIR}" ]]; then
  echo "Unable to find geomesa directory at ${GMDIR}"
  exit 1
fi

echo "Bootstrapping GeoMesa HBase with version %%project.version%% installed at ${GMDIR}"

pip install --upgrade awscli

if [[ ! -d "/opt" ]]; then
  echo "Unable to find /opt"
  exit 1
fi
chmod a+rwx /opt

ln -s ${GMDIR} /opt/geomesa
export GEOMESA_HBASE_HOME=/opt/geomesa

cat <<EOF > /etc/profile.d/geomesa.sh
export GEOMESA_HBASE_HOME=/opt/geomesa
export PATH=\$PATH:\$GEOMESA_HBASE_HOME/bin

EOF

## Make sure 'hbase' is up first!

ROOTDIR=`cat /usr/lib/hbase/conf/hbase-site.xml 2> /dev/null | tr '\n' ' ' | sed 's/ //g' | grep -o -P "<name>hbase.rootdir</name><value>.+?</value>" | sed 's/<name>hbase.rootdir<\/name><value>//' | sed 's/<\/value>//'`
while [[ -z "$ROOTDIR" ]]
do
      sleep 2
      echo Waiting for HBase to be configured.
      ROOTDIR=`cat /usr/lib/hbase/conf/hbase-site.xml 2> /dev/null | tr '\n' ' ' | sed 's/ //g' | grep -o -P "<name>hbase.rootdir</name><value>.+?</value>" | sed 's/<name>hbase.rootdir<\/name><value>//' | sed 's/<\/value>//'`
done
ROOTDIR="${ROOTDIR%/}" # standardize to remove trailing slash

chown -R $GMUSER:$GMUSER ${GMDIR}

# Configure coprocessor auto-registration
DISTRIBUTED_JAR_NAME=geomesa-hbase-distributed-runtime_2.11-%%project.version%%.jar

NL=$'\n'
echo The HBase Root dir is ${ROOTDIR}.
echo "# Auto-registration for geomesa coprocessors ${NL}export CUSTOM_JAVA_OPTS=\"${JAVA_OPTS} ${CUSTOM_JAVA_OPTS} -Dgeomesa.hbase.coprocessor.path=${ROOTDIR}/lib/${DISTRIBUTED_JAR_NAME}\" ${NL}" >> /opt/geomesa/conf/geomesa-env.sh

# Deploy the GeoMesa HBase distributed runtime to the HBase root directory
if [[ "$ROOTDIR" = s3* ]]; then
  aws s3 cp /opt/geomesa/dist/hbase/$DISTRIBUTED_JAR_NAME ${ROOTDIR}/lib/ && \
  echo "Installed GeoMesa distributed runtime to ${ROOTDIR}/lib/"
elif [[ "$ROOTDIR" = hdfs* ]]; then
  local libdir="${ROOTDIR}/lib"
  (sudo -u $GMUSER hadoop fs -test -d $libdir || sudo -u $GMUSER hadoop fs -mkdir $libdir) && \
  sudo -u $GMUSER hadoop fs -put -f ${GEOMESA_HBASE_HOME}/dist/hbase/$DISTRIBUTED_JAR_NAME $libdir/$DISTRIBUTED_JAR_NAME && \
  sudo -u $GMUSER hadoop fs -chown -R hbase:hbase $ROOTDIR/lib && \
  echo "Installed GeoMesa distributed runtime to $ROOTDIR/lib/"
fi

# Create an HDFS directory for Spark jobs
sudo -u $GMUSER hadoop fs -test -d /user/$GMUSER || sudo -u $GMUSER hadoop fs -mkdir /user/$GMUSER
sudo -u $GMUSER hadoop fs -chown $GMUSER:$GMUSER /user/$GMUSER

# Set up the classpath for Hadoop and HBase
cat <<EOF >> ${GEOMESA_HBASE_HOME}/conf/geomesa-env.sh

# Set the Hadoop Classpath
export GEOMESA_HADOOP_CLASSPATH=$(hadoop classpath)

# Set the HBase Classpath
export GEOMESA_HBASE_CLASSPATH=$(hbase classpath)

EOF

echo "Bootstrap complete...log out and relogin to complete process"
