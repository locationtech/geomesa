#!/bin/bash

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

cat <<EOF > /etc/profile.d/geomesa.sh
export GEOMESA_HOME=/opt/geomesa
export HBASE_HOME=/usr/lib/hbase
export HADOOP_HOME=/usr/lib/hadoop
export PATH=\$PATH:\$GEOMESA_HOME/bin

EOF

## Make sure 'hbase' is up first!

ROOTDIR=`cat /usr/lib/hbase/conf/hbase-site.xml 2> /dev/null | tr '\n' ' ' | sed 's/ //g' | grep -o -P "<name>hbase.rootdir</name><value>.+?</value>" | sed 's/<name>hbase.rootdir<\/name><value>//' | sed 's/<\/value>//'`
while [[ -z "$ROOTDIR" ]]
do
      sleep 2
      echo Waiting for HBase to be configured.
      ROOTDIR=`cat /usr/lib/hbase/conf/hbase-site.xml 2> /dev/null | tr '\n' ' ' | sed 's/ //g' | grep -o -P "<name>hbase.rootdir</name><value>.+?</value>" | sed 's/<name>hbase.rootdir<\/name><value>//' | sed 's/<\/value>//'`
done

# Copy AWS dependencies to geomesa lib dir
# Make sure HBase is properly initialized before running this
# script or else the configuration will not be set up
# properly
cp /usr/share/aws/emr/emrfs/lib/* /opt/geomesa/lib
cp /usr/lib/hbase/conf/hbase-site.xml /opt/geomesa/conf/

chown -R ec2-user:ec2-user ${GMDIR}

# Configure coprocessor auto-registration
DISTRIBUTED_JAR_NAME=geomesa-hbase-distributed-runtime_2.11-%%project.version%%.jar

NL=$'\n'
echo The HBase Root dir is ${ROOTDIR}.
echo "# Auto-registration for geomesa coprocessors ${NL}export CUSTOM_JAVA_OPTS=\"${JAVA_OPTS} ${CUSTOM_JAVA_OPTS} -Dgeomesa.hbase.coprocessor.path=${ROOTDIR}/lib/${DISTRIBUTED_JAR_NAME}\" ${NL}" >> /opt/geomesa/conf/geomesa-env.sh

# Deploy the GeoMesa HBase distributed runtime to the HBase root directory
aws s3 cp /opt/geomesa/dist/hbase/$DISTRIBUTED_JAR_NAME $ROOTDIR/lib/

# Create an HDFS directory for Spark jobs
sudo -u hdfs hadoop fs -mkdir /user/ec2-user
sudo -u hdfs hadoop fs -chown ec2-user:ec2-user /user/ec2-user

echo "Bootstrap complete...log out and relogin to complete process"
