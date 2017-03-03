#!/bin/bash

if [[ "$#" -ne 1 ]]; then
    echo "Error: Invalid input parameters"
    echo "Usage: bootstrap-geomesa-hbase.sh <geomesa-version>"
    exit 1
fi

VERSION=$1

echo "Bootstrapping GeoMesa HBase with version ${VERSION}"

pip install --upgrade awscli

test -d /opt || mkdir -p /opt
chmod a+rwx /opt

cd /opt
ln -s geomesa-hbase_2.11-${VERSION} geomesa

cat <<EOF > /etc/profile.d/geomesa.sh
export GEOMESA_HOME=/opt/geomesa
export HBASE_HOME=/usr/lib/hbase
export HADOOP_HOME=/usr/lib/hadoop
export PATH=\$PATH:\$GEOMESA_HOME/bin
EOF

# Copy AWS dependencies to geomesa lib dir
cp /usr/share/aws/emr/emrfs/lib/* /opt/geomesa/lib
cp /usr/lib/hbase/conf/hbase-site.xml /opt/geomesa/conf/

chown -R ec2-user:ec2-user /opt/geomesa-hbase_2.11-${VERSION}

# Create an HDFS directory for Spark jobs
sudo -u hdfs hadoop fs -mkdir /user/ec2-user
sudo -u hdfs hadoop fs -chown ec2-user:ec2-user /user/ec2-user

echo "Bootstrap complete...log out and relogin to complete process"

