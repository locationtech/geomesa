#!/usr/bin/env bash
#
# Bootstrap a GeoMesa cluster node for Elastic Map Reduce (v4)
#    - Requires applications: Hadoop and ZooKeeper-Sandbox

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 
# Config Settings you might want to update

# Accumulo
USER=accumulo
# NOTE: This password, the Accumulo instance secret and the geoserver password are left at
# The default settings. The default EMR Security group setting only allows ssh/22 open to
# external access so access to internal consoles and web UIs has to be done over SSH.
# At some point in the future when this is revisited remember that nodes can be added to an
# EMR at any point after creation so the password set during the initial spin-up would have
# to be persisted somewhere and provided to the newly created nodes at some later date.
USERPW=secret # TODO: Can't change until trace.password in accumulo-site.xml is updated
ACCUMULO_VERSION=1.7.0
ACCUMULO_TSERVER_OPTS=3GB
INSTALL_DIR=/opt
ACCUMULO_DOWNLOAD_BASE_URL=https://archive.apache.org/dist/accumulo

# GeoMesa 1.2.x
GEOMESA_VERSION=${geomesa.release.version}
GEOMESA_TARBALL="geomesa-dist-"${GEOMESA_VERSION}"-bin.tar.gz"
# If this variable points to an s3 bucket/key, the tarball is grabbed from there (fast)
GEOMESA_DIST_S3= #"s3://my-bucket/bootstrap/"${GEOMESA_TARBALL}
#   Otherwise from the locationtech nexus (slow)
GEOMESA_DIST_LT="https://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-dist/"${GEOMESA_VERSION}"/"${GEOMESA_TARBALL}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Step #1: Source external library functions 
if [ ! -f /tmp/bootstrap-geomesa-lib.sh ]; then
	aws s3 cp s3://elasticmapreduce-geomesa/bootstrap-geomesa-lib.sh /tmp
fi
source /tmp/bootstrap-geomesa-lib.sh

# Step #2: The EMR customize hooks run _before_ everything else, so Hadoop is not yet ready
THIS_SCRIPT="$(realpath "${BASH_SOURCE[0]}")"
RUN_FLAG="${THIS_SCRIPT}.run"
# On first boot skip past this script to allow EMR to set up the environment. Set a callback
# which will poll for availability of HDFS and then install Accumulo and then GeoMesa
if [ ! -f "$RUN_FLAG" ]; then
	touch "$RUN_FLAG"
	TIMEOUT= is_master && TIMEOUT=3 || TIMEOUT=4
	echo "bash -x $(realpath "${BASH_SOURCE[0]}") > /tmp/bootstrap-geomesa.log" | at now + $TIMEOUT min
	exit 0 # Bail and let EMR finish initializing
fi

# Step #3: Get Accumulo running
os_tweaks  
create_accumulo_user && install_accumulo && configure_accumulo

# Step #4: Install GeoMesa on master
if is_master ; then
	install_geomesa	
fi
