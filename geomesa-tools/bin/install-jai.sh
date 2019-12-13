#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

jai_version="1.1.3"
jt_version="1.3.1"
imageio_version="1.1"

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

osgeo_url="${GEOMESA_MAVEN_URL:-http://download.osgeo.org/webdav/}"
mvn_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${mvn_url}org/jaitools/jt-utils/${jt_version}/jt-utils-${jt_version}.jar"
  "${osgeo_url}geotools/javax/media/jai_codec/${jai_version}/jai_codec-${jai_version}.jar"
  "${osgeo_url}geotools/javax/media/jai_core/${jai_version}/jai_core-${jai_version}.jar"
  "${osgeo_url}geotools/javax/media/jai_imageio/${imageio_version}/jai_imageio-${imageio_version}.jar"
)

echo "Warning: Java Advanced Imaging (JAI) is LGPL licensed, and thus not distributed with GeoMesa. However, you may download it yourself."

downloadUrls "$install_dir" urls[@]
