#!/usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

imageio_version='1.1.25'
imageio_gdal_bindings_version='1.9.2'

# Load common functions and setup
if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

base_url="${GEOMESA_MAVEN_URL:-https://repo.boundlessgeo.com/main/}"

partial_url='geotools/it/geosolutions/imageio-ext'

declare -a urls=(
  "${base_url}${partial_url}/imageio-ext-gdalarcbinarygrid/${imageio_version}/imageio-ext-gdalarcbinarygrid-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalframework/${imageio_version}/imageio-ext-gdalframework-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdaldted/${imageio_version}/imageio-ext-gdaldted-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalecw/${imageio_version}/imageio-ext-gdalecw-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalecwjp2/${imageio_version}/imageio-ext-gdalecwjp2-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalehdr/${imageio_version}/imageio-ext-gdalehdr-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalenvihdr/${imageio_version}/imageio-ext-gdalenvihdr-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalerdasimg/${imageio_version}/imageio-ext-gdalerdasimg-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalframework/${imageio_version}/imageio-ext-gdalframework-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalidrisi/${imageio_version}/imageio-ext-gdalidrisi-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalkakadujp2/${imageio_version}/imageio-ext-gdalkakadujp2-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalmrsid/${imageio_version}/imageio-ext-gdalmrsid-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalmrsidjp2/${imageio_version}/imageio-ext-gdalmrsidjp2-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalnitf/${imageio_version}/imageio-ext-gdalnitf-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdalrpftoc/${imageio_version}/imageio-ext-gdalrpftoc-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-geocore/${imageio_version}/imageio-ext-geocore-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-imagereadmt/${imageio_version}/imageio-ext-imagereadmt-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-streams/${imageio_version}/imageio-ext-streams-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-tiff/${imageio_version}/imageio-ext-tiff-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-utilities/${imageio_version}/imageio-ext-utilities-${imageio_version}.jar"
  "${base_url}${partial_url}/imageio-ext-gdal-bindings/${imageio_gdal_bindings_version}/imageio-ext-gdal-bindings-${imageio_gdal_bindings_version}.jar"
)

echo "Warning: Imageio-ext is LGPL licensed, and thus not distributed with GeoMesa. However, you may download it yourself."

downloadUrls "$install_dir" urls[@]
