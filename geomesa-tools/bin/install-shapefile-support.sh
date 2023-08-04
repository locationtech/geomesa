#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

jai_version="1.1.3"
jt_version="1.6.0"
imageio_version="1.1"
jline_version="2.12.1"

# configure HOME and CONF_DIR, then load geomesa-env.sh
export %%tools.dist.name%%_HOME="${%%tools.dist.name%%_HOME:-$(cd "$(dirname "$0")"/.. || exit; pwd)}"
export GEOMESA_CONF_DIR="${GEOMESA_CONF_DIR:-$%%tools.dist.name%%_HOME/conf}"

if [[ -f "${GEOMESA_CONF_DIR}/geomesa-env.sh" ]]; then
  . "${GEOMESA_CONF_DIR}/geomesa-env.sh"
else
  echo >&2 "ERROR: could not read '${GEOMESA_CONF_DIR}/geomesa-env.sh', aborting script"
  exit 1
fi

install_dir="${%%tools.dist.name%%_HOME}/lib"

if [[ -n "$1" && "$1" != "--no-prompt" ]]; then
  install_dir="$1"
  shift
fi

osgeo_url="https://repo.osgeo.org/repository/release/"
if [[ "$GEOMESA_MAVEN_URL" != "https://search.maven.org/remotecontent?filepath=" ]]; then
  osgeo_url="$GEOMESA_MAVEN_URL"
fi

declare -a urls=(
  "${GEOMESA_MAVEN_URL}org/jaitools/jt-utils/${jt_version}/jt-utils-${jt_version}.jar"
  "${osgeo_url}/javax/media/jai_codec/${jai_version}/jai_codec-${jai_version}.jar"
  "${osgeo_url}/javax/media/jai_core/${jai_version}/jai_core-${jai_version}.jar"
  "${osgeo_url}/javax/media/jai_imageio/${imageio_version}/jai_imageio-${imageio_version}.jar"
  "${GEOMESA_MAVEN_URL}jline/jline/${jline_version}/jline-${jline_version}.jar"
)

echo >&2 "Installing shapefile support to $install_dir"
echo >&2 "Note: Java Advanced Imaging (JAI) is LGPL licensed."
echo >&2 "Note: JLine is BSD licensed and free to use and distribute, however, the provenance of the code could not be established by the Eclipse Foundation."

confirm="yes"
if [[ "$1" != "--no-prompt" ]]; then
  read -r -p "Would you like to download them (y/n)? " confirm
  confirm=${confirm,,} # lower-casing
fi

if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
  download_urls "$install_dir" urls[@]
fi
