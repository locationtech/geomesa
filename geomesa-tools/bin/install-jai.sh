#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

if [ -z "${%%gmtools.dist.name%%_HOME}" ]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
lib_dir="${%%gmtools.dist.name%%_HOME}/lib"

osgeo_url='http://download.osgeo.org'
mvn_url='http://central.maven.org'

url_codec="${osgeo_url}/webdav/geotools/javax/media/jai_codec/1.1.3/jai_codec-1.1.3.jar"
url_core="${osgeo_url}/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
url_jttools="${mvn_url}/maven2/org/jaitools/jt-utils/1.3.1/jt-utils-1.3.1.jar"
url_imageio="${osgeo_url}/webdav/geotools/javax/media/jai_imageio/1.1/jai_imageio-1.1.jar"

NL=$'\n'
read -r -p "Java Advanced Imaging (jai) is LGPL licensed and is not distributed with GeoMesa...are you sure you want to install the following files:${NL}${url_codec}${NL}${url_core}${NL}${url_jttools}${NL}${url_imageio}${NL}Confirm? [Y/n]" confirm
confirm=${confirm,,} #lowercasing
if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
  echo "Trying to install JAI tools from $url_jttools to ${lib_dir}"
  wget -O "${lib_dir}/jt-utils-1.3.1.jar" $url_jttools \
    && chmod 0755 "${lib_dir}/jt-utils-1.3.1.jar" \
    && echo "Successfully installed JAI tools to ${lib_dir}" \
    || { rm -f "${lib_dir}/jt-utils-1.3.1.jar"; echo "Failed to download: ${url_jttools}"; \
    errorList="${errorList[@]} ${url_jttools} ${NL}"; };

  echo "Trying to install JAI ImageIO from $url_imageio to ${lib_dir}"
  wget -O "${lib_dir}/jai_imageio-1.1.jar" $url_imageio \
    && chmod 0755 "${lib_dir}/jai_imageio-1.1.jar" \
    && echo "Successfully installed JAI imageio"\
    || { rm -f "${lib_dir}/jai_imageio-1.1.jar"; echo "Failed to download: ${url_imageio}"; \
    errorList="${errorList[@]} ${url_imageio} ${NL}"; };

  echo "Trying to install JAI Codec from $url_codec to ${lib_dir}"
  wget -O "${lib_dir}/jai_codec-1.1.3.jar" $url_codec \
    && chmod 0755 "${lib_dir}/jai_codec-1.1.3.jar" \
    && echo "Successfully installed JAI codec to ${lib_dir}"\
    || { rm -f "${lib_dir}/jai_codec-1.1.3.jar"; echo "Failed to download: ${url_codec}"; \
    errorList="${errorList[@]} ${url_codec} ${NL}"; };

  echo "Trying to install JAI Core from $url_core to ${lib_dir}"
  wget -O "${lib_dir}/jai_core-1.1.3.jar" $url_core \
    && chmod 0755 "${lib_dir}/jai_core-1.1.3.jar" \
    && echo "Successfully installed JAI core to ${lib_dir}"\
    || { rm -f "${lib_dir}/jai_core-1.1.3.jar"; echo "Failed to download: ${url_core}"; \
    errorList="${errorList[@]} ${url_core} ${NL}"; };

  if [[ -n "${errorList}" ]]; then
    echo "Failed to download: ${NL} ${errorList[@]}";
  fi
else
  echo "Cancelled installation of Java Advanced Imaging (jai)"
fi
