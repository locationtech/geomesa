#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for hbase
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

hbase_version="%%hbase.version%%"

# Resource download location
base_url="https://search.maven.org/remotecontent?filepath="
install_dir="$1"

# Command Line Help
NL=$'\n'
usage="usage: ./install-hbase.sh [target dir]"

if [[ (-z "${install_dir}") ]]; then
  echo "Error: Provide one arg which is the target directory (e.g. /opt/geoserver-2.9.1/webapps/geoserver/WEB-INF/lib)"
  echo "${usage}"
  exit
else
  read -r -p "Install hbase dependencies to ${install_dir}?${NL}Confirm? [Y/n]" confirm
  confirm=${confirm,,} #lowercasing
  if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
    # get stuff
    declare -a urls=(
      "${base_url}org/apache/hbase/hbase-client/${hbase_version}/hbase-client-${hbase_version}.jar"
      "${base_url}org/apache/hbase/hbase-common/${hbase_version}/hbase-common-${hbase_version}.jar"
    )

    for x in "${urls[@]}"; do
      fname=$(basename "$x");
      echo "fetching ${x}";
      wget -O "${install_dir}/${fname}" "$x" || { rm -f "${install_dir}/${fname}"; echo "Error downloading dependency: ${fname}"; \
        errorList="${errorList[@]} ${x} ${NL}"; };
    done
    if [[ -n "${errorList}" ]]; then
      echo "Failed to download: ${NL} ${errorList[@]}";
    fi
  else
    echo "Installation cancelled"
  fi
fi
