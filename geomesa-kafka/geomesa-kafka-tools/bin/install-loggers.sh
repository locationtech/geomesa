#!/usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

declare -a urls=(
  "https://search.maven.org/remotecontent?filepath=log4j/log4j/1.2.17/log4j-1.2.17.jar"
  "https://search.maven.org/remotecontent?filepath=org/slf4j/slf4j-log4j12/1.7.21/slf4j-log4j12-1.7.21.jar"
)

if [[ (-z "$1") ]]; then
  echo "Error: Provide one arg which is the target directory (e.g. /opt/jboss/standalone/deployments/geoserver.war/WEB-INF/lib)"
  exit
fi

install_dir=$1
NL=$'\n'
read -r -p "Install logging dependencies to '${install_dir}' (y/n)? " confirm
confirm=${confirm,,} #lowercasing
if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
  # get stuff
  for x in "${urls[@]}"; do
    fname=$(basename "$x");
    echo "fetching ${x}";
    wget -O "${1}/${fname}" "$x" || { rm -f "${1}/${fname}"; echo "Failed to download: ${x}"; \
      errorList="${errorList} ${x} ${NL}"; };
  done

  if [[ -n "${errorList}" ]]; then
    echo "Failed to download: ${NL} ${errorList}";
  fi
else
  echo "Installation cancelled"
fi
