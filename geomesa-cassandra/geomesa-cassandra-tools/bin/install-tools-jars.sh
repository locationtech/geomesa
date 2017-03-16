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

install_dir=$1
if [[ -z "${install_dir}" ]]; then
  install_dir="${%%gmtools.dist.name%%_HOME}/lib"
fi

NL=$'\n'
base_url='http://search.maven.org/remotecontent?filepath='

read -r -p "Install cassandra dependencies to ${install_dir}? (y/n) " confirm
confirm=${confirm,,} # Lowercasing
if [[ $confirm =~ ^(yes|y) || $confirm == "" ]]; then
  # Setup download URLs
  declare -a urls=(
    "${base_url}com/google/guava/guava/17.0/guava-17.0.jar"
    "${base_url}ch/qos/logback/logback-classic/1.1.3/logback-classic-1.1.3.jar"
    "${base_url}ch/qos/logback/logback-core/1.1.3/logback-core-1.1.3.jar"
  )

  # Download dependencies
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