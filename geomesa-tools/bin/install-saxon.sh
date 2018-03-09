#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

saxon_version="9.7.0-14"
# Load common functions and setup
if [[ -z "${%%gmtools.dist.name%%_HOME}" ]]; then
  export %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $%%gmtools.dist.name%%_HOME/bin/common-functions.sh

install_dir="${1:-${%%gmtools.dist.name%%_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}net/sf/saxon/Saxon-HE/${saxon_version}/Saxon-HE-${saxon_version}.jar"
)

echo "Warning: Saxon is free to use and distribute, however, the provenance of the code could not be established by the Eclipse Foundation, and thus it is not distributed with GeoMesa. However, you may download it yourself."

downloadUrls "$install_dir" urls[@]
