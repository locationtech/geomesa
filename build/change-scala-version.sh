#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

VALID_VERSIONS=( 2.11 2.12 )

SCALA_2_11_VERSION="2.11.7"
SCALA_2_12_VERSION="2.12.13"

usage() {
  echo "Usage: $(basename $0) [-h|--help] <version>
where :
  -h| --help Display this help text
  valid version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_VERSION=$1

check_scala_version() {
  for i in ${VALID_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_VERSION"

if [ $TO_VERSION = "2.12" ]; then
  FULL_VERSION="2.12.13"
  FROM_VERSION="2.11"
else
  FULL_VERSION="2.11.7"
  FROM_VERSION="2.12"
fi

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

export -f sed_i

BASEDIR=$(dirname $0)/..
find "$BASEDIR" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(artifactId.*\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' {}" \;

# Also update <scala.binary.version> in parent POM
# Match any scala binary version to ensure idempotency
sed_i '1,/<scala\.binary\.version>[0-9]*\.[0-9]*</s/<scala\.binary\.version>[0-9]*\.[0-9]*</<scala.binary.version>'$TO_VERSION'</' \
  "$BASEDIR/pom.xml"

# Also update <scala.version> in parent POM
sed_i '1,/<scala\.version>[0-9]*\.[0-9]*\.[0-9]*</s/<scala\.version>[0-9]*\.[0-9]*\.[0-9]*</<scala.version>'$FULL_VERSION'</' \
  "$BASEDIR/pom.xml"

# Update enforcer rules
sed_i 's|<exclude>\*:\*_'$TO_VERSION'</exclude>|<exclude>*:*_'$FROM_VERSION'</exclude>|' "$BASEDIR/pom.xml"
sed_i 's|<regex>'$FROM_VERSION'\.\*</regex>|<regex>'$TO_VERSION'.*</regex>|' "$BASEDIR/pom.xml"
sed_i 's|<regex>'$FROM_VERSION'</regex>|<regex>'$TO_VERSION'</regex>|' "$BASEDIR/pom.xml"
