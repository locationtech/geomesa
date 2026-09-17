#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

VALID_VERSIONS=("2.12" "2.13")
PATCH_VERSIONS=("21" "18")

usage() {
  echo "Usage: $(basename "$0") [-h|--help] <version>
where :
  -h| --help Display this help text
  valid version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

BASEDIR="$(dirname "$0")/../.."
TO_VERSION=$1
TO_PATCH_VERSION=""

for i in "${!VALID_VERSIONS[@]}"; do
  if [[ $TO_VERSION == "${VALID_VERSIONS[$i]}" ]]; then
    TO_PATCH_VERSION="${PATCH_VERSIONS[$i]}"
  fi
done

if [[ -z "$TO_PATCH_VERSION" ]]; then
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
fi

# pull out the scala version from the main artifactId
FROM_VERSION="$(sed -n '/geomesa_/ s| *<artifactId>geomesa_\([0-9]\.[0-9][0-9]*\)</artifactId>|\1|p' "$BASEDIR"/pom.xml)"

# Determine the correct sed command arguments based on the operating system
case "$OSTYPE" in
  darwin*|bsd*)
    sed_no_backup=( -i '' )
    ;;
  *)
    sed_no_backup=( -i )
    ;;
esac

# Process all pom.xml files, handling both macOS and Linux find behavior
find "$BASEDIR" -name 'pom.xml' -not -path '*target*' | while read -r file; do
  sed "${sed_no_backup[@]}" "s/\(artifactId.*\)_$FROM_VERSION/\1_$TO_VERSION/g" "$file"
done

# update scala.binary.version and scala.patch.version in .mvn/maven.config
# match any scala binary version to ensure idempotency
sed "${sed_no_backup[@]}" "s/-Dscala\.binary\.version=[0-9]\.[0-9][0-9]*/-Dscala.binary.version=$TO_VERSION/" \
  "$BASEDIR/.mvn/maven.config"
sed "${sed_no_backup[@]}" "s/-Dscala\.patch\.version=[0-9][0-9]*/-Dscala.patch.version=$TO_PATCH_VERSION/" \
  "$BASEDIR/.mvn/maven.config"

# Update enforcer rules
sed "${sed_no_backup[@]}" "s|<exclude>\*:\*_$TO_VERSION</exclude>|<exclude>*:*_$FROM_VERSION</exclude>|" "$BASEDIR/pom.xml"
