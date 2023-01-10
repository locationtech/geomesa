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

VALID_VERSIONS=("2.12" "2.13")
<<<<<<< HEAD
FULL_VERSIONS=("2.12.19" "2.13.12") # note: 2.13.13 breaks the zinc compile server
=======
<<<<<<< HEAD
FULL_VERSIONS=("2.12.17" "2.13.10")
=======
FULL_VERSIONS=("2.12.13" "2.13.10")
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)

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

<<<<<<< HEAD
BASEDIR=$(dirname "$0")/..
=======
BASEDIR=$(dirname $0)/..
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
TO_VERSION=$1
FULL_VERSION=""

for i in "${!VALID_VERSIONS[@]}"; do
<<<<<<< HEAD
  if [[ $TO_VERSION == "${VALID_VERSIONS[$i]}" ]]; then
=======
  if [[ $TO_VERSION == ${VALID_VERSIONS[$i]} ]]; then
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    FULL_VERSION="${FULL_VERSIONS[$i]}"
  fi
done

if [[ -z "$FULL_VERSION" ]]; then
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
fi

# pull out the scala version from the main artifactId
<<<<<<< HEAD
FROM_VERSION="$(sed -n '/geomesa_/ s| *<artifactId>geomesa_\([0-9]\.[0-9][0-9]*\)</artifactId>|\1|p' "$BASEDIR"/pom.xml)"

=======
FROM_VERSION="$(sed -n '/geomesa_/ s| *<artifactId>geomesa_\([0-9]\.[0-9][0-9]*\)</artifactId>|\1|p' $BASEDIR/pom.xml)"

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

export -f sed_i

>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
find "$BASEDIR" -name 'pom.xml' -not -path '*target*' -print \
  -exec sed -i "s/\(artifactId.*\)_$FROM_VERSION/\1_$TO_VERSION/g" {} \;

# update <scala.binary.version> in parent POM
# match any scala binary version to ensure idempotency
<<<<<<< HEAD
sed -i "1,/<scala\.binary\.version>[0-9]\.[0-9][0-9]*</s/<scala\.binary\.version>[0-9]\.[0-9][0-9]*</<scala.binary.version>$TO_VERSION</" \
  "$BASEDIR/pom.xml"

# update <scala.version> in parent POM
sed -i "1,/<scala\.version>[0-9]\.[0-9][0-9]*\.[0-9][0-9]*</s/<scala\.version>[0-9]\.[0-9][0-9]*\.[0-9][0-9]*</<scala.version>$FULL_VERSION</" \
=======
sed_i '1,/<scala\.binary\.version>[0-9]\.[0-9][0-9]*</s/<scala\.binary\.version>[0-9]\.[0-9][0-9]*</<scala.binary.version>'$TO_VERSION'</' \
  "$BASEDIR/pom.xml"

# update <scala.version> in parent POM
sed_i '1,/<scala\.version>[0-9]\.[0-9][0-9]*\.[0-9][0-9]*</s/<scala\.version>[0-9]\.[0-9][0-9]*\.[0-9][0-9]*</<scala.version>'$FULL_VERSION'</' \
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  "$BASEDIR/pom.xml"

# Update enforcer rules
sed -i "s|<exclude>\*:\*_$TO_VERSION</exclude>|<exclude>*:*_$FROM_VERSION</exclude>|" "$BASEDIR/pom.xml"
sed -i "s|<regex>$FROM_VERSION\.\*</regex>|<regex>$TO_VERSION.*</regex>|" "$BASEDIR/pom.xml"
sed -i "s|<regex>$FROM_VERSION</regex>|<regex>$TO_VERSION</regex>|" "$BASEDIR/pom.xml"
