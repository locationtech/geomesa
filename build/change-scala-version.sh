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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
FULL_VERSIONS=("2.12.19" "2.13.12") # note: 2.13.13 breaks the zinc compile server
=======
=======
>>>>>>> ce69a697516 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
>>>>>>> eb0bd279638 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 603c7b9204a (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8a20d811a7c (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
>>>>>>> be6b3b14b4a (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccf4d7c3bd (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
<<<<<<< HEAD
>>>>>>> b345d185175 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 603c7b9204a (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f01f17feca (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
<<<<<<< HEAD
>>>>>>> 8a20d811a7c (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> be6b3b14b4a (GEOMESA-3254 Add Bloop build support)
FULL_VERSIONS=("2.12.17" "2.13.10")
=======
FULL_VERSIONS=("2.12.13" "2.13.10")
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b345d185175 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8a20d811a7c (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
FULL_VERSIONS=("2.12.17" "2.13.10")
>>>>>>> 27d2a13b23 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
<<<<<<< HEAD
>>>>>>> ce69a697516 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
=======
FULL_VERSIONS=("2.12.17" "2.13.10")
=======
FULL_VERSIONS=("2.12.13" "2.13.10")
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eb0bd279638 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 603c7b9204a (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b3b14b4a (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
FULL_VERSIONS=("2.12.17" "2.13.10")
>>>>>>> 27d2a13b23 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
>>>>>>> ccf4d7c3bd (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
<<<<<<< HEAD
>>>>>>> b345d185175 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 603c7b9204a (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
FULL_VERSIONS=("2.12.17" "2.13.10")
>>>>>>> 27d2a13b23 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
>>>>>>> f01f17feca (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
<<<<<<< HEAD
>>>>>>> 8a20d811a7c (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> be6b3b14b4a (GEOMESA-3254 Add Bloop build support)

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
