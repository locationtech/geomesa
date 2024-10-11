#!/bin/bash
set -e
set -u
set -o pipefail

cd "$(dirname "$0")/../.." || exit

export LC_ALL=C # ensure stable sort order across different locales
POM="geomesa-utils-parent/geomesa-bom/pom.xml"

echo "Running maven build to generate installed artifact list"
deps=()
# mapfile reads the results into an array
# we get the list of artifacts from `mvn clean install` (seems to be only way...)
mapfile -t deps < <(
  mvn clean install -DskipTests -Pzinc -B -T2C 2>&1 |
  grep Installing | # pull out installed artifacts only
  grep -v -e "\-sources\.jar$" -e "\.pom$" | # skip sources jars and poms
  sed 's|.*\.m2/repository/org/locationtech/geomesa/||' | # strip line prefix
  sed 's|\.|!|g' | # replace . with ! so that classifiers sort after regular artifact
  sort | # sort artifacts
  sed 's|!|.|g' # undo classifier sort hack
)

# truncate everything after the opening dependencyManagement
sed -i '/    <dependencyManagement>/q' "$POM"
echo -e "        <dependencies>\n" >> "$POM"

function printDependency() {
  local dep="$1"
  # elements look like 'geomesa-utils_2.12/4.0.0-SNAPSHOT/geomesa-utils_2.12-4.0.0-SNAPSHOT.jar'
  artifact="$(echo "$dep" | awk -F '/' '{ print $1 }')"
  version="$(echo "$dep" | awk -F '/' '{ print $2 }')"
  classifier=""
  if ! [[ $dep =~ .*$artifact-$version.jar ]]; then
    classifier=$'\n'"                <classifier>$(echo "$dep" | sed -E 's/.*-([a-z]+)\.jar/\1/')</classifier>"
    if [[ $classifier =~ test ]]; then
      classifier="$classifier"$'\n'"                <scope>test</scope>"
    fi
  fi
  {
    echo "            <dependency>"
    echo "                <groupId>org.locationtech.geomesa</groupId>"
    echo "                <artifactId>${artifact%_*}_\${scala.binary.version}</artifactId>"
    echo "                <version>\${geomesa.version}</version>$classifier"
    echo "            </dependency>"
  } | tee -a "$POM"
}

for dep in "${deps[@]}"; do
  if ! [[ $dep =~ .*tests.jar ]]; then
    printDependency "$dep"
  fi
done

echo -e "\n            <!-- test dependencies -->\n" | tee -a "$POM"

for dep in "${deps[@]}"; do
  if [[ $dep =~ .*tests.jar ]]; then
    printDependency "$dep"
  fi
done

echo "
        </dependencies>
    </dependencyManagement>
</project>" >> "$POM"
