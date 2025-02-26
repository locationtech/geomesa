#!/usr/bin/env bash

cd "$(dirname "$0")/../.." || exit

# this sed expression changes the output of the versions plugin, which looks like:
# [INFO]   maven-shade-plugin ................................. 3.5.1 -> 3.6.0
# into something that looks like:
# <maven.shade.plugin.version>3.6.0</maven.shade.plugin.version>
mapfile -t updates < <(mvn versions:display-plugin-updates | grep '\->' | grep -v beta | \
  sed -e 's/\[INFO\]   \(.*:\)\?//' -e 's/^/</' -e 's/ \..*-> /-version>/' -e 's|<\(.*>\)\(.*\)|<\1\2</\1|' -e 's/-/./g' | sort)

for update in "${updates[@]}"; do
  if grep -q "${update%%>*}" pom.xml; then
    sed -i "s|${update%%>*}.*|$update|" pom.xml
  else
    # property doesn't match - pin on the end and fix it by hand
    echo "$update" >> pom.xml
  fi
done
