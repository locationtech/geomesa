#!/usr/bin/env bash
# Calculates CQs based on the following assumptions:
#
# 1. Any compile or provided scope dependency requires review

export LC_ALL=C # ensure stable sort order across different locales

cd "$(dirname "$0")/../.." || exit

rm build/dependencies.txt 2>/dev/null
mvn dependency:tree -Dstyle.color=never > build/deps-raw
grep ':compile' build/deps-raw | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] //' -e 's/[\| +-]*//' -e 's/(.*)//' -e 's/ //g' -e 's/\(.*\):\(.*\):jar:\(.*\):\(\w*\)/\1:\2:\3:\4/' | sort -u > build/dependencies.txt
echo "" >> build/dependencies.txt
while IFS= read -r cq; do
  if ! grep -q "${cq%:*}:" build/dependencies.txt; then
    echo "$cq" >> build/dependencies.txt
  fi
done < <(grep ':provided' build/deps-raw | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] //' -e 's/[\| +-]*//' -e 's/(.*)//' -e 's/ //g' -e 's/\(.*\):\(.*\):jar:\(.*\):\(\w*\)/\1:\2:\3:\4/' | sort -u)
rm build/deps-raw
