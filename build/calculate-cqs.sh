#!/usr/bin/env bash
# Calculates CQs based on the following assumptions:
#
# 1. Any direct or transitive compile scope dependency require a full CQ
# 2. Any direct provided scope dependencies require a 'works-with' CQ
# 3. Any direct test scope dependencies require a single 'test' CQ
# 4. Any transitive provided or test dependencies can be disregarded for IP purposes

export LC_ALL=C # ensure stable sort order across different locales

rm build/cqs.tsv 2>/dev/null
rm build/eclipse-dependencies.list 2>/dev/null
mvn dependency:tree -Dstyle.color=never > build/deps-raw
grep ':compile' build/deps-raw | grep -v 'omitted' | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] //' -e 's/[\| +-]*//' -e 's/(.*)//' -e 's/ //g' -e 's/\(.*\):\(.*\):jar:\(.*\):\(\w*\)/\1:\2\t\3\t\4/' | sort | uniq > build/cqs.tsv
echo "" >> build/cqs.tsv
for cq in $(grep ':provided' build/deps-raw | grep '^\[INFO\] +-' | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] +- //' -e 's/(.*)//' | sort | uniq); do
  dep="$(echo "$cq" | sed -E 's/(.*):(.*):jar:(.*):(\w*)/\1:\2\t\3\t\4/')"
  reg=$(echo "${dep%	*}" | sed -E 's/\s\s*/\\s*/g')
  if ! grep -q "$reg" build/cqs.tsv; then
    echo "$dep" >> build/cqs.tsv
  fi
done
# format compile and provided scope dependencies for processing by eclipse license tool
# exclude geotools as it doesn't pass automated tests but we have a license exemption for it
grep -v '^org.geotools' build/cqs.tsv | awk -F '\t' 'NF { print $1":"$2 }' > build/eclipse-dependencies.list
echo "" >> build/cqs.tsv
for cq in $(grep ':test' build/deps-raw | grep '^\[INFO\] +-' | grep -v 'org.locationtech.geomesa' | sed -e 's/\[INFO\] +- //' -e 's/(.*)//' | sort | uniq); do
  dep="$(echo "$cq" | sed -E 's/(.*):(.*):jar:(.*):(\w*)/\1:\2\t\3\t\4/')"
  reg=$(echo "${dep%	*}" | sed -E 's/\s\s*/\\s*/g')
  if ! grep -q "$reg" build/cqs.tsv; then
    echo "$dep" >> build/cqs.tsv
  fi
done
rm build/deps-raw

