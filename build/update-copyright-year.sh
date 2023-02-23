#!/usr/bin/env bash

to="$(date +%Y)"
from="$((to - 1))"
dir="$(cd "$(dirname "$0")/.." || exit; pwd)"

sed -i "s|<copyright.year>$from</copyright.year>|<copyright.year>$to</copyright.year>|" "$dir/pom.xml"

for file in $(find "$dir" -name '*.scala') $(find "$dir" -name '*.java'); do
  sed -i \
    -e "s/Copyright (c) 2013-$from Commonwealth Computer Research, Inc\./Copyright (c) 2013-$to Commonwealth Computer Research, Inc./" \
    -e "s/Copyright (c) 2016-$from Dstl/Copyright (c) 2016-$to Dstl/" \
    -e "s/Copyright (c) 2017-$from IBM/Copyright (c) 2017-$to IBM/" \
    -e "s/Copyright (c) 2019-$from The MITRE Corporation/Copyright (c) 2019-$to The MITRE Corporation/" \
    "$file"
done
