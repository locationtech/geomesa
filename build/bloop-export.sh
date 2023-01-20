#!/usr/bin/env bash

mvn -Pbloop ch.epfl.scala:bloop-maven-plugin:2.0.0:bloopInstall
if [[ $? -eq 0 ]]; then
  if [[ -f .bloop/geomesa-utils_2.12.json && -z "$(grep 'src/main/scala_2.12' .bloop/geomesa-utils_2.12.json)" ]]; then
    sed -i 's|\(.*/geomesa-utils/src/main/scala\)"|\0,\n\1\_2.12"|' .bloop/geomesa-utils_2.12.json
  fi
  if [[ -f .bloop/geomesa-utils_2.13.json && -z "$(grep 'src/main/scala_2.13' .bloop/geomesa-utils_2.13.json)" ]]; then
    sed -i 's|\(.*/geomesa-utils/src/main/scala\)"|\0,\n\1\_2.13"|' .bloop/geomesa-utils_2.13.json
  fi
fi
