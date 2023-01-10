#!/usr/bin/env bash

<<<<<<< HEAD
cd "$(dirname "$0")/.." || exit

<<<<<<< HEAD
mvn -Pbloop initialize ch.epfl.scala:bloop-maven-plugin:2.0.0:bloopInstall
=======
if mvn -Pbloop ch.epfl.scala:bloop-maven-plugin:2.0.0:bloopInstall; then
  if [[ -f .bloop/geomesa-utils_2.12.json ]] && ! grep -q 'src/main/scala_2.12' .bloop/geomesa-utils_2.12.json; then
    sed -i 's|\(.*/geomesa-utils/src/main/scala\)"|\0,\n\1\_2.12"|' .bloop/geomesa-utils_2.12.json
  fi
  if [[ -f .bloop/geomesa-utils_2.13.json ]] && ! grep -q 'src/main/scala_2.13' .bloop/geomesa-utils_2.13.json; then
=======
mvn -Pbloop ch.epfl.scala:bloop-maven-plugin:2.0.0:bloopInstall
if [[ $? -eq 0 ]]; then
  if [[ -f .bloop/geomesa-utils_2.12.json && -z "$(grep 'src/main/scala_2.12' .bloop/geomesa-utils_2.12.json)" ]]; then
    sed -i 's|\(.*/geomesa-utils/src/main/scala\)"|\0,\n\1\_2.12"|' .bloop/geomesa-utils_2.12.json
  fi
  if [[ -f .bloop/geomesa-utils_2.13.json && -z "$(grep 'src/main/scala_2.13' .bloop/geomesa-utils_2.13.json)" ]]; then
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    sed -i 's|\(.*/geomesa-utils/src/main/scala\)"|\0,\n\1\_2.13"|' .bloop/geomesa-utils_2.13.json
  fi
fi
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
