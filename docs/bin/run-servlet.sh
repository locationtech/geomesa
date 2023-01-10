#! /usr/bin/env bash
# runs a tomcat docker to host doc pages at http://localhost:8080/
# requires docs to be built, first
<<<<<<< HEAD
dir="$(cd "$(dirname "$0")/.." || exit; pwd)"
docker run --rm -p 8080:8080 \
  -v "$dir/target/html:/usr/local/tomcat/webapps/ROOT" \
=======
dir="$(cd "`dirname "$0"`"; pwd)"
docker run --rm -p 8080:8080 \
  -v $dir/../target/html:/usr/local/tomcat/webapps/ROOT \
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  tomcat:9.0.70-jdk11
