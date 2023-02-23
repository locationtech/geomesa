#! /usr/bin/env bash
# runs a tomcat docker to host doc pages at http://localhost:8080/
# requires docs to be built, first
dir="$(cd "$(dirname "$0")/.." || exit; pwd)"
docker run --rm -p 8080:8080 \
  -v "$dir/target/html:/usr/local/tomcat/webapps/ROOT" \
  tomcat:9.0.70-jdk11
