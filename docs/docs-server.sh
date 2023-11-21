#! /usr/bin/env bash

dir="$(cd "$(dirname "$0")" || exit 1; pwd)"

docker run --rm \
  -p 8080:8080 \
  -v "$dir/target/html:/usr/local/tomcat/webapps/ROOT" \
  tomcat:9.0-jdk11
