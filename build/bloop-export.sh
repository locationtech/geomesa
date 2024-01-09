#!/usr/bin/env bash

cd "$(dirname "$0")/.." || exit

mvn -Pbloop initialize ch.epfl.scala:bloop-maven-plugin:2.0.0:bloopInstall
