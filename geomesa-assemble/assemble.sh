#!/bin/bash
cd ../geomesa-tools
mvn clean install
cd ../geomesa-assemble
mvn clean install
tar xvzf target/*.tar.gz