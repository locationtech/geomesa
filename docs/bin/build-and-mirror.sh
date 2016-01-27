#!/bin/bash
#
# Builds the HTML documentation tree and mirrors it to the geomesa.github.io repository.

# IMPORTANT: Update these paths for your system!
GEOMESA=/opt/devel/src/geomesa
GEOMESA_GITHUB_IO=/opt/devel/src/geomesa.github.io

mvn clean install -f $GEOMESA/pom.xml
rsync -av $GEOMESA/docs/target/html/ $GEOMESA_GITHUB_IO/documentation/test/
