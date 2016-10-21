#!/usr/bin/env bash
rm $CATALINA_HOME/webapps/geoserver/WEB-INF/lib/geomesa-accumulo-gs-plugin*.jar 2>/dev/null
cp ../target/geomesa-accumulo-gs-plugin*.jar $CATALINA_HOME/webapps/geoserver/WEB-INF/lib/
