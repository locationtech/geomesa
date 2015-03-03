rm $CATALINA_HOME/webapps/geoserver/WEB-INF/lib/geomesa-plugin-*-geoserver-plugin.jar 2>/dev/null
cp ../target/geomesa-plugin-*-geoserver-plugin.jar $CATALINA_HOME/webapps/geoserver/WEB-INF/lib/
if [ "$1" = "--web" ]; then
  rm $CATALINA_HOME/webapps/geoserver/WEB-INF/lib/geomesa-web-csv-*-geoserver-plugin.jar 2>/dev/null
  cp ../../geomesa-web/geomesa-web-csv/target/geomesa-web-csv-*-geoserver-plugin.jar $CATALINA_HOME/webapps/geoserver/WEB-INF/lib/
fi

