# GeoMesa Web Data

### Building Instructions

This project contains web services for accessing GeoMesa.

If you wish to build this project separately, you can with maven:

```shell
geomesa> mvn clean install -pl geomesa-web/geomesa-web-data
```

### Installation Instructions

To install geomesa-web-data, copy ```geomesa-web/geomesa-web-data/target/geomesa-web-data-<version>-geoserver-plugin.jar```
into your geoserver lib folder. If not already present, you will also need the following jars:
 
```
geomesa-plugin-<version>-geoserver-plugin.jar
geomesa-web-core-version>.jar
geomesa-jobs-<version>.jar
spark-<version>-geomesa-assembly.jar
```

To get the required Spark jar, you may build the geomesa-web-install project using the 'assemble' profile.
Please note GeoMesa does not bundle Spark by default, and that Spark has not been approved for distribution
under the GeoMesa license.

You will need hadoop and slf4j jars that are compatible with your spark install. For a full list of jars
from a working GeoServer instance, refer to [Appendix A](#appendix-a-geoserver-jars). Of note, slf4j needs to be version 1.6.1.

In addition to installing jars, you will need to ensure that the hadoop configuration files are available
on the classpath. See the next sections for details.

#### Tomcat Installation

In Tomcat, this can be done by editing ```tomcat/bin/setenv.sh```. The exact line will depend
on your environment, but it will likely be one of the following:

```bash
CLASSPATH="$HADOOP_HOME/conf"
```
or
```bash
CLASSPATH="$HADOOP_CONF_DIR"
```

#### Jboss Installation

In Jboss, the easiest way to get the hadoop files on the classpath is to copy the contents of HADDOP_HOME/conf
into the exploded GeoServer war file under WEB-INF/classes.

Alternatively, you can add hadoop as a module, as described here: https://developer.jboss.org/wiki/HowToPutAnExternalFileInTheClasspath

You will need to exclude Jboss' custom slf4j module, as this interferes with Spark. To do so, add the
following exclusion to your GeoServer jboss-deployment-structure.xml:

```xml
<jboss-deployment-structure xmlns="urn:jboss:deployment-structure:1.1">
  <deployment>
    <dependencies>
        ...
    </dependencies>
    <exclusions>
      <module name="org.jboss.logging.jul-to-slf4j-stub" />
    </exclusions>
  </deployment>
</jboss-deployment-structure>

```

#### Advanced Configuration

##### Distributed Jars

The spark context will load a list of distributed jars to the remote cluster. This can be overridden by
supplying a file called ```spark-jars.list``` on the classpath. This file should contain jar file name prefixes,
one per line, which will be loaded from the environment. For example, to load slf4j-api-1.6.1.jar, you
would put a line in the file containing 'slf4j-api'.

##### Distributed Classloading

The spark context may not load distributed jars properly, resulting in serialization exceptions in the remote
cluster. You may force the classloading of distributed jars by setting the following system property:

```bash
-Dorg.locationtech.geomesa.spark.load-classpath=true
```

### Analytic Web Service

The analytic endpoint provides the ability to run spark jobs through a web service.

The main context path is ```/geoserver/geomesa/analytics```

#### Endpoints

The following paths are defined:

* POST /ds/:alias - Register a GeoMesa data store
  * instanceId
  * zookeepers
  * user
  * password
  * tableName
  * auths (optional)
  * visibilities (optional)
  * queryTimeout (optional)
  * queryThreads (optional)
  * recordThreads (optional)
  * writeMemory (optional)
  * writeThreads (optional)
  * collectStats (optional)
  * caching (optional)

  This method must be called to register any data store you wish to query later. It should not be called
  while the spark context is running. Registered data stores will persist between geoserver reboots.

* DELETE /ds/:alias - Delete a previously registered GeoMesa data store

* GET /ds/:alias - Display a registered GeoMesa data store

* GET /ds - Display all registered GeoMesa data stores

* POST /spark/config - Set spark configurations

  Options are passed as parameters. For a list of available options, see:

  https://spark.apache.org/docs/latest/configuration.html#available-properties <br/>
  https://spark.apache.org/docs/latest/running-on-yarn.html#spark-properties <br/>
  http://spark.apache.org/docs/latest/sql-programming-guide.html#caching-data-in-memory <br/>
  http://spark.apache.org/docs/latest/sql-programming-guide.html#other-configuration-options

  Configuration changes will not take place until the Spark SQL context is restarted. Configuration will
  persist between geoserver restarts.

* GET /spark/config - Displays the current spark configurations

* POST /sql/start - Start the Spark SQL context

* POST /sql/stop - Stop the Spark SQL context

* POST /sql/restart - Start, then stop the Spark SQL context

* GET /sql - Run a sql query
  * q or query - the SQL statement to execute
  * splits (optional) - the number of input splits to use in the Spark input format

  This method will execute a SQL query against any registered data stores. The Spark SQL context will be
  started if it is not currently running.

  The 'where' clause of the SQL statement may contain CQL, which will be applied separately. Columns must
  either be namespaced with a simple feature type name, or must be unambiguous among all registered simple
  feature types.

#### Response Formats

Responses can be returned in several different formats. This can be controlled by a request parameter, or
by an Accept header.

Request parameters:

```
format=txt
format=xml
format=json
```

Accept headers:

```
Accept: text/plain
Accept: application/xml
Accept: application/json
```

Text responses to SQL queries will be either TSV or CSV. The delimiter can be controlled with the 'delim'
parameter, which accepts the values 't', 'tab', 'c', or 'comma'.  

#### Example requests

Register a data store:

```
curl -d 'instanceId=myCloud' -d 'zookeepers=zoo1,zoo2,zoo3' -d 'tableName=myCatalog' -d 'user=user' -d 'password=password' http://localhost:8080/geoserver/geomesa/analytics/ds/myCatalog
```

Set the number of executors:

```
curl -d 'spark.executor.instances=10' http://localhost:8080/geoserver/geomesa/analytics/spark/conf
```

Group by: 

```
curl --header 'Accept: text/plain' --get --data-urlencode 'q=select mySft.myAttr, count(*) as count from mySft where bbox(mySft.geom, -115, 45, -110, 50) AND mySft.dtg during 2015-03-02T10:00:00.000Z/2015-03-02T11:00:00.000Z group by myattr' http://localhost:8080/geoserver/geomesa/analytics/sql
```

Join:

```
curl --header 'Accept: text/plain' --get --data-urlencode 'q=select mySft.myAttr, myOtherSft.myAttr from mySft, myOtherSft where bbox(mySft.geom, -115, 45, -110, 50) AND mySft.dtg during 2015-03-02T10:00:00.000Z/2015-03-02T11:00:00.000Z AND  bbox(myOtherSft.geom, -115, 45, -110, 50) AND myOtherSft.dtg during 2015-03-02T10:00:00.000Z/2015-03-02T11:00:00.000Z AND mySft.myJoinField = myOtherSft.myJoinField' http://localhost:8080/geoserver/geomesa/analytics/sql
```

### Appendix A: GeoServer Jars

| jar | size |
| --- | ---- |
| accumulo-core-1.5.2.jar | 3748459 |
| accumulo-fate-1.5.2.jar | 99782 |
| accumulo-start-1.5.2.jar | 53902 |
| accumulo-trace-1.5.2.jar | 116904 |
| activation-1.1.jar | 62983 |
| aopalliance-1.0.jar | 4467 |
| batik-anim-1.7.jar | 95313 |
| batik-awt-util-1.7.jar | 401858 |
| batik-bridge-1.7.jar | 558892 |
| batik-css-1.7.jar | 310919 |
| batik-dom-1.7.jar | 173530 |
| batik-ext-1.7.jar | 10257 |
| batik-gvt-1.7.jar | 242866 |
| batik-parser-1.7.jar | 73119 |
| batik-script-1.7.jar | 60604 |
| batik-svg-dom-1.7.jar | 601098 |
| batik-svggen-1.7.jar | 215274 |
| batik-transcoder-1.7.jar | 121997 |
| batik-util-1.7.jar | 128286 |
| batik-xml-1.7.jar | 30843 |
| bcprov-jdk14-1.46.jar | 1824421 |
| cglib-nodep-2.2.jar | 322362 |
| common-2.6.0.jar | 211652 |
| commons-beanutils-1.7.0.jar | 188671 |
| commons-cli-1.2.jar | 41123 |
| commons-codec-1.8.jar | 263865 |
| commons-collections-3.2.1.jar | 575389 |
| commons-configuration-1.6.jar | 298829 |
| commons-dbcp-1.3.jar | 148817 |
| commons-digester-1.7.jar | 139966 |
| commons-fileupload-1.2.1.jar | 57779 |
| commons-httpclient-3.1.jar | 305001 |
| commons-io-2.1.jar | 163151 |
| commons-jxpath-1.3.jar | 299994 |
| commons-lang-2.5.jar | 279193 |
| commons-logging-1.1.1.jar | 60686 |
| commons-math3-3.3.jar | 1952352 |
| commons-pool-1.5.3.jar | 96203 |
| commons-validator-1.1.4.jar | 84462 |
| com.noelios.restlet-1.0.8.jar | 150629 |
| com.noelios.restlet.ext.servlet-1.0.8.jar | 14072 |
| com.noelios.restlet.ext.simple-1.0.8.jar | 10114 |
| ecore-2.6.1.jar | 1231403 |
| ehcache-1.6.2.jar | 203035 |
| encoder-1.1.jar | 37176 |
| ezmorph-1.0.6.jar | 86487 |
| freemarker-2.3.18.jar | 924269 |
| geomesa-jobs-1.1.0-rc.6-SNAPSHOT.jar | 542802 |
| geomesa-plugin-1.1.0-rc.6-SNAPSHOT-geoserver-plugin.jar | 22453501 |
| geomesa-process-1.1.0-rc.6-SNAPSHOT.jar | 57247 |
| geomesa-web-core-1.1.0-rc.6-SNAPSHOT.jar | 23129 |
| geomesa-web-data-1.1.0-rc.6-SNAPSHOT-geoserver-plugin.jar | 42073524 |
| gs-gwc-2.5.2.jar | 166344 |
| gs-kml-2.5.2.jar | 180098 |
| gs-main-2.5.2.jar | 1668898 |
| gs-ows-2.5.2.jar | 165295 |
| gs-platform-2.5.2.jar | 43513 |
| gs-rest-2.5.2.jar | 54111 |
| gs-restconfig-2.5.2.jar | 216697 |
| gs-sec-jdbc-2.5.2.jar | 53783 |
| gs-sec-ldap-2.5.2.jar | 41977 |
| gs-wcs1_0-2.5.2.jar | 115594 |
| gs-wcs1_1-2.5.2.jar | 152920 |
| gs-wcs2_0-2.5.2.jar | 418107 |
| gs-wcs-2.5.2.jar | 46312 |
| gs-web-core-2.5.2.jar | 1305588 |
| gs-web-demo-2.5.2.jar | 355314 |
| gs-web-gwc-2.5.2.jar | 279018 |
| gs-web-sec-core-2.5.2.jar | 514960 |
| gs-web-sec-jdbc-2.5.2.jar | 23010 |
| gs-web-sec-ldap-2.5.2.jar | 19971 |
| gs-web-wcs-2.5.2.jar | 73698 |
| gs-web-wfs-2.5.2.jar | 22251 |
| gs-web-wms-2.5.2.jar | 114588 |
| gs-web-wps-2.5.2.jar | 85674 |
| gs-wfs-2.5.2.jar | 664966 |
| gs-wms-2.5.2.jar | 775325 |
| gs-wps-core-2.5.2.jar | 280694 |
| gt-api-11.2.jar | 162986 |
| gt-arcgrid-11.2.jar | 23725 |
| gt-coverage-11.2.jar | 485797 |
| gt-cql-11.2.jar | 195799 |
| gt-data-11.2.jar | 71304 |
| gt-epsg-hsql-11.2.jar | 1960273 |
| gt-geojson-11.2.jar | 62453 |
| gt-geotiff-11.2.jar | 28670 |
| gt-graph-11.2.jar | 170023 |
| gt-grid-11.2.jar | 35068 |
| gt-gtopo30-11.2.jar | 37216 |
| gt-image-11.2.jar | 22080 |
| gt-imagemosaic-11.2.jar | 385321 |
| gt-jdbc-11.2.jar | 202771 |
| gt-jdbc-postgis-11.2.jar | 41042 |
| gt-main-11.2.jar | 1678026 |
| gt-metadata-11.2.jar | 505718 |
| gt-opengis-11.2.jar | 345671 |
| gt-process-11.2.jar | 56613 |
| gt-process-feature-11.2.jar | 156131 |
| gt-process-geometry-11.2.jar | 11985 |
| gt-process-raster-11.2.jar | 121036 |
| gt-property-11.2.jar | 26798 |
| gt-referencing-11.2.jar | 1143703 |
| gt-render-11.2.jar | 456598 |
| gt-shapefile-11.2.jar | 202177 |
| gt-svg-11.2.jar | 8802 |
| gt-transform-11.2.jar | 40133 |
| gt-validation-11.2.jar | 210353 |
| gt-wfs-11.2.jar | 323911 |
| gt-wms-11.2.jar | 223944 |
| gt-xml-11.2.jar | 640672 |
| gt-xsd-core-11.2.jar | 302851 |
| gt-xsd-fes-11.2.jar | 56184 |
| gt-xsd-filter-11.2.jar | 105575 |
| gt-xsd-gml2-11.2.jar | 82530 |
| gt-xsd-gml3-11.2.jar | 1522493 |
| gt-xsd-ows-11.2.jar | 119999 |
| gt-xsd-sld-11.2.jar | 173268 |
| gt-xsd-wcs-11.2.jar | 148122 |
| gt-xsd-wfs-11.2.jar | 124342 |
| gt-xsd-wps-11.2.jar | 39793 |
| guava-11.0.1.jar | 1649781 |
| gwc-core-1.5.3.jar | 440482 |
| gwc-diskquota-core-1.5.3.jar | 88558 |
| gwc-diskquota-jdbc-1.5.3.jar | 52595 |
| gwc-georss-1.5.3.jar | 34260 |
| gwc-gmaps-1.5.3.jar | 8390 |
| gwc-kml-1.5.3.jar | 21311 |
| gwc-rest-1.5.3.jar | 55385 |
| gwc-tms-1.5.3.jar | 9881 |
| gwc-ve-1.5.3.jar | 5357 |
| gwc-wms-1.5.3.jar | 35600 |
| gwc-wmts-1.5.3.jar | 18909 |
| h2-1.1.119.jar | 1207393 |
| hadoop-annotations-2.2.0.jar | 16778 |
| hadoop-auth-2.2.0.jar | 49750 |
| hadoop-client-2.2.0.jar | 2559 |
| hadoop-common-2.2.0.jar | 2735584 |
| hadoop-hdfs-2.2.0.jar | 5242252 |
| hadoop-mapreduce-client-app-2.2.0.jar | 482042 |
| hadoop-mapreduce-client-common-2.2.0.jar | 656365 |
| hadoop-mapreduce-client-core-2.2.0.jar | 1455001 |
| hadoop-mapreduce-client-jobclient-2.2.0.jar | 35216 |
| hadoop-mapreduce-client-shuffle-2.2.0.jar | 21537 |
| hadoop-yarn-api-2.2.0.jar | 1158936 |
| hadoop-yarn-applications-distributedshell-2.2.0.jar | 32481 |
| hadoop-yarn-applications-unmanaged-am-launcher-2.2.0.jar | 13300 |
| hadoop-yarn-client-2.2.0.jar | 94728 |
| hadoop-yarn-common-2.2.0.jar | 1301627 |
| hadoop-yarn-server-common-2.2.0.jar | 175554 |
| hadoop-yarn-server-nodemanager-2.2.0.jar | 467638 |
| hadoop-yarn-server-resourcemanager-2.2.0.jar | 615387 |
| hadoop-yarn-server-web-proxy-2.2.0.jar | 25710 |
| hadoop-yarn-site-2.2.0.jar | 1935 |
| hsqldb-2.2.8.jar | 1395415 |
| htmlvalidator-1.2.jar | 243854 |
| imageio-ext-arcgrid-1.1.10.jar | 39836 |
| imageio-ext-geocore-1.1.10.jar | 25436 |
| imageio-ext-streams-1.1.10.jar | 52276 |
| imageio-ext-tiff-1.1.10.jar | 318010 |
| imageio-ext-utilities-1.1.10.jar | 40588 |
| itext-2.1.5.jar | 1117661 |
| jackson-core-asl-1.9.13.jar | 232248 |
| jackson-mapper-asl-1.9.3.jar | 773019 |
| jai_codec-1.1.3.jar | 258160 |
| jai_core-1.1.3.jar | 1900631 |
| jai_imageio-1.1.jar | 1140632 |
| jasypt-1.8.jar | 178961 |
| JavaAPIforKml-2.2.0.jar | 619507 |
| jdom-1.0.jar | 153253 |
| jdom-1.1.3.jar | 151304 |
| jettison-1.0.1.jar | 56702 |
| jgridshift-1.0.jar | 11497 |
| jline-2.10.3.jar | 164623 |
| json-lib-2.2.3-jdk15.jar | 148490 |
| json-simple-1.1.jar | 16046 |
| jsr-275-1.0-beta-2.jar | 91347 |
| jsr305-1.3.9.jar | 33015 |
| jt-attributeop-1.3.1.jar | 3839 |
| jt-contour-1.3.1.jar | 16794 |
| jt-rangelookup-1.3.1.jar | 16777 |
| jts-1.13.jar | 794991 |
| jt-utils-1.3.1.jar | 202251 |
| jt-vectorbinarize-1.3.1.jar | 10573 |
| jt-vectorize-1.3.1.jar | 14230 |
| jt-zonalstats-1.3.1.jar | 19969 |
| kafka_2.10-0.8.2.1.jar | 3991269 |
| kafka-clients-0.8.2.1.jar | 324010 |
| libthrift-0.9.0.jar | 347531 |
| log4j-1.2.15.jar | 391834 |
| lz4-1.2.0.jar | 165505 |
| mail-1.4.jar | 388864 |
| metrics-core-2.2.0.jar | 82123 |
| net.opengis.fes-11.2.jar | 227173 |
| net.opengis.ows-11.2.jar | 528859 |
| net.opengis.wcs-11.2.jar | 657924 |
| net.opengis.wfs-11.2.jar | 427887 |
| net.opengis.wps-11.2.jar | 196387 |
| org.json-2.0.jar | 48752 |
| org.restlet-1.0.8.jar | 175177 |
| org.restlet.ext.freemarker-1.0.8.jar | 2180 |
| org.restlet.ext.json-1.0.8.jar | 1730 |
| org.restlet.ext.spring-1.0.8.jar | 4504 |
| org.simpleframework-3.1.3.jar | 225333 |
| org.w3.xlink-11.2.jar | 52992 |
| oro-2.0.8.jar | 65261 |
| picocontainer-1.2.jar | 112635 |
| pngj-2.0.1.jar | 142870 |
| postgresql-8.4-701.jdbc3.jar | 472831 |
| protobuf-java-2.5.0.jar | 533455 |
| scala-compiler-2.10.5.jar | 14472529 |
| scala-library-2.10.5.jar | 7130751 |
| scala-reflect-2.10.5.jar | 3206152 |
| serializer-2.7.1.jar | 278281 |
| slf4j-api-1.6.1.jar | 25496 |
| slf4j-log4j12-1.6.1.jar | 9753 |
| snappy-java-1.1.1.6.jar | 592319 |
| spark-1.1.0-rc.6-SNAPSHOT-geomesa-assembly.jar | 73457081 |
| spring-aop-3.1.4.RELEASE.jar | 332932 |
| spring-asm-3.1.4.RELEASE.jar | 53082 |
| spring-beans-3.1.4.RELEASE.jar | 597184 |
| spring-context-3.1.4.RELEASE.jar | 838801 |
| spring-context-support-3.1.4.RELEASE.jar | 107164 |
| spring-core-3.1.4.RELEASE.jar | 451269 |
| spring-expression-3.1.4.RELEASE.jar | 179323 |
| spring-jdbc-3.1.4.RELEASE.jar | 405635 |
| spring-ldap-core-1.3.1.RELEASE.jar | 231729 |
| spring-security-config-3.1.0.RELEASE.jar | 202754 |
| spring-security-core-3.1.0.RELEASE.jar | 348567 |
| spring-security-crypto-3.1.0.RELEASE.jar | 41068 |
| spring-security-ldap-3.1.0.RELEASE.jar | 93631 |
| spring-security-web-3.1.0.RELEASE.jar | 255577 |
| spring-tx-3.1.4.RELEASE.jar | 245483 |
| spring-web-3.1.4.RELEASE.jar | 554802 |
| spring-webmvc-3.1.4.RELEASE.jar | 579461 |
| stax-1.2.0.jar | 179346 |
| stax-api-1.0.1.jar | 26514 |
| vecmath-1.3.2.jar | 249354 |
| wicket-1.4.12.jar | 1903610 |
| wicket-extensions-1.4.12.jar | 1179862 |
| wicket-ioc-1.4.12.jar | 23286 |
| wicket-spring-1.4.12.jar | 29377 |
| xml-apis-1.0.b2.jar | 109318 |
| xml-apis-ext-1.3.04.jar | 85686 |
| xml-commons-resolver-1.2.jar | 84091 |
| xmlpull-1.1.3.1.jar | 7188 |
| xpp3-1.1.3.4.O.jar | 119888 |
| xpp3_min-1.1.4c.jar | 24956 |
| xsd-2.6.0.jar | 992820 |
| xstream-1.4.7.jar | 531571 |
| zkclient-0.3.jar | 64009 |
| zookeeper-3.3.6.jar | 608239 |
