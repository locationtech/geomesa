# GeoMesa Plugin

### Building Instructions

If you wish to build this project separately, you can with mvn clean install

```geomesa/geomesa-plugin> mvn clean install```

This will produce jars in target/ which are intended to be as a plugin for Geoserver.  They should be deployed to
Geoserver's WEB-INF/lib directory.

### User Name Role Service

In order to use the User Name Role Service in place of the default in a GS install:

1. unzip the gs-main JAR into a spare directory
2. edit applicationSecurityContext.xml to replace XMLSecurityProvider with XMLUserNameRoleSecurityProvider (including package name!)
3. bundle the contents of the directory into a new JAR with ```jar cf```
4. replace the gs-main JAR from geoserver with the new JAR
5. edit ```$GEOSERVER_DATA/security/role/default/config.xml``` to replace XMLRoleService with XMLUserNameRoleService (including package name!)