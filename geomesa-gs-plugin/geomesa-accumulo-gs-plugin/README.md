# GeoMesa Plugin

### Building Instructions

If you wish to build this project separately, you can with mvn clean install

```geomesa/geomesa-gs-plugin/geomesa-accumulo-gs-plugin> mvn clean install```

This will produce a zip of jars in target/ which are intended to be used as a plugin for Geoserver.  They should be extracted and deployed to
Geoserver's WEB-INF/lib directory.

