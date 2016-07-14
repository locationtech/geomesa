# OpenStreetMap GPX Data for GeoMesa

This directory provides GPS data traces from the OpenStreetMap project. The GPS traces are a series of latitude/longitude pairs collected by OSM or uploaded by users. The datasets were last updated in 2013.

This readme describes the full process from original source data to GeoMesa ingest.

## Getting OSM-GPX data

The OSM-GPX data set can be downloaded using the provided ```download-data.sh``` script in `$GEOMESA_HOME/bin/` as such

    ./download-data.sh osm-gpx

providing a desired region when prompted.

Alternatively, download OSM_GPX data [here](http://planet.openstreetmap.org/gps/). It is formatted in a GPX 1.0 format, which is an XML format described by this [XSD](http://www.topografix.com/GPX/1/0/gpx.xsd). Regional extracts of the dataset can be found [here](http://zverik.osm.rambler.ru/gps/files/extracts/index.html)

## Cleaning the data

Before ingest, the .gpx files can be converted to CSV files through use of [this converter](https://github.com/jahhulbert-ccri/osm-parsers)

## Ingest Commands

Check that `osm-gpx` simple feature type is available on the GeoMesa tools classpath. This is the default case.

    geomesa env | grep osm-gpx

If it is not, merge the contents of `reference.conf` with `$GEOMESA_HOME/conf/application.conf`, or ensure that `reference.conf` is in `$GEOMESA_HOME/conf/sfts/osm-gpx`

Run the ingest. You may optionally point to a different accumulo instance using `-i` and `-z` options. See `geomesa help ingest` for more detail.

    geomesa ingest -u USERNAME -c CATALOGNAME -s osm-gpx -C osm-gpx osm-data.csv
